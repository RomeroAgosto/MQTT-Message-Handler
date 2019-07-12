#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "MQTTClient.h"
#include <pthread.h>
#include <unistd.h>

#include <sys/ipc.h>
#include <sys/msg.h>

#define ADDRESS     "tcp://localhost:1883"
#define CLIENTID    "Handler"
#define TIMEOUT     10000L

volatile MQTTClient_deliveryToken deliveredtoken;

pthread_mutex_t lock;

MQTTClient client;
MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
MQTTClient_message pubmsg = MQTTClient_message_initializer;
MQTTClient_deliveryToken token;

struct mesg_buffer {
    long mesg_type;
    char mesg_text[100];
} message;

void delivered(void *context, MQTTClient_deliveryToken dt)
{
    printf("Message with token value %d delivery confirmed\n", dt);
    deliveredtoken = dt;
}

int msgarrvd(void *context, char *topicName, int topicLen, MQTTClient_message *message)
{
    int i;
    char* payloadptr;

    printf("Message arrived\n");
    printf("     topic: %s\n", topicName);
    printf("   message: ");

    if(strcmp(topicName,"Control")==0)
       {
       	if(strcmp(message->payload,"q")==0) pthread_mutex_unlock(&lock);
       }


    payloadptr = message->payload;
    for(i=0; i<message->payloadlen; i++)
    {
        putchar(*payloadptr++);
    }
    putchar('\n');
    MQTTClient_freeMessage(&message);
    MQTTClient_free(topicName);
    return 1;
}

void connlost(void *context, char *cause)
{
    printf("\nConnection lost\n");
    printf("     cause: %s\n", cause);
}

void initHandler()
{
	int rc;
	if (pthread_mutex_init(&lock, NULL) != 0)
	{
		printf("\n mutex init has failed\n");
		exit(1);
	}

	MQTTClient_create(&client, ADDRESS, CLIENTID,
	MQTTCLIENT_PERSISTENCE_NONE, NULL);
	conn_opts.keepAliveInterval = 20;
	conn_opts.cleansession = 1;

	MQTTClient_setCallbacks(client, NULL, connlost, msgarrvd, delivered);

	if ((rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS)
	{
		printf("Failed to connect, return code %d\n", rc);
		exit(EXIT_FAILURE);
	}

}

void msgdelivery(char TOPIC[], char PAYLOAD[], int QOS)
{
    pubmsg.payload = PAYLOAD;
    pubmsg.payloadlen = strlen(PAYLOAD);
    pubmsg.qos = QOS;
    pubmsg.retained = 0;
    deliveredtoken = 0;
    MQTTClient_publishMessage(client, TOPIC, &pubmsg, &token);

    while(deliveredtoken != token);

}

void topSub(char TOPIC[], int QOS)
{
	MQTTClient_subscribe(client, TOPIC, QOS);

}

void * WorkerThread(void * a)
{
	pthread_mutex_lock(&lock);
	topSub("MQTT Queue", 1);
	topSub("Control", 1);
	topSub("MQTT Message", 1);
	return NULL;
}

void topUnsub(char TOPIC[])
{
    MQTTClient_unsubscribe(client, TOPIC);
}
int main(int argc, char* argv[])
{
	initHandler();

	key_t key;
	int msgid;

	if(argc==2)
	{
		key = atoi(argv[1]);
	}else
	{
		// ftok to generate unique key
		key = ftok("progfile", 65);
	}

	// msgget creates a message queue
	// and returns identifier

	msgid = msgget(key, 0666 | IPC_CREAT);

	printf("Handler: %d\n", msgid);

    // msgrcv to receive message
    msgrcv(msgid, &message, sizeof(message), 1, 0);
    printf("Received: %s\n", message.mesg_text);

	msgdelivery("Queue", message.mesg_text, 1);

	pthread_t thread_id;
	pthread_create(&thread_id, NULL, WorkerThread, NULL);

    sleep(1);
	pthread_mutex_lock(&lock);

    pthread_join(thread_id, NULL);

    // to destroy the message queue
    msgctl(msgid, IPC_RMID, NULL);

    MQTTClient_disconnect(client, 10000);
    MQTTClient_destroy(&client);

    pthread_mutex_destroy(&lock);
    return 0;
}
