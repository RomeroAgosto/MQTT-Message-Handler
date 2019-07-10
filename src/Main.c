#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "MQTTClient.h"

#define ADDRESS     "tcp://localhost:1883"
#define CLIENTID    "Handler"
#define TIMEOUT     10000L

volatile MQTTClient_deliveryToken deliveredtoken;

MQTTClient client;
MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
MQTTClient_message pubmsg = MQTTClient_message_initializer;
MQTTClient_deliveryToken token;

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
    printf("Waiting for publication of %s\n"
            "on topic %s for client with ClientID: %s\n",
            PAYLOAD, TOPIC, CLIENTID);
    while(deliveredtoken != token);

}

void topSub(char TOPIC[], int QOS)
{
	int ch;
	printf("Subscribing to topic %s\nfor client %s using QoS%d\n\n"
	           "Press Q<Enter> to quit\n\n", TOPIC, CLIENTID, QOS);
	    MQTTClient_subscribe(client, TOPIC, QOS);

	do
	{
		ch = getchar();
	} while(ch!='Q' && ch != 'q');


}

void topUnsub(char TOPIC[])
{
    MQTTClient_unsubscribe(client, TOPIC);
}
int main(int argc, char* argv[])
{
	initHandler();

	//pthread_t thread_id;
	//pthread_create(&thread_id, NULL,topSub, "MQTT Message", 1);

    msgdelivery("MQTT Examples", "Hello World!", 1);
    while(1);
    MQTTClient_disconnect(client, 10000);
    MQTTClient_destroy(&client);
    return 0;
}
