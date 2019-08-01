#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include "MQTTClient.h"
#include "C:\Users\Ricardo\eclipse-workspace\MQ Defines.h"

#define ADDRESS     "tcp://localhost:1883"
#define CLIENTID    "Handler"
#define TIMEOUT     10000L

/*
union semun {
	int val;
	struct semid_ds *buf;
	unsigned short *array;
};
*/

// structures for message queue
//	Struct to send
struct mesg_buffer QueueMessageSend;
//	Struct to serve as a buffer when a message is received to avoid overwrites
struct mesg_buffer QueueMessageBuffer;

volatile MQTTClient_deliveryToken deliveredtoken;

static int msgid_send, msgid_receive;

char running=1;

MQTTClient client;
MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
MQTTClient_message pubmsg = MQTTClient_message_initializer;
MQTTClient_deliveryToken token;

void MQSetup(void);

void MessageQueueSend(char topic[], char content[]);

int sendMessage(void);

int receiveNMessage(int long n);

int receiveAnyMessage(void);

/*
static int set_semvalue(void);

static void del_semvalue(void);

static int semaphore_v(void);

static int semaphore_p(void);
*/

void delivered(void *context, MQTTClient_deliveryToken dt)
{
    printf("Message with token value %d delivery confirmed\n", dt);
    deliveredtoken = dt;
}

int msgarrvd(void *context, char *topicName, int topicLen, MQTTClient_message *message)
{
    MessageQueueSend(topicName, message ->payload);

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

void MQTTSend(char TOPIC[], char PAYLOAD[], int QOS)
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

void topUnsub(char TOPIC[])
{
    MQTTClient_unsubscribe(client, TOPIC);
}

int main(int argc, char* argv[])
{
	initHandler();

	MQSetup();

	while(running){
		receiveAnyMessage();
		printf("Type: %ld\nContent: %s\nTopic: %s\n", QueueMessageBuffer.mesg_type, QueueMessageBuffer.content, QueueMessageBuffer.topic);
		if (strncmp(QueueMessageBuffer.topic, "end", 3) == 0)
		{
			running = 0;
		}
		if(QueueMessageBuffer.mesg_type==TYPE_SUB)topSub(QueueMessageBuffer.topic,1);
		if(QueueMessageBuffer.mesg_type==TYPE_PUB)MQTTSend(QueueMessageBuffer.topic, QueueMessageBuffer.content,1);
	}

	//Delete Message Queue
	if (msgctl(msgid_send, IPC_RMID, 0) == -1)
	{
		fprintf(stderr, "msgctl(IPC_RMID) failed\n");
		exit(EXIT_FAILURE);
	}
	if (msgctl(msgid_receive, IPC_RMID, 0) == -1)
	{
		fprintf(stderr, "msgctl(IPC_RMID) failed\n");
		exit(EXIT_FAILURE);
	}

    MQTTClient_disconnect(client, 10000);
    MQTTClient_destroy(&client);

    return 0;
}

void MQSetup(void)
{
	// msgget creates a message queue
	// and returns identifier
	// msgget used to join with the queue from the Handler
	msgid_receive = msgget((key_t)MQKEY_TOHANDLER, 0666 | IPC_CREAT);
	if (msgid_receive == -1)
	{
		fprintf(stderr, "msgget failed with error: %d\n", errno);
		exit(EXIT_FAILURE);
	}
	msgid_send = msgget((key_t)MQKEY_FROMHANDLER, 0666 | IPC_CREAT);
	if (msgid_send == -1)
	{
		fprintf(stderr, "msgget failed with error: %d\n", errno);
		exit(EXIT_FAILURE);
	}
	printf("Connection Established\n"
			"Handler MQID: %d	&	%d\n", msgid_send, msgid_receive);
}

void MessageQueueSend(char topic[], char content[]) {
	//Inform that it is a subscribe and to what topic

	QueueMessageSend.mesg_type=1;
	strncpy(QueueMessageSend.topic, topic, BUFSIZ/2);
	strncpy(QueueMessageSend.content, content, BUFSIZ/2);

	sendMessage();
}

int sendMessage(void) {
	if (msgsnd(msgid_send, (void *)&QueueMessageSend, MAX_TEXT, 0) == -1) {
		fprintf(stderr, "msgsnd failed\n");
		exit(EXIT_FAILURE);
	}
	return 0;
}

int receiveNMessage(int long n) {
	if (msgrcv(msgid_receive, (void *)&QueueMessageBuffer, BUFSIZ, n, 0) == -1) {
		fprintf(stderr, "msgrcv failed with error: %d\n", errno);
		exit(EXIT_FAILURE);
	}
	return 0;
}

int receiveAnyMessage(void) {
	if (msgrcv(msgid_receive, (void *)&QueueMessageBuffer, BUFSIZ, 0, 0) == -1) {
		fprintf(stderr, "msgrcv failed with error: %d\n", errno);
		exit(EXIT_FAILURE);
	}
	return 0;
}

/*
 * static int set_semvalue(void){

	union semun sem_union;
	sem_union.val = 1;
	if (semctl(sem_id, 0, SETVAL, sem_union) == -1) return(0);
	return(1);
}

static void del_semvalue(void){

	union semun sem_union;
	if (semctl(sem_id, 0, IPC_RMID, sem_union) == -1)
		fprintf(stderr, "Failed to delete semaphore\n");
}

static int semaphore_v(void){

	struct sembuf sem_b;
	sem_b.sem_num = 0;
	sem_b.sem_op = 1;
	sem_b.sem_flg = SEM_UNDO;
	if (semop(sem_id, &sem_b, 1) == -1) {
		fprintf(stderr, "semaphore_v failed\n");
		return(0);
	}
	return(1);
}

static int semaphore_p(void){

	struct sembuf sem_b;
	sem_b.sem_num = 0;
	sem_b.sem_op = -1;
	sem_b.sem_flg = SEM_UNDO;
	if (semop(sem_id, &sem_b, 1) == -1) {
		fprintf(stderr, "semaphore_p failed\n");
		return(0);
	}
	return(1);
}
*/
