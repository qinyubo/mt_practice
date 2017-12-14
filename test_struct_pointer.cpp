#include <iostream>
#include "mpi.h"
#include <pthread.h>
#include <unistd.h> //sleep function in Linux
#include <cstdlib>
//#include <list.h>
#include <queue>
#include <errno.h>
#include <vector>
#include <string>

using namespace std;

#define MAXTHRDS 2

pthread_t callThd[MAXTHRDS];
pthread_mutex_t mutex, mutex_debug;

struct mystruct{
	int sender_rank;
	string msg;
};

typedef struct mystruct struct_t;



/*for debugging */
void* master_test(void *arg){
int i, myrank;
long mythrd;
int task_id;
int receiver_rank;
struct_t *my_msg; //pointer



	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

	task_id = myrank;
	if(myrank == 0){
		receiver_rank = 1;
	}
	else
		receiver_rank = 0;


	//init my_msg
	my_msg = (struct_t *)malloc(sizeof(*my_msg));

	my_msg->sender_rank = myrank;
	my_msg->msg = "Hi from " + myrank;





	for(i=0; i<1;i++){
	//pthread_mutex_lock(&mutex);
	//cout <<"Master say hi from rank " << myrank << " thread " << mythrd <<endl;
	//sleep(0.5);
	

	MPI_Send(my_msg, sizeof(my_msg), MPI_BYTE, receiver_rank, 0, MPI_COMM_WORLD);
	pthread_mutex_lock(&mutex);
	cout << "I am rank " << myrank << " send to "<<receiver_rank <<endl;
	pthread_mutex_unlock(&mutex);

	//pthread_mutex_unlock(&mutex);
	}

	pthread_exit((void*)0);

}


void* worker_test(void *arg){
int i, myrank;
long mythrd;
int task_id;
int sender_rank;
MPI_Status status;
struct_t *recv_buf;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

	//task_id = myrank;
	if(myrank == 0){
		sender_rank = 1;
	}
	else
		sender_rank = 0;

	recv_buf = (struct_t *)malloc(sizeof(struct_t));

	for(i=0; i<1; i++){
	//pthread_mutex_lock(&mutex);
	//cout <<"Worker say hi from rank " << myrank << " thread " << mythrd <<endl;
	//sleep(0.5);
	//pthread_mutex_unlock(&mutex);
		MPI_Recv(recv_buf, sizeof(recv_buf), MPI_BYTE, sender_rank, 0, MPI_COMM_WORLD, &status);
		pthread_mutex_lock(&mutex);
		cout << "I am rank " << myrank << " received from "<<recv_buf->sender_rank<<" with msg: "\
		<< recv_buf->msg <<endl;
		pthread_mutex_unlock(&mutex);


	}

	pthread_exit((void*)0);

}


int multithreading(){
/* 1. create threads
   2. assign master and worker
 */
pthread_attr_t attr;
int numthrds;
int i;
long j;
void *status;
int myrank;

MPI_Comm_rank(MPI_COMM_WORLD, &myrank);



/*Create thread attribute to specify that the main thread needs
to join with the threads it create.
*/

	/*Create mutex*/
	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_init(&mutex_debug, NULL);



	/*Create threads*/
	numthrds = MAXTHRDS;

	for(j=0; j<numthrds; j++){
		if(j==0){ //master thread
			pthread_create(&callThd[j], NULL, master_test, (void *)j); //Master thread
		}
		else{
			pthread_create(&callThd[j], NULL, worker_test, (void *)j); //Worker thread
		}
	}


	/*Wait for the other threads within this node*/
	for(i=0; i<numthrds; i++){
		pthread_join(callThd[i], &status);
	}

	/*Release mutex*/	
	pthread_mutex_destroy(&mutex);
	pthread_mutex_destroy(&mutex_debug); //debug mutex lock

	return 0;

}




int main(int argc, char** argv){

	int world_rank, world_size;
	int provided;
	MPI_Request request;
	MPI_Status status;

	MPI_Init(&argc, &argv);

	/*
	MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE, &provided);  
	if(provided != MPI_THREAD_MULTIPLE)  
	{  
   		 cout << "MPI do not Support Multiple thread" << endl;  
   		 exit(0);  
	} 
	*/

	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);


	multithreading();

/*
	cout << "MPI_wait() " << endl;
	MPI_Wait(&request, &status);
*/
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);

	cout << "Rank " << world_rank << " finish job!"<< endl;



	MPI_Finalize();
	cout << "Rank " << world_rank << " MPI_Finalize!"<< endl;


	return 0;


}