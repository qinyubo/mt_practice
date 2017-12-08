#include <stdio.h>
#include "mpi.h"
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h> //sleep function in Linux
#include <list.h>

#define MAXTHRDS 3
#define TASK_NUM 4
pthread_t callThd[MAXTHRDS];
pthread_mutex_t mutex;




//task queue
struct task_queue{
	struct list_head sending_queue;
	struct list_head received_queue;
};


//work request structure
struct work_request {
	struct list_head wr_entry;
	int 	sender_rank;
	long 	sender_thrdid;

	int 	receiver_rank;
	int 	task_id;
	
};

struct thread_status{

	int status[MAXTHRDS];
	int total_busy_thread;
	int task_budget=10;
};

//struct work_request *wk_req;
struct task_queue *tk_queue;
struct thread_status *thrd_status;

int init_task_status(){
	int i;
	thrd_status->total_busy_thread = 0;
	thrd_status->task_budget = 10;

	for(i=0; i<MAXTHRDS; i++){
		thrd_status->status[i] = 0;
	}

	return 0;
}



int pick_receiver(){  //randomly pickup a rank as receiver
	int receiver_rank;
	int cur_size;

	MPI_Comm_size(MPI_COMM_WORLD, &cur_size);

	srand(time(NULL));

	receiver_rank = rand() % cur_size; //this generate a random number between 0 to cur_size

	return receiver_rank;
}

int pick_task(){ //pick a task for worker to execute
	int task_id;

	srand(time(NULL));
	task_id = rand() % TASK_NUM;

	return task_id;
}

void run_task(int task_id, int rank, int thrd_id){

	switch(task_id){
		case 0:
			print("Task 0: rank %d thrd_id %ld sleep 1 sec.\n", rank, thrd_id);
			sleep(1);
			break;

		case 1:
			print("Task 1: rank %d thrd_id %ld sleep 2 sec.\n", rank, thrd_id);
			sleep(1);
			break;

		case 2:
			print("Task 2: rank %d thrd_id %ld sleep 3 sec.\n", rank, thrd_id);
			sleep(1);
			break;

		case 3:
			print("Task 3: rank %d thrd_id %ld sleep 4 sec.\n", rank, thrd_id);
			sleep(1);
			break;

		default :
			print("Unknown task, break!\n");
			break:

	}
}


void *master_thrd(void *arg){

	int i, myrank;
	long mythrd;
	struct work_request *wr_get;
	struct work_request *wr_send;
	MPI_Status mpi_status;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);


	while(thrd_status->task_budget > 0){
	
		pthread_mutex_lock(&mutex);
		//update thread status
		thrd_status->status[mythrd] = 1; //means busy
		thrd_status->total_busy_thread += 1;
		pthread_mutex_unlock(&mutex);

		/* Listen and receive MPI message */
		MPI_Rcev (wr_get, sizeof(struct work_request), SOME_TYPE, MPI_ANY_SOURCE, MPI_COMM_WORLD, &mpi_status);

		if (received){
		pthread_mutex_lock(&mutex);
		//update thread status
		thrd_status->task_budget -= 1; //means busy
		pthread_mutex_unlock(&mutex);
		}

		//put received request to the received task queue
		/* require some operations */
		list_add(&wr_get->wr_entry, &tk_queue->received_queue);

		/* Sending MPI message */
		//fetch work request from sending queue
		if (! list_empty(&tk_queue->sending_queue)){

			wr_send = list_entry(tk_queue->sending_queue, struct work_request, wr_entry);
			MPI_Send(&wr_send, sizeof(struct work_request)+1, SOME_TYPE, wr_send->receiver_rank, MPI_COMM_WORLD);
		}

	}

	/* Don't have budget, send back a message to return task */



}





void *worker_thrd(void *arg){
	int i, myrank;
	long mythrd;
	struct work_request *wr_get;  //work_request, local thread fetch from task queue
	struct work_request *wr_send;  //thread generate new task and put into sending queue
	int task_id;
	int receiver_rank;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

	while(1){//keep looping
		
		//fetch work request from received pending queue
		if(! list_empty(&tk_queue->received_queue)){

			pthread_mutex_lock(&mutex);
			//update thread status
			thrd_status->status[mythrd] = 1; //means busy
			thrd_status->total_busy_thread += 1;
			wr_get = list_entry(tk_queue->received_queue, struct work_request, wr_entry);
			pthread_mutex_unlock(&mutex);

			task_id = wr_get.task_id;
			//execute task
			run_task(task_id, myrank, mythrd);
			//task_id for next work request
			task_id = pick_task();
			//receiver rank
			receiver_rank = pick_receiver();

			//write work request
			wr_send = calloc(1, sizeof(struct work_request));

			wr_send->sender_rank = myrank;
			wr_send->sender_thrdid = mythrd;
			wr_send->receiver_rank = receiver_rank;
			wr_send->task_id = task_id;
			
			//put work request into sending queue
			pthread_mutex_lock(&mutex);
			list_add(&wr_send->wr_entry, &tk_queue->sending_queue);
			//update thread status
			thrd_status->status[mythrd] = 0; //means idle
			thrd_status->total_busy_thread -= 1;			
			pthread_mutex_unlock(&mutex);

			//update 
			
		}//end of if	
	}//end of while
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



/*Create thread attribute to specify that the main thread needs
to join with the threads it create.
*/
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	/*Create mutex*/
	pthread_mutex_init(&mutex, NULL);

	//init task queue
	INIT_LIST_HEAD(&tk_queue->sending_queue);
	INIT_LIST_HEAD(&tk_queue->received_queue);

	//init task status
	init_task_status();

	/*Create threads*/
	numthrds = MAXTHRDS;

	for(j=0; j<numthrds; j++){
		if(j==0) //master thread
			pthread_create(&callThd[j], &attr, master_thrd, (void *)j);
		else
			pthread_create(&callThd[j], &attr, worker_thrd, (void *)j);
	}

	/*Release the thread attribute handle*/
	pthread_attr_destroy(&attr);

	/*Wait for the other threads within this node*/
	for(i=0; i<numthrds; i++){
		pthread_join(callThd[i], &status);
	}

	/*Release mutex*/
	pthread_mutex_destroy(&mutex);

}



int main(int argc, char** argv){

	int world_rank, world_size;

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);


	multithreading();

	printf("Rank %d finish job!\n", world_rank);

	MPI_Finalize();

	return 0;


}
