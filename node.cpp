#include <iostream>
#include "mpi.h"
#include <pthread.h>
#include <unistd.h> //sleep function in Linux
#include <cstdlib>
//#include <list.h>
#include <queue>

using namespace std;


#define MAXTHRDS 3
#define TASK_NUM 4
#define TASK_BUDGET 10
pthread_t callThd[MAXTHRDS];
pthread_mutex_t mutex, mutex_debug;

queue <int> sending_queue;
queue <int> received_queue;



struct thread_status{

	int status[MAXTHRDS];
	int total_busy_thread;
	int task_budget;
};

struct thread_status thrd_status;

int init_task_status(){
	int i;

	//thrd_status = malloc(sizeof(*thrd_status));
	//memset(thrd_status, 0, sizeof(*thrd_status));


	thrd_status.total_busy_thread = 0;
	thrd_status.task_budget = TASK_BUDGET;

	for(i=0; i<MAXTHRDS; i++){
		thrd_status.status[i] = 0;
	}

	return 0;
}

int init_task_queue(){
	//giving initial task id
	received_queue.push(1);
	sending_queue.push(2);

	return 0;
}



int pick_receiver(){  //randomly pickup a rank as receiver
	int receiver_rank;
	int cur_size;

	MPI_Comm_size(MPI_COMM_WORLD, &cur_size);

	srand(time(NULL));

	if (cur_size > 1){
		receiver_rank = rand() % cur_size; //this generate a random number between 0 to cur_size
		return receiver_rank;
	}
	else
		return -1; //I am the only MPI process left
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
			cout << "Task 0: rank " << rank << " thrd_id " << thrd_id << " sleep 1 sec." <<endl; 
			sleep(1);
			break;

		case 1:
			cout << "Task 1: rank " << rank << " thrd_id " << thrd_id << " sleep 2 sec." <<endl; 
			sleep(1);
			break;

		case 2:
			cout << "Task 2: rank " << rank << " thrd_id " << thrd_id << " sleep 3 sec." <<endl; 
			sleep(1);
			break;

		case 3:
			cout << "Task 3: rank " << rank << " thrd_id " << thrd_id << " sleep 4 sec." <<endl; 
			sleep(1);
			break;

		default :
			cout << "Unknown task, break!" <<endl;
			break;

	}
	return;
}

void Finalize_my_task(){ //move all task in sending queue to received queue,
						 //doing all taks myself till run out of task budget
	
	pthread_mutex_lock(&mutex);
	while (!sending_queue.empty()){
		received_queue.push(sending_queue.front());
		sending_queue.pop();
	}
	pthread_mutex_unlock(&mutex);

	return;
}


void *master_thrd(void *arg){

	int i, myrank, receiver_rank, sender_rank;
	long mythrd;
	int* wr_get[100];
	int* wr_send[100];
	MPI_Status mpi_status;
	int err=0;
	int task_id;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

	//update master thread status
	pthread_mutex_lock(&mutex);
	thrd_status.status[mythrd] = 1; //means busy
	thrd_status.total_busy_thread += 1;
	pthread_mutex_unlock(&mutex);

	//Init buffer
	for(int i=0; i<100; i++){
		wr_get[i] = 0;
		wr_send[i] = 0;
	}
	//MPI_Barrier(MPI_COMM_WORLD);
	pthread_mutex_lock(&mutex_debug);
	cout << "I am rank " << myrank << " master thred #" << mythrd <<endl;
	pthread_mutex_unlock(&mutex_debug);


	while(thrd_status.task_budget > 0){

		pthread_mutex_lock(&mutex_debug);
		cout << "Debug: I am rank " << myrank << " master thred #" << mythrd << " In while looping"<<endl;
		cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
		pthread_mutex_unlock(&mutex_debug);	

		// Sending MPI message 
		pthread_mutex_lock(&mutex);
		if(!sending_queue.empty()){
			task_id = sending_queue.front();
			sending_queue.pop();
			//wr_send[0] = task_id;
			receiver_rank = pick_receiver();

		pthread_mutex_lock(&mutex_debug);
		cout << "Debug: I am rank " << myrank << " master thred #" << mythrd << " receiver_rank= " << receiver_rank <<endl;
		pthread_mutex_unlock(&mutex_debug);	

			if (receiver_rank >= 0){
				MPI_Send(&task_id, sizeof(int)+1, MPI_INT, receiver_rank, 0, MPI_COMM_WORLD);
				pthread_mutex_lock(&mutex_debug);
				cout << "Debug: I am rank " << myrank << " master thred #" << mythrd << " SEND!"<<endl;
				cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
				pthread_mutex_unlock(&mutex_debug);	
			}
			else
				Finalize_my_task();
		}
		else{

				pthread_mutex_lock(&mutex_debug);
				cout << "Debug: I am rank " << myrank << " master thred #" << mythrd << " sending_queue EMPTY!"<<endl;
				//cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
				pthread_mutex_unlock(&mutex_debug);	

		}
		pthread_mutex_unlock(&mutex);


		// Listen and receive MPI message 
		err = MPI_Recv(&task_id, sizeof(int), MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &mpi_status);
		pthread_mutex_lock(&mutex_debug);
		cout << "Debug: I am rank " << myrank << " master thred #" << mythrd << " RECEIVED!"<<endl;
		cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
		pthread_mutex_unlock(&mutex_debug);	


		if (err == MPI_SUCCESS){
		pthread_mutex_lock(&mutex);
		//task_id = wr_get[0];
		received_queue.push(task_id);
		//wr_get[0] = 0;
		pthread_mutex_unlock(&mutex);

		pthread_mutex_lock(&mutex_debug);
		cout << "Debug: I am rank " << myrank << " master thred #" << mythrd << " PUSHED!"<<endl;
		cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
		pthread_mutex_unlock(&mutex_debug);	
		}



	}


	//update master thread status
	pthread_mutex_lock(&mutex);
	thrd_status.status[mythrd] = 0; //means idle
	thrd_status.total_busy_thread -= 1;
	pthread_mutex_unlock(&mutex);

	//return; //finish work, back to multithreading() 
		pthread_mutex_lock(&mutex_debug);
		cout << "Debug: I am rank " << myrank << " master thred #" << mythrd << " DONE!!"<<endl;
		//cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
		pthread_mutex_unlock(&mutex_debug);	

}





void *worker_thrd(void *arg){
	int i, myrank;
	long mythrd;
	int wr_get;  //work_request, local thread fetch from task queue
	int wr_send;  //thread generate new task and put into sending queue
	int task_id;
	//int receiver_rank;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

	pthread_mutex_lock(&mutex_debug);
	cout << "I am rank " << myrank << " worker thred #" << mythrd <<endl;
	pthread_mutex_unlock(&mutex_debug);

	while(thrd_status.task_budget > 0){//keep looping
		
		//fetch work request from received pending queue
		if(! received_queue.empty()){

			pthread_mutex_lock(&mutex);
			//update thread status
			thrd_status.status[mythrd] = 1; //means busy
			thrd_status.total_busy_thread += 1;
			
			//fetch work request from received queue, temporarily just task id
			wr_get = received_queue.front();
			received_queue.pop();
			thrd_status.task_budget -= 1; 
			pthread_mutex_unlock(&mutex);

			task_id = wr_get;
			//execute task
			run_task(task_id, myrank, mythrd);
			
			//task_id for next work request
			wr_send = pick_task();
			
			//pthread_mutex_lock(&mutex);
			//cout << "Debug: I am rank " << myrank << " worker thred #" << mythrd << "wr_send=" <<wr_send<<endl;
			//pthread_mutex_unlock(&mutex);
			
			//put work request into sending queue
			pthread_mutex_lock(&mutex);
			//list_add(&wr_send->wr_entry, &tk_queue->sending_queue);
			sending_queue.push(wr_send);
			//update thread status
			thrd_status.status[mythrd] = 0; //means idle
			thrd_status.total_busy_thread -= 1;			
			pthread_mutex_unlock(&mutex);

			pthread_mutex_lock(&mutex_debug);
			cout << "Debug: I am rank " << myrank << " worker thred #" << mythrd <<" wr_send="<< wr_send << " PUSHED!"<<endl;
			cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
			pthread_mutex_unlock(&mutex_debug);	

			//update 
			
		}//end of if	

			//pthread_mutex_lock(&mutex);
			//cout << "Debug: I am rank " << myrank << " worker thred #" << mythrd << " In while looping"<<endl;
			//pthread_mutex_unlock(&mutex);
	}//end of while

		pthread_mutex_lock(&mutex_debug);
		cout << "Debug: I am rank " << myrank << " woker thred #" << mythrd << " DONE!"<<endl;
		//cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
		pthread_mutex_unlock(&mutex_debug);	

	//return; //back to multithreading
}



/*for debugging */
void* master_test(void *arg){
int i, myrank;
	long mythrd;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

	pthread_mutex_lock(&mutex);
	printf("Master say hi from rank %d thread %ld. \n", myrank, mythrd);
	pthread_mutex_unlock(&mutex);

	pthread_exit((void*)0);

}


void* worker_test(void *arg){
int i, myrank;
	long mythrd;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

	pthread_mutex_lock(&mutex);
	printf("Worker say hi from rank %d thread %ld. \n", myrank, mythrd);
	pthread_mutex_unlock(&mutex);

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



/*Create thread attribute to specify that the main thread needs
to join with the threads it create.
*/
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	/*Create mutex*/
	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_init(&mutex_debug, NULL);

	//init task status
	init_task_status();

	init_task_queue();

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
	pthread_mutex_destroy(&mutex_debug);

	return 0;

}



int main(int argc, char** argv){

	int world_rank, world_size;

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);


	multithreading();

	cout << "Rank" << world_rank << "finish job!" << endl;

	MPI_Finalize();

	return 0;


}
