#include <iostream>
#include "mpi.h"
#include <pthread.h>
#include <unistd.h> //sleep function in Linux
#include <cstdlib>
//#include <list.h>
#include <queue>
#include <errno.h>

using namespace std;


#define MAXTHRDS 3
#define TASK_NUM 4
#define TASK_BUDGET 10
pthread_t callThd[MAXTHRDS];
pthread_mutex_t mutex, mutex_debug;

queue <int> sending_queue;
queue <int> received_queue;

int task_budget = 10;



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
	int receiver_rank, myrank;
	int cur_size;

	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
	MPI_Comm_size(MPI_COMM_WORLD, &cur_size);

	srand(time(NULL));

	if (cur_size > 1){
		do{receiver_rank = rand() % cur_size; }
		while(receiver_rank == myrank);
	pthread_mutex_lock(&mutex_debug);
	cout << "PIN12 I am rank " << myrank << " return receiver_rank="<<receiver_rank<<endl;
	pthread_mutex_unlock(&mutex_debug);
		return receiver_rank;
	}
	else
		return -1; //I am the only MPI process left
}

int pick_task(){ //pick a task for worker to execute
	int task_id;
	pthread_mutex_lock(&mutex_debug);
		cout << "I am in pick_task()" <<endl;
		pthread_mutex_unlock(&mutex_debug);

	srand(time(NULL));
	task_id = rand() % TASK_NUM;

	return task_id;
}

void run_task(int task_id, int rank, int thrd_id){
	pthread_mutex_lock(&mutex_debug);
		cout << "NUM 5 I am rank " << rank << " worker thred #" << thrd_id <<endl;
		pthread_mutex_unlock(&mutex_debug);

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
	
	
		pthread_mutex_lock(&mutex_debug);
		cout << "Debug: I am in finalize my task"<<endl;
		//cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
		pthread_mutex_unlock(&mutex_debug);	



//	pthread_mutex_lock(&mutex);
	if(!sending_queue.empty()){
		while (!sending_queue.empty()){
			received_queue.push(sending_queue.front());
			sending_queue.pop();
		}
	}
	else
		received_queue.push(1); //push task 1, in case deadlock
//	pthread_mutex_unlock(&mutex);

	return;
}


void *master_thrd(void *arg){

	int i, myrank, receiver_rank, sender_rank;
	long mythrd;
	int* wr_get[100];
	int* wr_send[100];
	//MPI_Status mpi_status;
	MPI_Request mpi_status;
	int err=0;
	int task_id;
	int err_lock=0;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);


	//Init buffer
	for(int i=0; i<100; i++){
		wr_get[i] = 0;
		wr_send[i] = 0;
	}
	//MPI_Barrier(MPI_COMM_WORLD);
	pthread_mutex_lock(&mutex_debug);
	cout << "PIN1 I am rank " << myrank << " master thred #" << mythrd <<endl;
	pthread_mutex_unlock(&mutex_debug);


	while(task_budget > 0){
		/*
		pthread_mutex_lock(&mutex_debug);
		cout << "PIN9 I am rank " << myrank << " master thred #" << mythrd << " sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() \
		<<"  task_budget= "<<task_budget<<endl;
		pthread_mutex_unlock(&mutex_debug);
		*/


		// Sending MPI message 
		err_lock = pthread_mutex_trylock(&mutex);
		if(err_lock == EBUSY){
//		cout << "#1 Mutex lock busy rank " << myrank << " master thred #" << mythrd <<endl;
		continue;
		}
		else if(!sending_queue.empty()){
			task_id = sending_queue.front();
			sending_queue.pop();
			//wr_send[0] = task_id;
			receiver_rank = pick_receiver();
			pthread_mutex_lock(&mutex_debug);
			cout << "PIN2 I am rank " << myrank << " master thred #" << mythrd <<endl;
			pthread_mutex_unlock(&mutex_debug);
		

			
			if (receiver_rank >= 0){
			//pthread_mutex_lock(&mutex);
			//received_queue.push(task_id);
				cout <<"Ready to MPI_SEND" << endl;
			err = MPI_Send(&task_id, 16, MPI_INT, receiver_rank, 0, MPI_COMM_WORLD);
				cout <<"After to MPI_SEND" << endl;
			if(err == MPI_SUCCESS){
				pthread_mutex_lock(&mutex_debug);
				cout << "PIN10 I am rank " << myrank << " master thred #" << mythrd <<" MPI_SEND succeed! Sent to "\
				<< receiver_rank <<endl;
				pthread_mutex_unlock(&mutex_debug);
			}
			else{
				pthread_mutex_lock(&mutex_debug);
				cout << "PIN10 I am rank " << myrank << " master thred #" << mythrd <<" MPI_SEND fail!! "<<endl;
				pthread_mutex_unlock(&mutex_debug);

			}
			pthread_mutex_lock(&mutex_debug);
				cout << "PIN3 I am rank " << myrank << " master thred #" << mythrd <<endl;
				pthread_mutex_unlock(&mutex_debug);
			}
			else{
				Finalize_my_task();
				pthread_mutex_lock(&mutex_debug);
				cout << "PIN4 I am rank " << myrank << " master thred #" << mythrd <<endl;
				pthread_mutex_unlock(&mutex_debug);
			}
			
			//received_queue.push(task_id);
		pthread_mutex_unlock(&mutex);		
		}
		else pthread_mutex_unlock(&mutex);	
		

		pthread_mutex_lock(&mutex_debug);
		cout << "PIN7 I am rank " << myrank << " master thred #" << mythrd <<endl;
		pthread_mutex_unlock(&mutex_debug);
		

		if(myrank == 0){
			sender_rank = 1;
		}
		else
			sender_rank = 0;

	
		// Listen and receive MPI message 
		MPI_Barrier(MPI_COMM_WORLD);
		pthread_mutex_lock(&mutex);	

		cout <<"Ready to MPI_RECV" << endl;
		err = MPI_Irecv(&task_id, 256, MPI_INT, sender_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &mpi_status);
		cout <<"After to MPI_RECV" << endl;
		/*
		pthread_mutex_lock(&mutex_debug);
		cout << "PIN5 I am rank " << myrank << " master thred #" << mythrd <<endl;
		pthread_mutex_unlock(&mutex_debug);
		*/
		if (err == MPI_SUCCESS){
		//pthread_mutex_lock(&mutex);
		//task_id = wr_get[0];
		received_queue.push(task_id);
		//wr_get[0] = 0;
		pthread_mutex_unlock(&mutex);
		/*
		pthread_mutex_lock(&mutex_debug);
		cout << "PIN6 I am rank " << myrank << " master thred #" << mythrd <<" MPI RECEIVED succeed! From source="\
		<<endl;
		//<<mpi_status.MPI_SOURCE<<endl;
		pthread_mutex_unlock(&mutex_debug);
		*/

		}
		/*
		else if(err == MPI_ERR_COMM){
		pthread_mutex_lock(&mutex_debug);
		cout << "Recv ERROR MPI_ERR_COMM I am rank " << myrank << " master thred #" << mythrd <<endl;
		pthread_mutex_unlock(&mutex_debug);
		pthread_mutex_unlock(&mutex);
		}
		else if(err == MPI_ERR_TYPE){
		pthread_mutex_lock(&mutex_debug);
		cout << "Recv ERROR MPI_ERR_TYPE I am rank " << myrank << " master thred #" << mythrd <<endl;
		pthread_mutex_unlock(&mutex_debug);
		pthread_mutex_unlock(&mutex);
		}
		else if(err == MPI_ERR_COUNT){
		pthread_mutex_lock(&mutex_debug);
		cout << "Recv ERROR MPI_ERR_COUNT I am rank " << myrank << " master thred #" << mythrd <<endl;
		pthread_mutex_unlock(&mutex_debug);
		pthread_mutex_unlock(&mutex);
		}
		else if(err == MPI_ERR_TAG){
		pthread_mutex_lock(&mutex_debug);
		cout << "Recv ERROR MPI_ERR_TAG I am rank " << myrank << " master thred #" << mythrd <<endl;
		pthread_mutex_unlock(&mutex_debug);
		pthread_mutex_unlock(&mutex);
		}
		else if(err == MPI_ERR_RANK){
		pthread_mutex_lock(&mutex_debug);
		cout << "Recv ERROR MPI_ERR_RANK I am rank " << myrank << " master thred #" << mythrd <<endl;
		pthread_mutex_unlock(&mutex_debug);
		pthread_mutex_unlock(&mutex);
		}
		*/
		else{
		pthread_mutex_lock(&mutex_debug);
		cout << "PIN6 I am rank " << myrank << " master thred #" << mythrd <<" MPI RECEUVED fail! "<<endl;
		pthread_mutex_unlock(&mutex_debug);
		pthread_mutex_unlock(&mutex);
		}
		

		pthread_mutex_lock(&mutex_debug);
		cout << "PIN7 I am rank " << myrank << " master thred #" << mythrd <<endl;
		pthread_mutex_unlock(&mutex_debug);


	}
		pthread_mutex_lock(&mutex_debug);
		cout << "PIN8 I am rank " << myrank << " master thred #" << mythrd <<"DONE!"<<endl;
		pthread_mutex_unlock(&mutex_debug);

}





void *worker_thrd(void *arg){
	int i, myrank;
	long mythrd;
	int wr_get;  //work_request, local thread fetch from task queue
	int wr_send;  //thread generate new task and put into sending queue
	int task_id;
	int err_lock=0;
	//int receiver_rank;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

	pthread_mutex_lock(&mutex_debug);
	cout << "I am rank " << myrank << " worker thred #" << mythrd <<endl;
	pthread_mutex_unlock(&mutex_debug);

	while(task_budget > 0){//keep looping
		/*
		pthread_mutex_lock(&mutex_debug);
		cout << "NUM 1 I am rank " << myrank << " worker thred #" << mythrd << " sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() \
		<<"  task_budget= "<<task_budget<<endl;
		pthread_mutex_unlock(&mutex_debug);
		*/
		
		//fetch work request from received pending queue
		err_lock = pthread_mutex_trylock(&mutex);
		if(err_lock == EBUSY){
		//cout << "#2 Mutex lock busy rank " << myrank << " worker thred #" << mythrd <<endl;
		continue;
		}
		else if(!received_queue.empty()){ 		
		
			pthread_mutex_lock(&mutex_debug);
			cout << "NUM 2 I am rank " << myrank << " worker thred #" << mythrd <<endl;
			pthread_mutex_unlock(&mutex_debug);

			//pthread_mutex_lock(&mutex);

			//fetch work request from received queue, temporarily just task id
			wr_get = received_queue.front();
			received_queue.pop();

			pthread_mutex_unlock(&mutex); //don't let run_task block


			task_budget -= 1; 
			//pthread_mutex_unlock(&mutex);

			task_id = wr_get;
			//execute task
			run_task(task_id, myrank, mythrd);
			
			//task_id for next work request
			wr_send = pick_task();
			pthread_mutex_lock(&mutex_debug);
			cout << "NUM 3 I am rank " << myrank << " worker thred #" << mythrd <<endl;
			pthread_mutex_unlock(&mutex_debug);
			
			
			//put work request into sending queue
					
			//list_add(&wr_send->wr_entry, &tk_queue->sending_queue);
			pthread_mutex_trylock(&mutex); 
				
			sending_queue.push(wr_send);

			pthread_mutex_lock(&mutex_debug);
			cout << "NUM 4 I am rank " << myrank << " worker thred #" << mythrd <<endl;
			pthread_mutex_unlock(&mutex_debug);
			//update 
		pthread_mutex_unlock(&mutex);
			
		}//end of if
		else pthread_mutex_unlock(&mutex);	
	}//end of while
	pthread_mutex_lock(&mutex_debug);
			cout << "NUM 6 I am rank " << myrank << " worker thred #" << mythrd <<"DONE!"<<endl;
			pthread_mutex_unlock(&mutex_debug);

	//return; //back to multithreading
}


/*for debugging */
void* master_test(void *arg){
int i, myrank;
	long mythrd;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

	for(i=0; i<10;i++){
	//pthread_mutex_lock(&mutex);
	cout <<"Master say hi from rank " << myrank << "thread " << mythrd <<endl;
	sleep(0.5);
	//pthread_mutex_unlock(&mutex);
	}

	pthread_exit((void*)0);

}


void* worker_test(void *arg){
int i, myrank;
	long mythrd;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

	for(i=0; i<10; i++){
	//pthread_mutex_lock(&mutex);
	cout <<"Worker say hi from rank " << myrank << "thread " << mythrd <<endl;
	sleep(0.5);
	//pthread_mutex_unlock(&mutex);
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



/*Create thread attribute to specify that the main thread needs
to join with the threads it create.
*/
	//pthread_attr_init(&attr);
	//pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	/*Create mutex*/
	pthread_mutex_init(&mutex, NULL);
	pthread_mutex_init(&mutex_debug, NULL);

	//init task status
	//init_task_status();

	task_budget = 10;

	init_task_queue();

	/*Create threads*/
	numthrds = MAXTHRDS;

	for(j=0; j<numthrds; j++){
		if(j==0){ //master thread
			pthread_create(&callThd[j], NULL, master_thrd, (void *)j);
			pthread_mutex_lock(&mutex_debug);
			cout << "Debug: create master thread"<<endl;
			//cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
			pthread_mutex_unlock(&mutex_debug);	
		}
		else{
			pthread_create(&callThd[j], NULL, worker_thrd, (void *)j);
			pthread_mutex_lock(&mutex_debug);
			cout << "Debug: create worker thread"<<endl;
			//cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
			pthread_mutex_unlock(&mutex_debug);	
		}
	}

	/*Release the thread attribute handle*/
	//pthread_attr_destroy(&attr);

	/*Wait for the other threads within this node*/
	for(i=0; i<numthrds; i++){
		pthread_join(callThd[i], &status);
		pthread_mutex_lock(&mutex_debug);
		cout << "Debug: I am at multithreading() pthread_join"<<endl;
		//cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
		pthread_mutex_unlock(&mutex_debug);	

	}

	/*Release mutex*/
	pthread_mutex_lock(&mutex_debug);
	cout << "Debug: before destroy mutex"<<endl;
		//cout << "Debug: sending_queue empty=" << sending_queue.empty() << " received_queue empty=" << received_queue.empty() <<endl;
	pthread_mutex_unlock(&mutex_debug);	
	pthread_mutex_destroy(&mutex);
	pthread_mutex_destroy(&mutex_debug);

	cout << "Debug: after destroy mutex"<<endl;

	return 0;

}



int main(int argc, char** argv){

	int world_rank, world_size;
	int provided;

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

	cout << "Rank" << world_rank << "finish job!" << endl;

	MPI_Finalize();

	return 0;


}
