#include "worker.h"


void *worker_thrd(void *arg){
	int i, myrank;
	long mythrd;

	mythrd = (long)arg;
	MPI_Comm_rank (MPI_COMM_WORLD, &myrank);

	pthread_mutex_lock(&mutex);
	printf("Worker say hi from rank %d thread %ld. \n", myrank, mythrd);
	pthread_mutex_unlock(&mutex);

	pthread_exit((void*)0);

}