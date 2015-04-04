// Replace "Full name" and "netid" here with your name and netid

// Copyright (C) [Zixuan Yi] ([zyi3]) 2015

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>

#include "mpsortutil.h"

#define QUEUE_SIZE (64)

extern int nthreads, verbose; // defined in main.c
extern char * outfile_name;

static FILE * outfile;
static int* data;
static int nitems;
static int capacity;

int start_merge = 0;

typedef struct _node{
	int start;
	int end;
	int size;
}node_t;

int q_count, m_count, q_in, m_in, q_out, m_out;

pthread_cond_t en1 = PTHREAD_COND_INITIALIZER;
pthread_cond_t de1 = PTHREAD_COND_INITIALIZER;	

node_t* q_queue[QUEUE_SIZE];

pthread_mutex_t m1 = PTHREAD_MUTEX_INITIALIZER;	

static int compare_fn(const void *arg1, const void *arg2) {
  return (*((int*)arg1)) - (*((int*)arg2)); 
}

/**
 * Stream-based fast sort. The stream sort may be faster because you can start processing the data
 * before it has been fully read.
 * Before data arrives, you can pre-create threads and prepared to send output to the given file
 * @param nthreads number of threads to use including the calling thread (these should created once and reused for multiple tesks)
 * @param verbose whether to print to standard out the number of unique values for each merged segment
 * @param outfile output FILE descriptor (either stdout or a output file)
*/

void q_enqueue(node_t* node) {		//q_queue
	pthread_mutex_lock(&m1);
	while(q_count >= QUEUE_SIZE){
		pthread_cond_wait(&en1, &m1);
	}
	q_queue[(q_in) % QUEUE_SIZE] = node;
	(q_count)++;
	(q_in)++;
	pthread_cond_broadcast(&de1);
	pthread_mutex_unlock(&m1);	
}

int q_null_flag = 0;

node_t* q_dequeue() {			//q_queue
	if(q_null_flag){
		return NULL;
	}
	pthread_mutex_lock(&m1); 
  	while((q_count) <= 0){
		pthread_cond_wait(&de1, &m1);
	}
	node_t *cur = q_queue[(q_out) % QUEUE_SIZE];
	if(cur == NULL){
		q_null_flag = 1;
		pthread_cond_broadcast(&en1);
		pthread_mutex_unlock(&m1);
		return NULL;
	}
	(q_count)--;
	(q_out)++;
	pthread_cond_broadcast(&en1);
	pthread_mutex_unlock(&m1);
	return cur;
}

void p_merge(){

}
/**
 * Stream-based fast sort. The stream sort may be faster because you can start processing the data
 * before it has been fully read.
 * Before data arrives, you can pre-create threads and prepared to send output to the given file
 * @param nthreads number of threads to use including the calling thread (these should created once and reused for multiple s)
 * @param verbose whether to print to standard out the number of unique values for each merged segment
 * @param outfile output FILE descriptor (either stdout or a output file)
*/

pthread_cond_t s_leep = PTHREAD_COND_INITIALIZER;
pthread_mutex_t m2 = PTHREAD_MUTEX_INITIALIZER;

void barrier(){
	pthread_mutex_lock(&m2);
	if(!start_merge){
		while(!start_merge)
			pthread_cond_wait(&s_leep, &m2);
	}
	else{
		pthread_cond_broadcast(&s_leep);
	}
	pthread_mutex_unlock(&m2);
}

int No = 0;

void* myworker_func(void* arg){
	node_t * node;
	while((node = q_dequeue())){
		qsort(data + node->start, node->size, sizeof(int), compare_fn);
		if(verbose) {
     			print_stat(data, node->start, node->end);
		}
		free(node); 
	}

	barrier();	//wait until merge start;

	if(nitmes<=256)
		return NULL;
	

	int seg_len = nitems / nthreads;	
	p_merge();

	return NULL;
}

pthread_t *tid;		

void stream_init() {
	outfile = open_outfile(outfile_name);
  	// do awesome stuff
  	
	data = malloc(16777216*sizeof(int));

  	tid = malloc((nthreads-1) * sizeof(pthread_t));
	for(int i=0; i<nthreads-1; i++){
		pthread_create(&tid[i], NULL, myworker_func, NULL);
	}
	//write(2, "init complete.   ", 14);
}

/**
 * Additional data has arrived and is ready to be processed in the buffer. 
 * This function may be called multiple times (between stream_init() and stream_en()d).
 * The buffer is re-used for new data, you will need to copy / process the data before returning from this method.
 * @param buffer pointer to the buffer. The buffer contents is only valid for the duration of the call.
 * @param count the number of items in the buffer (256 >= count > 0). This may be less than 256 for the last segment.
*/

void stream_data(int* buffer, int count) {
	// You can already start sorting before the data is fully read into memory
	// do awesome stuff
	int old_n = nitems;
	nitems += count;
	node_t * new_node = malloc(sizeof(node_t));
	new_node->size = count;
	new_node->start = old_n;
	new_node->end = nitems;

	for(int i=0; i<count; i++){
		data[old_n+i] = buffer[i];
	}
	q_enqueue(new_node);
}
/**
 * All data has been delivered. Your implementation should now finish writing out sorted data and verbose info.
* The output stream will be closed for you after this call returns
*/
void stream_end() {
// do awesome stuff
	start_merge = 1;
	q_enqueue(NULL);

	create_tesks(NULL, 0, nitems);
	myworker_func(NULL);	//Don't waste this main thread!

	//write(2, "main thread", 11);

	void* t;
	for(int i=0; i<(nthreads-1); i++){
		pthread_join(tid[i], &t);
	}

// then print to outfile e.g.
  for(int i = 0; i < nitems; i++) 
     fprintf(outfile, "%d\n", data[i]);
//  if(verbose) {
//     	print_stat(data, 0, nitems);
//	write(2,"FIN",3);
//  }


  if(outfile != stdout) 
     fclose(outfile);
}

