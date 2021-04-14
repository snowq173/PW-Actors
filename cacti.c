#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <stdbool.h>
#include <pthread.h>
#include "cacti.h"

/************************************************************************************/
/*																					*/
/*									UTILITY FUNCTIONS 								*/
/*																					*/
/************************************************************************************/

static void mutex_lock(pthread_mutex_t *mutex) {
	int err;
	if((err = pthread_mutex_lock(mutex)) != 0) {
		perror("Error: pthread_mutex_lock");
		exit(1);
	}
}

static void mutex_unlock(pthread_mutex_t *mutex) {
	int err;
	if((err = pthread_mutex_unlock(mutex)) != 0) {
		perror("Error: pthread_mutex_unlock");
		exit(1);
	}
}

static void cond_wait(pthread_cond_t *condition, pthread_mutex_t *mutex) {
	int err;
	if((err = pthread_cond_wait(condition, mutex)) != 0) {
		perror("Error: pthread_cond_wait");
		exit(1);
	}
}

static void cond_signal(pthread_cond_t *condition) {
	int err;
	if((err = pthread_cond_signal(condition)) != 0) {
		perror("Error: pthread_cond_signal");
		exit(1);
	}
}

/************************************************************************************/
/*																					*/
/*																					*/
/*									THREAD POOL 									*/
/*																					*/
/*																					*/
/************************************************************************************/

typedef enum {
	success					=  0,
	null_pointer			= -1,
	memory_error			= -2,
	attr_init_error			= -3,
	mutex_init_error		= -4,
	cond_init_error			= -5,
	attr_destroy_error		= -6,
	pthread_create_error	= -7
} error_t;

typedef enum {
	alive					= 0,
	dead 					= 1,
	uninitialised			= 2
} actor_state_t;

typedef enum {
	waiting 				= 0,
	working 				= 1
} work_state_t;

typedef struct thread_pool {
	pthread_t *threads;
	pthread_attr_t attr;
	pthread_cond_t await_cond;
	pthread_cond_t finish_cond;
	pthread_mutex_t mutex;

	bool shutdown;
	bool active_join;

	size_t waiting_threads;
	size_t waiting_messages;
	size_t arrays_size;
	size_t pool_size;
	size_t working_count;
	size_t actors_count;
	size_t alive_actors;
	size_t actors_to_serve;
	size_t work_queue_iter;
	size_t work_queue_size;
	size_t work_queue_count;

	size_t *actor_status;
	size_t *work_state;
	size_t *queue_iterators;
	size_t *messages_in_queue;
	size_t *work_queue;

	message_t ***message_queues;
	role_t **actor_roles;
	void **actor_state_ptr;
	actor_id_t *served_actor;
} thread_pool_t;

static thread_pool_t *pool = NULL;

static void thread_pool_destroy() {
	if(pool == NULL) {
		return;
	}

	for(size_t i = 0; i < POOL_SIZE; ++i) {
		pthread_join(pool->threads[i], NULL);
	}

	if(pthread_attr_destroy(&pool->attr)) {
		perror("Error in attr_destroy");
		exit(1);
	}

	if(pthread_mutex_destroy(&pool->mutex)) {
		perror("Error in mutex_destroy");
		exit(1);
	}


	for(size_t i = 0; i < pool->arrays_size; ++i) {
		free(pool->message_queues[i]);
	}

	free(pool->work_state);
	free(pool->actor_roles);
	free(pool->actor_status);
	free(pool->actor_state_ptr);
	free(pool->message_queues);
	free(pool->messages_in_queue);
	free(pool->queue_iterators);
	free(pool->work_queue);

	free(pool->served_actor);
	free(pool->threads);
	free(pool);

	pool = NULL;
}

static void catch(__attribute__((unused)) int signal, 
				  __attribute__((unused)) siginfo_t *info, 
				  __attribute__((unused)) void *more) {

	if(pool == NULL) {
		return;
	}
	else {

		mutex_lock(&pool->mutex);
		pool->shutdown = true;

		if(pool->waiting_threads > 0) {
			cond_signal(&pool->await_cond);
			mutex_unlock(&pool->mutex);
			mutex_lock(&pool->mutex);
		}

		if(!(pool->active_join)) {
			mutex_unlock(&pool->mutex);
			thread_pool_destroy();
		}
		else {
			cond_signal(&pool->finish_cond);
			mutex_unlock(&pool->mutex);
		}
	}
}

static void append_to_queue(actor_id_t target_id) {
	size_t current_size = pool->work_queue_size;
	size_t current_iter = pool->work_queue_iter;

	if(pool->work_queue_count == current_size - 1) {

		void *realloc_ptr = realloc(pool->work_queue,
									2 * current_size * sizeof(size_t));

		if(realloc_ptr == NULL) {
			perror("Error in realloc...");
			exit(1);
		}

		pool->work_queue = (size_t *) realloc_ptr;

		for(size_t i = 0; i < pool->work_queue_iter; ++i) {
			pool->work_queue[current_size + i] = pool->work_queue[i];
		}

		for(size_t i = 0; i < current_size; ++i) {
			pool->work_queue[i] = pool->work_queue[i + current_iter];
		}

		pool->work_queue_size = 2 * current_size;
		pool->work_queue_iter = 0;
	}

	size_t pos = (pool->work_queue_iter + pool->work_queue_count) % (pool->work_queue_size);
	pool->work_queue[pos] = target_id;
	pool->work_queue_count++;
}

static actor_id_t queue_pop() {
	actor_id_t current = pool->work_queue[pool->work_queue_iter];
	
	pool->work_queue_iter = (pool->work_queue_iter + 1) % (pool->work_queue_size);
	pool->work_queue_count--;

	return current;
}

static size_t map_thread_to_index() {
	size_t index;

	for(size_t i = 0; i < POOL_SIZE; ++i) {
		if(pthread_equal(pthread_self(), pool->threads[i])) {
			index = i;
		}
	}

	return index;
}

static void handle_godie_msg(size_t actor_id) {
	pool->actor_status[actor_id] = dead;
	mutex_unlock(&pool->mutex);
}

static void reallocate_arrays() {
	size_t old_size = pool->arrays_size;

	if(old_size == CAST_LIMIT) {
		perror("Error: CAST_LIMIT; can't create new actor - terminating...");
		exit(1);
	}

	void *alloc_ptr;

	if((alloc_ptr = realloc(pool->message_queues, 2*old_size*ACTOR_QUEUE_LIMIT*sizeof(message_t *))) == NULL) {

		perror("Critical: realloc");
		exit(1);
	}
	pool->message_queues = alloc_ptr;

	if((alloc_ptr = realloc(pool->actor_roles, 2*old_size*sizeof(role_t *))) == NULL) {
					
		perror("Critical: realloc");
		exit(1);
	}
	pool->actor_roles = alloc_ptr;

	if((alloc_ptr = realloc(pool->messages_in_queue, 2*old_size*sizeof(size_t))) == NULL) {
					
		perror("Critical: realloc");
		exit(1);
	}
	pool->messages_in_queue = alloc_ptr;

	if((alloc_ptr = realloc(pool->actor_status, 2*old_size*sizeof(size_t))) == NULL) {
					
		perror("Critical: realloc");
		exit(1);
	}
	pool->actor_status = alloc_ptr;
	
	if((alloc_ptr = realloc(pool->queue_iterators, 2*old_size*sizeof(size_t))) == NULL) {

		perror("Critical: realloc");
		exit(1);
	}
	pool->queue_iterators = alloc_ptr;

	if((alloc_ptr = realloc(pool->actor_state_ptr, 2*old_size*sizeof(void *))) == NULL) {

		perror("Critical: realloc");
		exit(1);
	}
	pool->actor_state_ptr = alloc_ptr;

	if((alloc_ptr = realloc(pool->work_state, 2*old_size*sizeof(size_t))) == NULL) {
		perror("Critical: realloc");
		exit(1);
	}
	pool->work_state = alloc_ptr;
	
	for(size_t i = old_size; i < 2*old_size; ++i) {
		pool->actor_status[i] = uninitialised;
		pool->queue_iterators[i] = 0;
		pool->messages_in_queue[i] = 0;
		pool->actor_state_ptr[i] = NULL;

		if((pool->message_queues[i] = malloc(ACTOR_QUEUE_LIMIT * sizeof(message_t *))) == NULL) {
			perror("Critical: malloc");
			exit(1);
		}
	}

	pool->arrays_size = 2*old_size;
}

static void handle_spawn_msg(message_t *acquired_message) {

	if(pool->actors_count == CAST_LIMIT) {
		mutex_unlock(&pool->mutex);
		return;
	}

	if(pool->actors_count == pool->arrays_size) {

		reallocate_arrays();
	}

	size_t new_actor_id = pool->actors_count;

	pool->actor_status[new_actor_id] = alive;
	pool->actor_roles[new_actor_id] = acquired_message->data;

	pool->actors_count++;
	pool->alive_actors++;

	message_t *message = malloc(sizeof(message_t));

	if(message == NULL) {

		perror("Critical: malloc");
		exit(1);
	}

	message->message_type = MSG_HELLO;
	message->data = (void *) actor_id_self();

	pool->message_queues[new_actor_id][0] = message;
	pool->messages_in_queue[new_actor_id] = 1;
	pool->waiting_messages++;
	pool->actors_to_serve++;
	pool->work_state[new_actor_id] = waiting;

	append_to_queue(new_actor_id);

	if(pool->waiting_threads > 0) {

		cond_signal(&pool->await_cond);
	}

	mutex_unlock(&pool->mutex);
}

static void handle_other_msg(size_t actor_id, message_t *message) {

	act_t fun = pool->actor_roles[actor_id]->prompts[message->message_type];

	mutex_unlock(&pool->mutex);

	(*fun)(&pool->actor_state_ptr[actor_id], 0, message->data);
}

/* Function executed by each thread in pool
 */
static void *thread_action(void *pool) {

	thread_pool_t *pool_ptr = (thread_pool_t *) pool;
	message_t *acquired_message;
	size_t current_actor;

	struct sigaction action;
	sigset_t block_mask;
	sigemptyset(&block_mask);

	action.sa_sigaction = catch;
	action.sa_mask = block_mask;
	action.sa_flags = SA_SIGINFO;

	if(sigaction(SIGINT, &action, NULL) != 0) {
		perror("Initialising sigaction failed.");
		exit(1);
	}

	while(true) {
		mutex_lock(&pool_ptr->mutex);

		if(pool_ptr->shutdown) {

			break;
		}

		pool_ptr->waiting_threads++;

		while(pool_ptr->actors_to_serve == 0 && !pool_ptr->shutdown) {
			cond_wait(&pool_ptr->await_cond, &pool_ptr->mutex);
		}

		pool_ptr->waiting_threads--;

		if(pool_ptr->shutdown) {

			break;
		}

		current_actor = queue_pop();

		pool_ptr->waiting_messages--;
		pool_ptr->actors_to_serve--;

		pool_ptr->served_actor[map_thread_to_index()] = current_actor;

		acquired_message = pool_ptr->message_queues[current_actor][pool_ptr->queue_iterators[current_actor]];

		/* Update queue of actor */
		pool_ptr->queue_iterators[current_actor] = (pool_ptr->queue_iterators[current_actor] + 1) % 
													ACTOR_QUEUE_LIMIT;


		pool_ptr->messages_in_queue[current_actor]--;

		pool_ptr->work_state[current_actor] = working;

		bool check_dead = (pool_ptr->actor_status[current_actor] == dead || 
						   acquired_message->message_type == MSG_GODIE);

		if(pool_ptr->messages_in_queue[current_actor] == 0 && 
		   check_dead) {

			pool_ptr->alive_actors--;
		}

		/* Process message */

		if(acquired_message->message_type == MSG_GODIE) {

			handle_godie_msg(current_actor);
		}
		else if(acquired_message->message_type == MSG_SPAWN) {

			handle_spawn_msg(acquired_message);
		}
		else if(acquired_message->message_type >= 0 &&
				(size_t) acquired_message->message_type < pool_ptr->actor_roles[current_actor]->nprompts) {

			handle_other_msg(current_actor, acquired_message);
		}
		else {
			perror("Critical: unknown message type - terminating...");
			exit(1);
		}

		free(acquired_message);

		/* Update working status and wake threads if there is need to */

		mutex_lock(&pool_ptr->mutex);


		pool_ptr->work_state[current_actor] = waiting;

		if(pool_ptr->messages_in_queue[current_actor] > 0) {

			append_to_queue(current_actor);
			pool_ptr->actors_to_serve++;

			if(pool_ptr->waiting_threads > 0) {

				cond_signal(&pool_ptr->await_cond);
			}
		}

		if(pool_ptr->alive_actors == 0) {

			pool_ptr->shutdown = true;
			cond_signal(&pool_ptr->await_cond);
		}

		mutex_unlock(&pool_ptr->mutex);
	}

	pool_ptr->working_count--;

	if(pool_ptr->working_count > 0) {
		cond_signal(&pool_ptr->await_cond);
		mutex_unlock(&pool_ptr->mutex);
	}
	else {
		cond_signal(&pool_ptr->finish_cond);
		mutex_unlock(&pool_ptr->mutex);
	}

	return NULL;
}

#define DEFAULT_SIZE 512

static int thread_pool_init() {
	int err;

	if(pool == NULL)
		return null_pointer;

	if(pthread_attr_init(&pool->attr))
		return attr_init_error;

	if((pool->threads = malloc(POOL_SIZE * sizeof(pthread_t))) == NULL)
		return memory_error;

	if((pool->served_actor = malloc(POOL_SIZE * sizeof(actor_id_t))) == NULL)
		return memory_error;

	if((pool->actor_roles = malloc(DEFAULT_SIZE * sizeof(role_t *))) == NULL)
		return memory_error;

	if((pool->actor_status = malloc(DEFAULT_SIZE * sizeof(size_t))) == NULL)
		return memory_error;

	if((pool->actor_state_ptr = malloc(DEFAULT_SIZE * sizeof(void *))) == NULL)
		return memory_error;

	if((pool->message_queues = malloc(DEFAULT_SIZE * sizeof(message_t **))) == NULL)
		return memory_error;

	if((pool->messages_in_queue = malloc(DEFAULT_SIZE * sizeof(size_t))) == NULL)
		return memory_error;

	if((pool->queue_iterators = malloc(DEFAULT_SIZE * sizeof(size_t))) == NULL)
		return memory_error;

	if((pool->work_state = malloc(DEFAULT_SIZE * sizeof(size_t))) == NULL)
		return memory_error;

	if((pool->work_queue = malloc(DEFAULT_SIZE * sizeof(size_t))) == NULL)
		return memory_error;

	for(size_t i = 0; i < DEFAULT_SIZE; ++i)
		if((pool->message_queues[i] = malloc(ACTOR_QUEUE_LIMIT * sizeof(message_t *))) == NULL)
			return memory_error;

	for(size_t i = 0; i < DEFAULT_SIZE; ++i) {
		pool->actor_status[i] = uninitialised;
		pool->queue_iterators[i] = 0;
		pool->messages_in_queue[i] = 0;
		pool->actor_roles[i] = NULL;
		pool->actor_state_ptr[i] = NULL;
		pool->work_state[i] = waiting;
	}

	for(size_t i = 0; i < POOL_SIZE; ++i) {
		pool->served_actor[i] = 0;
	}

	pool->arrays_size = DEFAULT_SIZE;
	pool->pool_size = POOL_SIZE;
	pool->actors_count = 0;
	pool->waiting_messages = 0;
	pool->waiting_threads = 0;
	pool->actors_to_serve = 0;
	pool->work_queue_size = DEFAULT_SIZE;
	pool->work_queue_iter = 0;
	pool->work_queue_count = 0;
	pool->working_count = POOL_SIZE;
	pool->alive_actors = 0;
	pool->shutdown = false;
	pool->active_join = false;

	if((err = pthread_mutex_init(&pool->mutex, NULL)) != 0)
		return mutex_init_error;

	if((err = pthread_cond_init(&pool->await_cond, NULL)) != 0)
		return cond_init_error;

	if((err = pthread_cond_init(&pool->finish_cond, NULL)) != 0)
		return cond_init_error;

	for(size_t i = 0; i < POOL_SIZE; i++) {
		if((err = pthread_create(&pool->threads[i], &pool->attr, thread_action, (void *) pool)) != 0) {
			thread_pool_destroy();
			return pthread_create_error;
		}
	}

	return success;
}

/************************************************************************************/
/*																					*/
/*																					*/
/*									ACTORS SYSTEM 									*/
/*																					*/
/*																					*/
/************************************************************************************/

actor_id_t actor_id_self() {

	return pool->served_actor[map_thread_to_index()];
}

void actor_system_join(actor_id_t actor) {

	if(pool == NULL){
		return;
	}

	mutex_lock(&pool->mutex);

	if((size_t) actor >= pool->actors_count) {
		mutex_unlock(&pool->mutex);
		perror("Actor with specified id does not exist...");
		return;
	}

	pool->active_join = true;

	while(true) {

		if(pool->working_count > 0) {

			cond_wait(&pool->finish_cond, &pool->mutex);
		}
		else {

			break;
		}
	}

	pool->active_join = false;
	mutex_unlock(&pool->mutex);
	thread_pool_destroy();
}

int send_message(actor_id_t actor, message_t message) {
	if(pool == NULL)
		return -1;

	mutex_lock(&pool->mutex);

	if(pool->shutdown) {
		mutex_unlock(&pool->mutex);
		return -1;
	}

	if((size_t) actor >= pool->actors_count) {
		mutex_unlock(&pool->mutex);
		return -2;
	}

	if(pool->actor_status[actor] == dead) {
		mutex_unlock(&pool->mutex);
		return -1;
	}

	if(pool->messages_in_queue[actor] == ACTOR_QUEUE_LIMIT) {
		mutex_unlock(&pool->mutex);
		return -3;
	}

	size_t iter = (pool->queue_iterators[actor] + pool->messages_in_queue[actor]) % ACTOR_QUEUE_LIMIT;

	message_t *message_copy = malloc(sizeof(message_t));

	if(message_copy == NULL) {
		mutex_unlock(&pool->mutex);
		return -1;
	}

	message_copy->message_type = message.message_type;
	message_copy->nbytes = message.nbytes;
	message_copy->data = message.data;

	pool->message_queues[actor][iter] = message_copy;
	pool->messages_in_queue[actor]++;
	pool->waiting_messages++;

	if(pool->work_state[actor] == waiting && pool->messages_in_queue[actor] == 1) {

		append_to_queue(actor);
		pool->actors_to_serve++;

		if(pool->waiting_threads > 0) {
			cond_signal(&pool->await_cond);
		}
	}

	mutex_unlock(&pool->mutex);

	return 0;
}

int actor_system_create(actor_id_t *actor, role_t *const role) {
	pool = malloc(sizeof(thread_pool_t));

	if(pool == NULL)
		return -1;

	if(thread_pool_init() != 0)
		return -1;

	*actor = 0;

	pool->actor_status[0] = alive;
	pool->actor_roles[0] = role;
	pool->actor_state_ptr[0] = NULL;
	pool->work_state[0] = waiting;
	pool->messages_in_queue[0] = 0;
	pool->actors_count = 1;
	pool->alive_actors = 1;

	send_message(0, (message_t) { .message_type = MSG_HELLO,
								  .nbytes = 0,
								  .data = NULL });

	return 0;
}
