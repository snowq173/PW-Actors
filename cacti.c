#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <stdbool.h>
#include <pthread.h>
#include "cacti.h"

/*									UTILITY FUNCTIONS 								*/


static void mutex_lock(pthread_mutex_t *mutex) {
	int err;
	/* Check for non-zero integer value returned by pthread_mutex_lock.
	 * Display appropriate error message on such event and exit program
	 * with error code 1
	 */
	if((err = pthread_mutex_lock(mutex)) != 0) {
		perror("Error: pthread_mutex_lock");
		exit(1);
	}
}

static void mutex_unlock(pthread_mutex_t *mutex) {
	int err;
	/* Check for non-zero integer value returned by pthread_mutex_unlock.
	 * Display appropriate error message on such event and exit program
	 * with error code 1
	 */
	if((err = pthread_mutex_unlock(mutex)) != 0) {
		perror("Error: pthread_mutex_unlock");
		exit(1);
	}
}

static void cond_wait(pthread_cond_t *condition, pthread_mutex_t *mutex) {
	int err;
	/* Check for non-zero integer value returned by pthread_cond_wait.
	 * Display appropriate error message on such event and exit program
	 * with error code 1
	 */
	if((err = pthread_cond_wait(condition, mutex)) != 0) {
		perror("Error: pthread_cond_wait");
		exit(1);
	}
}

static void cond_signal(pthread_cond_t *condition) {
	int err;
	/* Check for non-zero integer value returned by pthread_cond_signal.
	 * Display appropriate error message on such event and exit program
	 * with error code 1
	 */
	if((err = pthread_cond_signal(condition)) != 0) {
		perror("Error: pthread_cond_signal");
		exit(1);
	}
}

/*									THREAD POOL 									*/

/* Enum which specifies integer values
 * appropriate to errors that might encounter
 * during function execution
 */

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

/* actor_status enum */

typedef enum {
	alive					= 0,
	dead 					= 1,
	uninitialised			= 2
} actor_state_t;

/* thread_pool struct */

typedef struct thread_pool {
	pthread_t *threads;
	pthread_attr_t attr;
	pthread_cond_t await_cond;
	pthread_cond_t finish_cond;
	pthread_mutex_t mutex;

	bool shutdown;

	size_t waiting_messages;
	size_t arrays_size;
	size_t pool_size;
	size_t working_count;
	size_t actors_count;
	size_t alive_actors;
	size_t actors_iter;
	size_t *actor_status;
	size_t *queue_iterators;
	size_t *messages_in_queue;

	message_t ***message_queues;
	role_t **actor_roles;
	void **actor_state_ptr;
	actor_id_t *served_actor;
} thread_pool_t;

/* pointer for used thread pool */

static thread_pool_t *pool = NULL;

static void thread_pool_destroy(thread_pool_t *pool) {
	if(pool == NULL) {
		return;
	}

	for(size_t i = 0; i < POOL_SIZE; ++i) {
		pthread_join(pool->threads[i], NULL);
	}

	/* Destroy pool's pthread_attr_t object,
	 * displays appropriate error message on fail
	 */
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

	free(pool->served_actor);
	free(pool->actor_roles);
	free(pool->actor_status);
	free(pool->actor_state_ptr);
	free(pool->message_queues);
	free(pool->messages_in_queue);
	free(pool->queue_iterators);


	/* Deallocate memory allocated for
	 * pthread_t objects array
	 */
	free(pool->threads);
	free(pool);
}

/* Function executed after catching SIGINT signal
 */
static void catch(int signal, siginfo_t *info, void *more) {
	if(pool == NULL) {
		return;
	}
	else {
		pool->shutdown = true;
		for(size_t i = 0; i < POOL_SIZE; ++i) {
			pthread_join(pool->threads[i], NULL);
		}

		thread_pool_destroy(pool);

		exit(0);
	}
}

static size_t map_thread_to_index() {
	for(size_t i = 0; i < POOL_SIZE; ++i)
		if(pthread_equal(pthread_self(), pool->threads[i]))
			return i;
}


static void handle_godie_msg(size_t actor_id) {

}

static void handle_spawn_msg(size_t actor_id) {

}

static void handle_hello_msg(size_t actor_id) {

}

/* Function executed by each thread in pool
 */
static void *thread_action(void *pool) {
	
	thread_pool_t *pool_ptr = (thread_pool_t *) pool;
	message_t *acquired_message;
	size_t iter;

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

		while(pool_ptr->waiting_messages == 0 && !pool_ptr->shutdown) {
			cond_wait(&pool_ptr->await_cond, &pool_ptr->mutex);
		}

		if(pool_ptr->shutdown) {
			printf("Breaking...\n");
			break;
		}

		iter = pool_ptr->actors_iter;

		while(pool_ptr->messages_in_queue[iter] == 0) {
			iter = (iter + 1) % (pool_ptr->actors_count);
		}

		/* reduce the number of waiting messages */
		pool_ptr->waiting_messages--;
		/* update served actor for thread */
		pool_ptr->served_actor[map_thread_to_index()] = iter;
		/* get message from queue */
		acquired_message = pool_ptr->message_queues[iter][pool_ptr->queue_iterators[iter]];
		
		/* do stuff with queue */
		pool_ptr->queue_iterators[iter] = (pool_ptr->queue_iterators[iter] + 1) % ACTOR_QUEUE_LIMIT;
		pool_ptr->messages_in_queue[iter]--;

		printf("Message processing by... %d\n", actor_id_self());
		/* if actor is told to die, clear the queue and delete messages */
		if(acquired_message->message_type == MSG_GODIE) {

			pool_ptr->actor_status[iter] = dead;
			pool_ptr->alive_actors--;

			size_t actor_queue_iter = pool_ptr->queue_iterators[iter];
			size_t messages_left = pool_ptr->messages_in_queue[iter];

			while(messages_left > 0) {
				free(pool_ptr->message_queues[iter][actor_queue_iter]);

				actor_queue_iter = (actor_queue_iter + 1) % ACTOR_QUEUE_LIMIT;
				messages_left--;
			}

			/* update actors iterator */
			pool_ptr->actors_iter = (iter + 1) % (pool_ptr->actors_count);


			if(pool_ptr->alive_actors == 0) {
				printf("Shutting down...\n");
				pool_ptr->shutdown = true;
				cond_signal(&pool_ptr->await_cond);
			}

			mutex_unlock(&pool_ptr->mutex);
		}
		else if(acquired_message->message_type == MSG_SPAWN) {

			if(pool_ptr->actors_count == pool_ptr->arrays_size) {

				size_t old_size = pool_ptr->arrays_size;

				if(old_size == CAST_LIMIT) {
					perror("Error: CAST_LIMIT; can't create new actor - terminating...");
					exit(1);
				}

				pool_ptr->arrays_size = 2*(pool_ptr->arrays_size);

				if((pool_ptr->message_queues = 
					realloc(pool_ptr->message_queues, 
					2*sizeof(pool_ptr->message_queues))) == NULL) {

					perror("Critical: realloc");
					exit(1);
				}
				printf("ok queues\n");
				if((pool_ptr->actor_roles = 
					realloc(pool_ptr->actor_roles, 
					2*sizeof(pool_ptr->actor_roles))) == NULL) {
					
					perror("Critical: realloc");
					exit(1);
				}
				printf("ok actor roles\n");
				if((pool_ptr->messages_in_queue = 
					realloc(pool_ptr->messages_in_queue, 
					2*sizeof(pool_ptr->messages_in_queue))) == NULL) {
					
					perror("Critical: realloc");
					exit(1);
				}
				printf("ok messages in queue\n");
				if((pool_ptr->actor_status = 
					realloc(pool_ptr->actor_status, 
					2*sizeof(pool_ptr->actor_status))) == NULL) {
					
					perror("Critical: realloc");
					exit(1);
				}
				printf("ok actor states\n");
				if((pool_ptr->queue_iterators = 
					realloc(pool_ptr->queue_iterators, 
					2*sizeof(pool_ptr->queue_iterators))) == NULL) {

					perror("Critical: realloc");
					exit(1);
				}
				printf("ok queue iterators\n");
				for(size_t i = old_size; i < 2*old_size; ++i) {
					pool_ptr->actor_status[i] = uninitialised;

					if((pool_ptr->message_queues[i] = malloc(ACTOR_QUEUE_LIMIT * sizeof(message_t *))) == NULL) {
						perror("Critical: malloc");
						exit(1);
					}
				}

				pool_ptr->arrays_size = 2*old_size;
			}

			size_t new_actor_id = pool_ptr->actors_count;

			printf("NEW ACTOR ID: %d, BY: %d\n", new_actor_id, actor_id_self());

			pool_ptr->actors_count++;
			pool_ptr->actor_status[new_actor_id] = alive;
			pool_ptr->actor_roles[new_actor_id] = acquired_message->data;
			pool_ptr->alive_actors++;

			/* create 'HELLO' message which is to be sent to new actor */
			message_t *message = malloc(sizeof(message_t));

			if(message == NULL) {

				perror("Critical: malloc");
				exit(1);
			}

			message->message_type = MSG_HELLO;
			message->data = (void *) actor_id_self();

			pool_ptr->message_queues[new_actor_id][0] = message;
			pool_ptr->messages_in_queue[new_actor_id] = 1;
			pool_ptr->queue_iterators[new_actor_id] = 0;
			pool_ptr->actor_state_ptr[new_actor_id] = NULL;
			pool_ptr->waiting_messages++;

			printf("Current arrays size: %d, waiting messages: %d, actors: %d\n", pool_ptr->arrays_size, pool_ptr->waiting_messages,
				pool_ptr->actors_count);

			/* update actors iterator */
			pool_ptr->actors_iter = (iter + 1) % (pool_ptr->actors_count);

			mutex_unlock(&pool_ptr->mutex);
		}
		else if(acquired_message->message_type == MSG_HELLO) {

			printf("ACTOR: %d GOT HELLO FROM: %d\n", (size_t) iter, acquired_message->data);

			act_t fun = pool_ptr->actor_roles[iter]->prompts[MSG_HELLO];

			/* update actors iterator */
			pool_ptr->actors_iter = (iter + 1) % (pool_ptr->actors_count);

			mutex_unlock(&pool_ptr->mutex);

			(*fun)(&pool_ptr->actor_state_ptr[iter], 0, NULL);
		}
		else {
			perror("Critical: unknown message type - terminating...");
			exit(1);
		}

		free(acquired_message);

	} /* while */


	pool_ptr->working_count--;

	if(pool_ptr->working_count > 0) {
		cond_signal(&pool_ptr->await_cond);
		mutex_unlock(&pool_ptr->mutex);
	}
	else {
		printf("Last..\n");
		cond_signal(&pool_ptr->finish_cond);
		mutex_unlock(&pool_ptr->mutex);
	}



	return NULL;
}

#define DEFAULT_SIZE 256

static int thread_pool_init(size_t thread_count) {
	/* integer variable for checking value returned by
	 * pthread library init functions
	 */
	int err;

	/* NULL pointer passed as argument, return
	 * null_pointer error code
	 */
	if(pool == NULL)
		return null_pointer;

	/* Try to init pthread_attr_t object for pool,
	 * return attr_init_error error code on fail
	 */
	if(pthread_attr_init(&pool->attr))
		return attr_init_error;

	/* Try to allocate memory for array of pthread_t objects;
	 * return memory_error code on fail (NULL returned by malloc)
	 */
	if((pool->threads = malloc(thread_count * sizeof(pthread_t))) == NULL)
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

	for(size_t i = 0; i < DEFAULT_SIZE; ++i)
		if((pool->message_queues[i] = malloc(ACTOR_QUEUE_LIMIT * sizeof(message_t *))) == NULL)
			return memory_error;

	for(size_t i = 0; i < DEFAULT_SIZE; ++i) {
		pool->actor_status[i] = uninitialised;
		pool->queue_iterators[i] = 0;
		pool->messages_in_queue[i] = 0;
		pool->actor_roles[i] = NULL;
	}

	for(size_t i = 0; i < POOL_SIZE; ++i) {
		pool->served_actor[i] = 0;
	}

	pool->arrays_size = DEFAULT_SIZE;
	pool->pool_size = thread_count;
	pool->actors_count = 0;
	pool->actors_iter = 0;
	pool->waiting_messages = 0;
	pool->working_count = POOL_SIZE;
	pool->alive_actors = 0;
	pool->shutdown = false;

	/* Try to initialise pool's mutex; return mutex_init_error
	 * code on fail (when pthread_mutex_init returns non-zero
	 * integer value)
	 */
	if((err = pthread_mutex_init(&pool->mutex, NULL)) != 0)
		return mutex_init_error;

	if((err = pthread_cond_init(&pool->await_cond, NULL)) != 0)
		return cond_init_error;

	if((err = pthread_cond_init(&pool->finish_cond, NULL)) != 0)
		return cond_init_error;

	/* Try to create threads for pool; destroy pool and return 
	 * pthread_create_error integer value on fail (non-zero integer value
	 * returned by pthread_create function)
	 */
	for(size_t i = 0; i < thread_count; i++) {
		if((err = pthread_create(&pool->threads[i], &pool->attr, thread_action, (void *) pool)) != 0) {
			thread_pool_destroy(pool);
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


/* Returns the id of actor
 */
actor_id_t actor_id_self() {
	/* Find the index in array of threads which correspond to the
	 * thread which is executing the action and return the id of actor
	 * who is served by it at the moment
	 */
	for(size_t i = 0; i < pool->pool_size; ++i)
		if(pthread_equal(pool->threads[i], pthread_self()))
			return pool->served_actor[i];
}

/* Creates new actor system, sets id of first actor in the actor
 * integer variable, sets the role of first actor according to the
 * passed pointer
 */
int actor_system_create(actor_id_t *actor, role_t *const role) {
	int err;

	pool = malloc(sizeof(thread_pool_t));

	/* Try to init the pool, return -1 on error code
	 * (appropriate non-zero integer value specified in
	 * threadpool.h header) returned by thread_pool_init
	 * function. Return 0 otherwise (that is, on success)
	 */
	if(err = thread_pool_init(POOL_SIZE))
		return -1;

	*actor = 0;

	/* set alive status */
	pool->actor_status[*actor] = alive;
	/* set role */
	pool->actor_roles[*actor] = role;
	/* set initial state_ptr */
	pool->actor_state_ptr[*actor] = NULL;
	/* initialise empty queue */
	pool->messages_in_queue[*actor] = 0;
	/* update count of actors */
	pool->actors_count = 1;
	/* update count of alive actors */
	pool->alive_actors++;

	return *actor;
}

/* Waits for actor system to finish its action
 */
void actor_system_join(actor_id_t actor) {
	/* Check if pool has been initialised and if not
	 * then return from the function as there is nothing to do
	 */
	if(pool == NULL){
		printf("NULL POINTER\n");
		return;
	}

	mutex_lock(&pool->mutex);

	/* Wait for the pool's threads to finish their work, stay
	 * in while(true) loop so as to prevent accidental pool destroy
	 * when spuriously woken up from pthread_cond_t await_cond
	 * pool's variable
	 */
	while(true) {
		printf("HERE\n");
		/* Check if the pool has to shut down but there are some
		 * threads that are still working - in such case we need
		 * to wait till their work is finished
		 */
		bool shutdown_but_threads_working = pool->shutdown &&
											(pool->working_count > 0);


		if(shutdown_but_threads_working) {
			/* Wait for the threads to finish on conditional variable
			 */
			cond_wait(&pool->finish_cond, &pool->mutex);
		}
		else {
			/* exit the loop as the pool has shut down and
			 * each thread finished working
			 */
			break;
		}
	}

	/* Unlock the pool's mutex */
	mutex_unlock(&pool->mutex);
	/* And finally destroy the thread pool */
	thread_pool_destroy(pool);

	printf("Leaving the %s\n", __func__);
}

/* Sends message to specified actor
 */
int send_message(actor_id_t actor, message_t message) {

	if(pool == NULL)
		return -1;

	mutex_lock(&pool->mutex);

	if(pool->shutdown) {
		mutex_unlock(&pool->mutex);
		return -1;
	}

	/* check if targetted actor exists in system */
	if(actor >= pool->actors_count) {
		mutex_unlock(&pool->mutex);
		return -2;
	}

	/* check if targetted actor is dead,
	 */
	if(pool->actor_status[actor] == dead) {
		mutex_unlock(&pool->mutex);
		return -1;
	}

	/* check if targetted actor's queue is full */
	if(pool->messages_in_queue[actor] == ACTOR_QUEUE_LIMIT) {
		mutex_unlock(&pool->mutex);
		return -3;
	}

	size_t iter = (pool->queue_iterators[actor] + pool->messages_in_queue[actor]) % ACTOR_QUEUE_LIMIT;


	/* allocate memory for copy of message which is to be sent */
	message_t *message_copy = malloc(sizeof(message_t));

	if(message_copy == NULL) {
		mutex_unlock(&pool->mutex);

		/* error occured while allocating memory, return -1 */
		return -1;
	}

	/* set message copy's parameters according to sent message parameters */
	message_copy->message_type = message.message_type;
	message_copy->nbytes = message.nbytes;
	message_copy->data = message.data;

	/* insert the pointer to the message copy to the actor's queue */
	pool->message_queues[actor][iter] = message_copy;
	pool->messages_in_queue[actor]++;
	pool->waiting_messages++;

	/* signal await_cond as the work is to be done */
	if(pool->waiting_messages == 1) {
		cond_signal(&pool->await_cond);
	}

	mutex_unlock(&pool->mutex);

	return 0;
}