#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include "cacti.h"

/* Actor functions prototypes */
void hello_handler(void **, size_t, void *);
void callback_handler(void **, size_t, void *);
void init_handler(void **, size_t, void *);
void hello_second(void **, size_t, void *);
void clear_handler(void **, size_t, void *);
void count_handler(void **, size_t, void *);

/* Message codes */
#define MSG_CLEAR 1
#define MSG_CALLBACK 2
#define MSG_COUNT 3
#define MSG_INIT 3

/* Struct used as actor computation state */
typedef struct actor_state {
	bool first;
	bool last;

	actor_id_t parent;
	actor_id_t id;

	unsigned long long *result;
	unsigned long long current;
	unsigned long long limit;
} actor_state_t;

/* Struct used as initial state of computation */
typedef struct state {
	unsigned long long int target;
	unsigned long long int *result_pointer;
} state_t;

/* Actor functions arrays */
act_t prompts_array[] = { hello_handler, clear_handler, callback_handler, init_handler };
act_t prompts_second[] = { hello_second, clear_handler, callback_handler, count_handler };

/* Actor roles */
role_t roles = (role_t) { .nprompts = 4, .prompts = prompts_array};
role_t roles_more = (role_t) { .nprompts = 4, .prompts = prompts_second};

void hello_handler(void **stateptr, 
				   __attribute__((unused)) size_t nbytes,
				   __attribute__((unused)) void *data) {

	*stateptr = malloc(sizeof(actor_state_t));

	actor_state_t *ptr = (actor_state_t *) *stateptr;

	ptr->first = true;
	ptr->id = actor_id_self();
	ptr->last = false;
	ptr->current = 0;
}

void init_handler(void **stateptr,
				  __attribute__((unused)) size_t nbytes,
				  void *data) {

	state_t *data_ptr = (state_t *) data;
	actor_state_t *my_state = (actor_state_t *) *stateptr;

	my_state->limit = data_ptr->target;
	my_state->result = data_ptr->result_pointer;

	send_message(actor_id_self(), (message_t) { .message_type = MSG_SPAWN,
												.data = &roles_more});
}

void callback_handler(void **stateptr,
					  __attribute__((unused)) size_t nbytes,
					  void *data) {

	actor_state_t *son_state = (actor_state_t *) data;
	actor_state_t *my_state = (actor_state_t *) *stateptr;

	son_state->current = my_state->current + 1;
	son_state->limit = my_state->limit;
	son_state->first = false;
	son_state->last = (son_state->current == son_state->limit);
	son_state->result = my_state->result;

	send_message(son_state->id, (message_t) { .message_type = MSG_COUNT,
											  .data = NULL});
}

void hello_second(void **stateptr,
				  __attribute__((unused)) size_t nbytes,
				  void *data) {

	*stateptr = malloc(sizeof(actor_state_t));
	actor_state_t *my_state = (actor_state_t *) *stateptr;

	my_state->parent = (actor_id_t) data;
	my_state->id = actor_id_self();

	send_message(my_state->parent, (message_t) { .message_type = MSG_CALLBACK,
												 .nbytes = 0,
												 .data = *stateptr});
}

void count_handler(void **stateptr,
		   		   __attribute__((unused)) size_t nbytes,
		  		   __attribute__((unused)) void *data) {

	actor_state_t *my_state = (actor_state_t *) *stateptr;

	unsigned long long *result_ptr = my_state->result;
	*result_ptr = (*result_ptr) * (my_state->current);

	if(my_state->last) {
		send_message(actor_id_self(), (message_t) { .message_type = MSG_CLEAR,
													.nbytes = 0,
													.data = NULL});
	}
	else {
		send_message(actor_id_self(), (message_t) { .message_type = MSG_SPAWN,
													.data = &roles_more });
	}
}

void clear_handler(void **stateptr,
				   __attribute__((unused)) size_t nbytes,
				   __attribute__((unused)) void *data) {

	actor_state_t *my_state = (actor_state_t *) *stateptr;

	if(!my_state->first) {
		send_message(my_state->parent, (message_t) { .message_type = MSG_CLEAR,
													 .nbytes = 0,
													 .data = NULL});
	}

	free(*stateptr);
	send_message(actor_id_self(), (message_t) { .message_type = MSG_GODIE,
												.nbytes = 0,
											 	.data = NULL});
}

/* Main function */

int main(void) {
	unsigned long long int n;
	unsigned long long int res = 1;

	scanf("%llu", &n);

	int err;
	actor_id_t first;

	/* structure passed ONLY to first actor */
	state_t *initial = malloc(sizeof(state_t));

	initial->target = n;
	initial->result_pointer = &res;

	if((err = actor_system_create(&first, &roles)) != 0) {
		perror("Error in creating actor system...\n");
	}

	if(n > 0) {
		send_message(first, (message_t) { .message_type = MSG_INIT,
										  .data = initial });
	}
	else {
		send_message(first, (message_t) { .message_type = MSG_CLEAR,
										  .nbytes = 0,
										  .data = NULL});
	}

	actor_system_join(first);

	printf("%llu\n", res);

	free(initial);

	return 0;
}