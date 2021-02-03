#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include "cacti.h"

#define MSG_COUNT 1

void hello_handler(void **, size_t, void *);
void count_handler(void **, size_t, void *);

static size_t w;
static size_t k;

static int32_t *matrix;
static int32_t *times;
static int32_t *sums;

static actor_id_t *ids;

static size_t actors_count = 0;

act_t prompts_array[] = { hello_handler, count_handler };
role_t roles = (role_t) { .nprompts = 2, .prompts = prompts_array };

size_t get_position(actor_id_t actor) {
	size_t pos;

	for(size_t i = 0; i < k; ++i)
		if(ids[i] == actor)
			pos = i;

	return pos;
}

typedef struct state {
	size_t row;
	int32_t sum;
} state_t;


static state_t *computing_state;

void hello_handler(__attribute__((unused)) void **stateptr,
				   __attribute__((unused)) size_t nbytes,
				   __attribute__((unused)) void *data) {

	ids[actors_count] = actor_id_self();

	actors_count++;

	if(actors_count < k) {
		send_message(actor_id_self(), (message_t) { .message_type = MSG_SPAWN, 
													.nbytes = sizeof(actor_id_t),
													.data = &roles });
	}
	else {
		computing_state = malloc(sizeof(state_t));
		computing_state->row = 0;
		computing_state->sum = sums[0];

		send_message(ids[0], (message_t) { .message_type = MSG_COUNT,
										   .nbytes = sizeof(computing_state),
										   .data = (void *) computing_state});
	}
}

void count_handler(__attribute__((unused)) void **stateptr, 
				   __attribute__((unused))size_t nbytes, 
				   void *data) {

	state_t *current_state = (state_t *) data;

	size_t row_number = current_state->row;
	size_t column_number = get_position(actor_id_self());

	int32_t value = matrix[row_number * k + column_number];

	usleep(times[row_number * k + column_number]*1000);

	sums[row_number] = (computing_state->sum + value);
	current_state->sum = sums[row_number];

	if(column_number < k - 1) {
		send_message(ids[column_number + 1], (message_t) { .message_type = MSG_COUNT,
														   .nbytes = sizeof(computing_state),
														   .data = (void *) current_state});
	}
	else {

		if(row_number < w - 1) {
			current_state->row = row_number + 1;
			current_state->sum = 0;

			send_message(ids[0], (message_t) { .message_type = MSG_COUNT,
											   .nbytes = sizeof(computing_state),
											   .data = (void *) current_state});
		}
		else {
			for(size_t i = 0; i < k; ++i) {
				send_message(ids[i], (message_t) { .message_type = MSG_GODIE,
												   .nbytes = 0,
												   .data = NULL});
			}
		}
	}
}

int main(void) {
	int err;

	scanf("%lu%lu", &w, &k);

	matrix = malloc(w * k * sizeof(int32_t));
	times = malloc(w * k * sizeof(int32_t));
	sums = malloc(w * sizeof(int32_t));
	ids = malloc(k * sizeof(actor_id_t));

	for(size_t i = 0; i < w; ++i) {
		for(size_t j = 0; j < k; ++j) {
			scanf("%d%d", &matrix[i * k + j], &times[i * k + j]);
		}

		sums[i] = 0;
	}

	actor_id_t first_actor;

	if((err = actor_system_create(&first_actor, &roles)) != 0) {
		perror("Error in creating actor system...\n");
		return 1;
	}

	actor_system_join(first_actor);

	for(size_t i = 0; i < w; ++i) {
		printf("%d\n", sums[i]);
	}

	free(matrix);
	free(times);
	free(sums);
	free(ids);
	free(computing_state);

	return 0;
}
