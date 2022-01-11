#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Ch1: making a simple REPL(Read-Execute-Print Loop, similar to a shell).
/* FAQ: 1. 'malloc' underlying principle;
        2. 'getline';
        3. the effect of 'fflush';
        4. exit & return;
*/

typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

InputBuffer* new_input_buffer() {
    // dynamic allocation
    InputBuffer* buffer = malloc(sizeof(InputBuffer));
    memset(buffer, 0, sizeof(buffer));
    return buffer;
}

void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

void print_prompt() {
    printf("db> ");
    fflush(stdout);
}

void read_input(InputBuffer* input_buffer) {
    /*
    ssize_t getline(char **lineptr, size_t *n, FILE *stream);
    args:
        @lineptr : a pointer to the variable we use to point to the buffer
    containing the read line. If it set to NULL it is mallocatted by getline and
    should thus be freed by the user, even if the command fails.
        @n : a pointer to the variable we use to save the size of allocated
    buffer.
        @stream : the input stream to read from. We’ll be reading from standard
    input.
        @return value : the number of bytes read, which may be less than the
    size of the buffer.
    */
    ssize_t bytes_read =
        getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // Ignore trailing newline
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = '\0';
}

int main(int argc, char* argv[]) {
    // define a input buffer
    InputBuffer* input_buffer = new_input_buffer();
    // main loop
    while (1) {
        print_prompt();
        read_input(input_buffer);

        if (0 == strcmp(input_buffer->buffer, "exit")) {
            close_input_buffer(input_buffer);
            exit(EXIT_SUCCESS);
        } else {
            printf("Unrecognized command '%s'.\n", input_buffer->buffer);
        }
    }
}