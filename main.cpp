#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Ch6: The Cursor Abstraction.
/* 1.
 */

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } ExecuteResult;

const uint16_t COLUMN_USERNAME_SIZE = 32;
const uint32_t COLUMN_EMAIL_SIZE = 255;

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
    StatementType type;
    Row row_to_insert;  // only used by insert statement
} Statement;

Statement new_statement() {
    Statement statement;
    memset((void*)&statement, 0, sizeof(Statement));
    return statement;
}

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096u;
const uint32_t TABLE_MAX_PAGES = 100;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
    int fd;
    uint32_t file_length;
    void* pages[TABLE_MAX_PAGES];  // page cache
} Pager;

typedef struct {
    uint32_t num_rows;
    Pager* pager;
} Table;

typedef struct {
    Table* table;
    uint32_t row_num;
    bool end_of_table;
} Cursor;

Cursor* table_start(Table* table) {
    Cursor* cursor = (Cursor*)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = table->num_rows == 0;
    return cursor;
}

Cursor* table_end(Table* table) {
    Cursor* cursor = (Cursor*)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;
    return cursor;
}

void cursor_advance(Cursor* cursor) {
    if (cursor->row_num < cursor->table->num_rows) {
        cursor->row_num += 1;
        cursor->end_of_table =
            cursor->row_num == cursor->table->num_rows ? true : false;
    }
}

void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

// store in page
void serialize_row(Row* source, void* dest) {
    memcpy((void*)((char*)(dest) + ID_OFFSET), &(source->id), ID_SIZE);
    memcpy((void*)((char*)(dest) + USERNAME_OFFSET), &(source->username),
           USERNAME_SIZE);
    memcpy((void*)((char*)(dest) + EMAIL_OFFSET), &(source->email), EMAIL_SIZE);
}
// fetch from page
void deserialize_row(void* source, Row* dest) {
    memcpy(&(dest->id), (void*)((char*)(source) + ID_OFFSET), ID_SIZE);
    memcpy(&(dest->username), (void*)((char*)(source) + USERNAME_OFFSET),
           USERNAME_SIZE);
    memcpy(&(dest->email), (void*)((char*)(source) + EMAIL_OFFSET), EMAIL_SIZE);
}

void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
               TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if (pager->pages[page_num] == NULL) {
        // cache miss. Allocate memory and load from file.
        uint32_t file_page_num = pager->file_length / PAGE_SIZE;
        void* page = malloc(PAGE_SIZE);
        // might save a partial of page at the end of the file.
        if (pager->file_length % PAGE_SIZE) {
            file_page_num += 1;
        }
        // already exists on disk.
        if (page_num <= file_page_num) {
            lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->fd, page, PAGE_SIZE);
            if (bytes_read < 0) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

// read/write a row by a pointer
void* cursor_value(Cursor* cursor) {
    uint32_t page_num = cursor->row_num / ROWS_PER_PAGE;
    void* page = get_page(cursor->table->pager, page_num);
    uint32_t row_offset = cursor->row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return (void*)((char*)(page) + byte_offset);
}

Pager* page_open(const char* filename) {
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1) {
        printf("Unable to open db file: %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }
    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager* pager = (Pager*)malloc(sizeof(Pager));
    pager->fd = fd;
    pager->file_length = file_length;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pager->pages[i] = NULL;
    }
    return pager;
}

Table* db_open(const char* filename) {
    Pager* pager = page_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;

    Table* table = (Table*)malloc(sizeof(Table));
    table->num_rows = num_rows;
    table->pager = pager;
    return table;
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page.\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
    if (offset < 0) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->fd, pager->pages[page_num], size);
    if (bytes_written < 0) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void db_close(Table* table) {
    Pager* pager = table->pager;
    //-------persistence
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;
    for (uint32_t i = 0; i < num_full_pages; ++i) {
        if (pager->pages[i] != NULL) {
            pager_flush(pager, i, PAGE_SIZE);
            free(pager->pages[i]);
            pager->pages[i] = NULL;
        }
    }
    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0) {
        if (pager->pages[num_full_pages] != NULL) {
            pager_flush(pager, num_full_pages, num_additional_rows * ROW_SIZE);
            free(pager->pages[num_full_pages]);
            pager->pages[num_full_pages] = NULL;
        }
    }
    //---------
    int result = close(pager->fd);
    if (result < 0) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    // make sure free every page
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

InputBuffer* new_input_buffer() {
    // dynamic allocation
    InputBuffer* buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
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

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
    if (0 == strcmp(input_buffer->buffer, ".exit")) {
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;
    // WARNING: sscanf may cause buffer overflow!
    /*int args_assigned = sscanf(input_buffer->buffer, "insert %d %s %s",
                           &(statement->row_to_insert.id),
                           statement->row_to_insert.username,
                           statement->row_to_insert.email);*/
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");
    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE ||
        strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement);
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Row* row_to_insert = &(statement->row_to_insert);
    Cursor* cursor = table_end(table);
    serialize_row(row_to_insert, cursor_value(cursor));
    table->num_rows += 1;
    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Row row;
    Cursor* cursor = table_start(table);
    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}

int main(int argc, char* argv[]) {
    // define a input buffer and table (hard code)
    InputBuffer* input_buffer = new_input_buffer();
    if (argc < 2) {
        printf("Must supply a db filename.\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table* table = db_open(filename);
    // main loop
    while (1) {
        print_prompt();
        read_input(input_buffer);
        // parse meta command
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf("Unrecognized command %s.\n", input_buffer->buffer);
                    continue;
            }
        }
        // parse statement
        Statement statement = new_statement();
        switch (prepare_statement(input_buffer, &statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case PREPARE_STRING_TOO_LONG:
                printf("String is too long.\n");
                continue;
            case PREPARE_NEGATIVE_ID:
                printf("ID must be positive.\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s'.\n",
                       input_buffer->buffer);
                continue;
        }
        // execute statement
        switch (execute_statement(&statement, table)) {
            case EXIT_SUCCESS:
                printf("Executed.\n");
                break;
            case EXECUTE_TABLE_FULL:
                printf("Error: Table full.\n");
                break;
        }
    }
}