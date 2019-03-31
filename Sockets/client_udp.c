#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/un.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <unistd.h>
#include <signal.h>

typedef enum Message_Type{
    JOIN = 0,
    JOIN_ACK = 1,
    FREE = 2,
    FULL = 3,
    RETURN = 4,
} Message_Type;

typedef struct Token {
    char message[64];
    char dest_id[64];
    char source_id[64];
    in_addr_t join_ip_address;
    in_port_t join_port;
    int cnt;
    int ttl;
    Message_Type message_type;
} Token;

Token token;
char* user_id;
in_addr_t next_ip_address;
int out_port;
int in_port;
int has_token;
int udp_in_socket;
int is_pending_message;
char pending_message[64];
char pending_id[64];

void clean_exit(){
    if(close(udp_in_socket) == -1)
        perror("closing in socket failure");

    printf("\nClosed sockets");
    printf("\nExited\n");
}

void error_exit(char* error_message){
    perror(error_message);
    exit(EXIT_FAILURE);
}

void udp_send(){
    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = next_ip_address;
    address.sin_port = htons((uint16_t) out_port);
    if(sendto(udp_in_socket, &token, sizeof(token), 0, (const struct sockaddr*) &address, sizeof(address)) != sizeof(token)){
        error_exit("message send failure");
    }
    fprintf(stderr, "\nSend message to %s %d\n", inet_ntoa(address.sin_addr), ntohs(address.sin_port));
}

void forward_message() {
    if(!has_token) {
        fprintf(stderr, "Do not have token\n");
        return;
    }

    udp_send();
    has_token = 0;
}

void prepare_message(int sig) {
    signal(SIGTSTP, prepare_message);

    if(is_pending_message){
        printf("\nThere is already waiting message to send\n");
        return;
    }
    is_pending_message = 1;

    printf("\nEnter target id:\n");

    char* buffer1;
    size_t bufsize1 = 64;
    size_t characters1;
    buffer1 = (char *) malloc(bufsize1 * sizeof(char));
    characters1 = (size_t) getline(&buffer1, &bufsize1, stdin);
    buffer1[characters1 - 1] = 0;
    printf("\nEnter message:\n");

    char* buffer2;
    size_t bufsize2 = 64;
    buffer2 = (char *) malloc(bufsize2 * sizeof(char));
    getline(&buffer2, &bufsize2, stdin);

    memset(pending_id, 0, 64);
    memset(pending_message, 0, 64);

    strcpy(pending_id, buffer1);
    strcpy(pending_message, buffer2);

    token.ttl = 3;
}

void handle_return() {
    has_token = 1;
    if(strcmp(user_id, token.source_id) == 0) {
        fprintf(stderr, "\nMessage delivered successfully\n");
        memset(token.source_id, 0, 64);
        token.cnt++;
        token.message_type = FREE;
    }
}

void handle_message(){
    fprintf(stderr, "\n\nThis is message for me: %s\n", token.message);
    memset(token.dest_id, 0, 64);
    memset(token.message, 0, 64);
    token.message_type = RETURN;
}

void handle_full(){
    has_token = 1;
    if(strcmp(token.dest_id, user_id) == 0) {
        handle_message();
    }
    if(strcmp(user_id, token.source_id) == 0) {
        token.ttl--;
        if(token.ttl <= 0){
            fprintf(stderr, "\nMessage to %s deleted due to TTL\n", token.dest_id);
            memset(token.dest_id, 0, 64);
            memset(token.message, 0, 64);
            memset(token.source_id, 0, 64);
            token.message_type = FREE;
        }
    }
    token.cnt++;
}

void handle_free() {
    has_token = 1;
    if(is_pending_message){
        is_pending_message = 0;
        memset(token.dest_id, 0, 64);
        memset(token.source_id, 0, 64);
        memset(token.message, 0, 64);
        strcpy(token.dest_id, pending_id);
        strcpy(token.source_id, user_id);
        strcpy(token.message, pending_message);
        memset(pending_id, 0, 64);
        memset(pending_message, 0, 64);
        token.message_type = FULL;
    }
    token.cnt++;
}

void handle_join_ack(struct sockaddr_in addr) {
    printf("Received join_ack from %s %d\n", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

    next_ip_address = token.join_ip_address;
    out_port = token.join_port;
}

void handle_join(struct sockaddr_in addr) {
    printf("Received join request from %s %d\n", inet_ntoa(addr.sin_addr), ntohs(addr.sin_port));

    token.message_type = JOIN_ACK;
    token.join_ip_address = next_ip_address;
    token.join_port = (in_port_t) out_port;

    next_ip_address = addr.sin_addr.s_addr;
    out_port = ntohs(addr.sin_port);
    udp_send();
}

struct sockaddr_in udp_receive(){
    struct sockaddr_in address;
    int len = sizeof(address);
    if(recvfrom(udp_in_socket, &token, sizeof(token), 0, (struct sockaddr*) &address, (socklen_t *) &len) != sizeof(token))
        error_exit("message receive failure");

    if(token.message_type == FREE || token.message_type == FULL)
        fprintf(stderr, "\nReceived token: %d from %s %d\n", token.cnt, inet_ntoa(address.sin_addr), ntohs(address.sin_port));
    return address;
}

void receive_message(){
    struct sockaddr_in address = udp_receive();

    switch (token.message_type) {
        case JOIN:
            handle_join(address);
            receive_message();
            break;
        case JOIN_ACK:
            handle_join_ack(address);
            break;
        case FREE:
            handle_free();
            break;
        case FULL:
            handle_full();
            break;
        case RETURN:
            handle_return();
            break;
    }
}

void join_token_ring(){
    token.message_type = JOIN;

    udp_send();

    receive_message();
//    printf("joined to %s %d\n", next_ip_address, ntohs(address.sin_port));
}

void initialise_in_socket() {
    udp_in_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if(udp_in_socket == -1)
        error_exit("ip socket creation failure");

    struct sockaddr_in in_address;
    bzero(&in_address, sizeof(in_address));
    in_address.sin_family = AF_INET;
    in_address.sin_addr.s_addr = INADDR_ANY;
    in_address.sin_port = htons((uint16_t) in_port);

    if(bind(udp_in_socket, (const struct sockaddr*) &in_address, sizeof(in_address)) == -1)
        error_exit("in socket bind failure");

    fprintf(stderr, "Socket initialised\n");
}

void parse_args(int argc, char* argv[]) {
    if(argc != 6)
        error_exit("invalid arguments");

    user_id = argv[1];

    inet_aton(argv[2], (struct in_addr*) &next_ip_address);
    printf("%d\n", next_ip_address);
    if(next_ip_address == INADDR_NONE)
        error_exit("Invalid ip address");

    char* ptr = argv[3];
    out_port = (int) strtol(ptr, &ptr, 10);
    if(*ptr != '\0')
        error_exit("Invalid port number");

    ptr = argv[4];
    in_port = (int) strtol(ptr, &ptr, 10);
    if(*ptr != '\0')
        error_exit("Invalid port number");
    
    ptr = argv[5];
    has_token = strtol(ptr, &ptr, 10) == 1 ? 1 : 0;
    if(has_token){
        token.cnt = 0;
        token.join_port = 0;
        token.join_ip_address = 0;
    }
}

int main(int argc, char* argv[]){
    atexit(clean_exit);
    parse_args(argc, argv);
    initialise_in_socket();

    join_token_ring();
    signal(SIGTSTP, prepare_message);

    token.message_type = FREE;
    while(1){
        forward_message();
        receive_message();
        sleep(1);
    }
}