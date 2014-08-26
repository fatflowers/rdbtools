/*
 * rdb parser for redis.
 * author: @hulk 
 * date: 2014-08-13
 */
#include "main.h"
#include "aof.h"
#include "rdb_parser.h"
#include "rediscounter.h"

void _parsePanic(char *msg, char *file, int line) {
    fprintf(stderr,"!!! Software Failure. Press left mouse button to continue");
    fprintf(stderr,"Guru Meditation: %s #%s:%d",msg,file,line);
#ifdef HAVE_BACKTRACE
    fprintf(stderr,"(forcing SIGSEGV in order to print the stack trace)");
    *((char*)-1) = 'x';
#endif
}

/**
 * @brief _format_kv
 * User handle in redis-counter.
 * Format a string with key and value, the string is to be dumped in aof file.
 * Define a way to get a aof file number from hashing key.
 * @param key
 * @param key_len
 * @param value
 * @param hashed_key
 * Save hashed aof file number of type int.
 * @return Formatted string of k&v
 */
char * _format_kv(void *key, int key_len, long value, void *hashed_key, int aof_number){
    if(key_len <= 0)
        return NULL;
    *(int *)hashed_key = (((char *)key)[0] - '0') % aof_number;
    char tmp[1024];
    sprintf(tmp, "%s:%ld\n", (char *)key, value);
    return (char *)strdup(tmp);
}

// rdb-parser user handler.
void* userHandler (int type, void *key, void *val, unsigned int vlen, time_t expiretime) {
#if 0
    unsigned int i;
    if(type == REDIS_STRING) {
        printf("STRING\t%d\t%s\t%s\n", (int)expiretime, (char *)key, (char *)val);
    } else if (type == REDIS_SET) {
        sds *res = (sds *)val;
        printf("SET\t%d\t%s\t[", (int)expiretime, (char *)key);
        for(i = 0; i < vlen; i++) {
            printf("%s, ", res[i]);
        }
        printf("]\n");
    } else if(type == REDIS_LIST) {
        sds *res = (sds *)val;
        printf("LIST\t%d\t%s\t[", (int)expiretime, (char *)key);
        for(i = 0; i < vlen; i++) {
            printf("%s, ", res[i]);
        }
        printf("]\n");
    } else if(type == REDIS_ZSET) { 
        sds *res = (sds *)val;
        printf("ZSET\t%d\t%s\t", (int)expiretime, (char *)key);
        for(i = 0; i < vlen; i += 2) {
            printf("(%s,", res[i]);
            printf("%s), ", res[i+1]);
        }
        printf("\n");

    } else if(type == REDIS_HASH) {
        sds *res = (sds *)val;
        printf("HASH\t%d\t%s\t", (int)expiretime, (char *)key);
        for(i = 0; i < vlen; i += 2) {
            printf("(%s, ", res[i]);
            printf("%s), ", res[i+1]);
        }
        printf("\n");
    }
    return NULL;
#endif
}


int main(int argc, char **argv) {
    char *usage = "Usage:\nrdb_tools -[t service name] -[f rdb file path]"
            "\nService name: rdbparser or rediscounter"
            "\nrdbparser, with following options:"
            "\n [d]\n"
            "\t-d --dump \tparser info, to dump parser stats info.\n\t\t\tDefault: no\n"
            "\n"
            "\nrediscounter, with following options:"
            "\n [-n number] [-o aof filename] [s]\n"
            "\t-n --number \tspecify number of aof files.\n\t\t\tDefault: 1\n"
            "\t-o --name \tspecify name of aof files. \n\t\t\tDefault: output.aof\n"
            "\t-s --save \tSave mode, save aof file. \n\t\t\tDefault: no\n"
            "Notice: This tool only test on redis 2.2 and 2.4, so it may be error in 2.4 later.\n";
    if(argc <= 4) {
        fprintf(stderr, "%s", usage);
        exit(1);
    }    
    char *rdbFile = NULL;
    // option variables for rdb-parser
    BOOL dumpParseInfo = FALSE;
    int parse_result;
    // service to use
    int service = -1;
    // option variables for redis-counter
    int aof_number = 1;
    char *aof_filename = "output.aof";
    int dump_aof = -1;

    /***
     * Arguments
     * -f rdb file path
     * -d rdbparser, dump parser info.
     * -t service name like "rdbparser", "rediscounter"
     * -n rediscounter, aof file number for save kv
     * -o rediscounter, aof output file name
     * -s rediscounter, is dump aof file.
     ***/
    char * optstring = "f:dt:n:o:s";
    int ch;
    while((ch = getopt(argc, argv, optstring)) != -1){
        switch(ch){
        case 'f':
            rdbFile = optarg;
            break;
        case 'd':
            dumpParseInfo = TRUE;
            break;
        case 't':
            if(strcmp("rdbparser", optarg) == 0){
                service = RDB_PARSER;
            }
            else if(strcmp("rediscounter", optarg) == 0){
                service = REDIS_COUNTER;
            }
            else{
                fprintf(stderr, "Wrong service type: %s\n", optarg);
                exit(1);
            }
            break;
        case 'n':
            aof_number = atoi(optarg);
            break;
        case 'o':
            aof_filename = optarg;
            break;
        case 's':
            dump_aof = 1;
            printf("dump_aoffffff %d\n", dump_aof);
            break;
        default:
            fprintf(stderr, "Unknown option -%c\n", (char)ch);
            exit(1);
        }
    }
    if(!rdbFile){
        fprintf(stderr, "U need to specify a rdb file path first with -f option.\n");
        exit(1);
    }
    if(service == -1){
        fprintf(stderr, "U need to specify a service name first with -t option.\n");
        exit(1);
    }
    if(service == RDB_PARSER){
        printf("--------------------------------------------RDB PARSER------------------------------------------\n");
        parse_result = rdbParse(rdbFile, userHandler);
        printf("--------------------------------------------RDB PARSER------------------------------------------\n");
        if(parse_result == PARSE_OK && dumpParseInfo) {
            dumpParserInfo();
        }
    }
    if(service == REDIS_COUNTER){
        printf("--------------------------------------------REDIS COUNTER------------------------------------------\n");
        rdb_load(rdbFile, _format_kv, aof_number, aof_filename, dump_aof);
        printf("--------------------------------------------REDIS COUNTER------------------------------------------\n");
    }
    return 0;
}
