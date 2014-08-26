/*
 * rdb parser for redis.
 * author: @hulk 
 * date: 2014-08-13
 */
#include "main.h"
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
char * _format_kv(void *key, int key_len, long value, void *hashed_key){
    if(key_len <= 0)
        return NULL;
    *(int *)hashed_key = (((char *)key)[0] - '0') % aof_number;
    char tmp[1024];
    sprintf(tmp, "%s:%ld\n", (char *)key, value);
    return (char *)strdup(tmp);
}


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
    char *usage = "Usage:\nrdb_tools -[p/c]\n"
            "\n-p rdb-parser, with following options:"
            "\n-f file [d]\n"
            "\t-f --file \tspecify which rdb file would be parsed.\n"
            "\t-d --dump \tparser info, to dump parser stats info.\n"
            "\n"
            "-c redis-counter, with following options:"
            "\n-f file [-n number] [-a aof filename] [s]\n"
            "\t-f --file \tspecify full path of rdb file would be parsed.\n"
            "\t-n --number \tspecify number of aof files.\n\t\t\tDefault: 1\n"
            "\t-a --name \tspecify name of aof files. \n\t\t\tDefault: output.aof\n"
            "\t-s --save \tSave mode, save aof file. \n\t\t\tDefault: no\n"
            "Notice: This tool only test on redis 2.2 and 2.4, so it may be error in 2.4 later.\n";
    if(argc <= 4) {
        fprintf(stderr, "%s", usage);
        exit(1);
    }    
    char *rdbFile;
    int i;
    //rdb-parser
    if(argv[1][0] == '-' && argv[1][1] == 'p'){
        int parse_result;
        BOOL dumpParseInfo = FALSE;
        for(i = 2; i < argc; i++) {
            if(argc > i+1 && argv[i][0] == '-' && argv[i][1] == 'f') {
                i += 1;
                rdbFile = argv[i];
            } else if(argv[i][0] == '-' && argv[i][1] == 'd'){
                dumpParseInfo = TRUE;
            }
        }
        if(!rdbFile) {
            fprintf(stderr, "ERR: U must specify rdb file by option -f filepath.\n");
            exit(1);
        }

        /* start parse rdb file. */
        printf("--------------------------------------------RDB PARSER------------------------------------------\n");
        parse_result = rdbParse(rdbFile, userHandler);
        printf("--------------------------------------------RDB PARSER------------------------------------------\n");
        if(parse_result == PARSE_OK && dumpParseInfo) {
            dumpParserInfo();
        }
    }
    //redis-counter
    else if(argv[1][0] == '-' && argv[1][1] == 'c'){
        for(i = 2; i < argc; i++) {
                if(argc > i+1 && argv[i][0] == '-' && argv[i][1] == 'f') {
                    i += 1;
                    rdbFile = argv[i];
                }else if(argv[i][0] == '-' && argv[i][1] == 'n'){
                    i += 1;
                    aof_number = atoi(argv[i]);
                }
                else if(argv[i][0] == '-' && argv[i][1] == 'a'){
                    i += 1;
                    aof_filename = argv[i];
                }
                else if(argv[i][0] == '-' && argv[i][1] == 's'){
                    dump_aof = 1;
                }
            }
            rdb_load(rdbFile, _format_kv);
    }else{
        fprintf(stderr, "ERR: U must specify a specific tool to use.\n");
        exit(1);
    }

    return 0;
}
