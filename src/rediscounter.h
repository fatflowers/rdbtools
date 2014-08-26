/***
 * redis counter.
 * Parse redis rdb file, count deleted keys, other keys and saved keys.
 * Save key value pair into aof files, format style can be defined with format_kv_handler defined in rediscounter.h and called by rdb_load function.
 *
 * author: sunlei
 * date: 2014.08.22
***/

#ifndef REDISCOUNTER_H
#define REDISCOUNTER_H
#include "main.h"



/**
 * @brief _format_kv
 * Format a string with key and value, the string is to be dumped in aof file.
 * Define a way to get a aof file number from hashing key.
 * @param key
 * @param key_len
 * @param value
 * @param hashed_key
 * Save hashed aof file number of type int.
 * @return Formatted string of k&v
 */
typedef char * format_kv_handler(void * key, int key_len, long value, void *hashed_key);

/***
 * rdb_state
 * Store info of rdb file in rdb header section.
 */
typedef struct rdb_state{
    long long offset;
    long long size; // dbsize
    long long used;
    long long deleted;
    long long key_size;
    long long entry_size;
    long long value_size;
    sds rdb_filename;
}rdb_state;

/**
 * Global values defined in rediscounter.c.
*/
extern int aof_number;
extern char * aof_filename;
extern long long REDISCOUNTER_RDB_BLOCK;
extern int dump_aof;// -1 for don't save aof, 1 for save aof.

int rdb_load(char *filename, format_kv_handler handler);
// print state every PRINT_BLOCK keys
#define PRINT_BLOCK 50000000
#define RDB_INVALID_LEN 252
#define AOF_BUFFER_SIZE 10240

// return state
#define COUNTER_ERR -1
#define COUNTER_OK 1

/* Defines related to the dump file format. To store 32 bits lengths for short
 * keys requires a lot of space, so we check the most significant 2 bits of
 * the first byte to interpreter the length:
 *
 * 00|000000 => if the two MSB are 00 the len is the 6 bits of this byte
 * 01|000000 00000000 =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
 * 10|000000 [32 bit integer] => if it's 01, a full 32 bit len will follow
 * 11|000000 this means: specially encoded object will follow. The six bits
 *           number specify the kind of object that follows.
 *           See the REDIS_RDB_ENC_* defines.
 *
 * Lenghts up to 63 are stored using a single byte, most DB keys, and may
 * values, will fit inside. */
#define REDIS_RDB_6BITLEN 0
#define REDIS_RDB_14BITLEN 1
#define REDIS_RDB_32BITLEN 2
#define REDIS_RDB_ENCVAL 3
#define REDIS_RDB_LENERR UINT_MAX


/* When a length of a string object stored on disk has the first two bits
 * set, the remaining two bits specify a special encoding for the object
 * accordingly to the following defines: */
// encoding state
#define REDIS_RDB_ENC_INT8 0        /* 8 bit signed integer */
#define REDIS_RDB_ENC_INT16 1       /* 16 bit signed integer */
#define REDIS_RDB_ENC_INT32 2       /* 32 bit signed integer */
#define REDIS_RDB_ENC_LZF 3         /* string compressed with FASTLZ */

#endif // REDISCOUNTER_H
