/****************************
* Aof module
*
* init_aof
* Init a aof struct.
*
* save_aof
* Dump data in buffer to aof file.
*
* add_aof
* Add data to buffer. If buffer is full, call save_aof.
*
* set_aofs
* Init all the Aof objects with global variables aof_number and aof_filename.
*
* author: sunlei
* 2014.08.26
****************************/

#ifndef AOF_H
#define AOF_H

#include "main.h"
#include "fmacros.h"

// return state
#define AOF_ERR -1
#define AOF_OK 1
#define AOF_BUFFER_SIZE 10240

// struct for each aof file.
typedef struct Aof{
   int index; // aof file index.
   char *filename; // aof file name.
   FILE *fp; // aof file pointer.
   sds buffer; // buffer dumped every AOF_BUFFER_SIZE bytes.
}Aof;

/**
 * @brief init_aof
 * Init a Aof struct.
 * @param aof_obj
 * Target Aof struct.
 * @param index
 * aof file index.
 * @param filename
 * aof file name.
 * @return
 */
int init_aof(Aof * aof_obj, int index, char *filename);

int save_aof(Aof * aof_obj);

int add_aof(Aof * aof_obj, char * item);

Aof *set_aofs(int aof_number, char *aof_filename);

#endif
