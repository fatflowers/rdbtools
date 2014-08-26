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
**/

#ifndef AOF_H
#define AOF_H

#include "main.h"

// return state
#define AOF_ERR -1
#define AOF_OK 1
#define AOF_BUFFER_SIZE 10240

extern int aof_number;
extern char * aof_filename;
extern int dump_aof;


void set_aof_global();

typedef struct Aof{
   int index;
   char *filename;
   FILE *fp;
   sds buffer;
}Aof;

int init_aof(Aof * aof_obj, int index, char *filename);

int save_aof(Aof * aof_obj);

int add_aof(Aof * aof_obj, char * item);

// init all the Aof objects with aof_number and aof_filename
Aof *set_aofs();

#endif
