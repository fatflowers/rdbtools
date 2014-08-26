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

#include "aof.h"

int init_aof(Aof * aof_obj, int index, char *filename){
   aof_obj->index = index;
   aof_obj->filename = (char *)strdup(filename);
   if(!filename)
       goto err;
   if((aof_obj->fp = fopen(aof_obj->filename, "ab+")) == NULL)
       goto err;
   if((aof_obj->buffer = sdsnewlen(NULL, AOF_BUFFER_SIZE)) == NULL)
       goto err;
   return AOF_OK;
err:
   free(aof_obj->filename);
   fclose(aof_obj->fp);
   sdsfree(aof_obj->buffer);
   fprintf(stderr, "init_aof filename: %s error: %s\n",
           filename, strerror(errno));
   return AOF_ERR;
}

int save_aof(Aof * aof_obj){
   if(strlen(aof_obj->buffer) <= 0)
       return AOF_OK;
   if(fwrite(aof_obj->buffer, strlen(aof_obj->buffer), 1, aof_obj->fp) != 1)
       goto err;
   // clear the buffer
   aof_obj->buffer[0] = '\0';
   return AOF_OK;
err:
   fprintf(stderr, "save_aof error: %s\n",strerror(errno));
   return AOF_ERR;
}

int add_aof(Aof * aof_obj, char * item){
   if(aof_obj->buffer == NULL || item == NULL){
       fprintf(stderr, "add_aof error\n");
       return AOF_ERR;
   }
   // dump buffer to file if it's too large
   if(strlen(aof_obj->buffer) + strlen(item) >= AOF_BUFFER_SIZE)
       save_aof(aof_obj);

   strcat(aof_obj->buffer, item);
   return AOF_OK;
}

Aof *set_aofs(int aof_number, char *aof_filename){
   Aof * aof_set = (Aof *)malloc(sizeof(Aof) * aof_number);
   if(aof_number <= 0 || !aof_filename || !aof_set){
       fprintf(stderr, "wrong aof_number or aof_filename\n");
       return NULL;
   }

   char buf[1024];
   int i;
   for(i = 0; i < aof_number; i++){
       sprintf(buf, "%s.%09d", aof_filename, i);
       if(init_aof(aof_set + i, i, buf) == AOF_ERR){
           return NULL;
       }
   }
   return aof_set;
}
