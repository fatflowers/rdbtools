/***
 * redis counter.
 * Parse redis rdb file, count deleted keys, other keys and saved keys.
 * Save key value pair into aof files, format style can be defined with format_kv_handler defined in rediscounter.h and called by rdb_load function.
 *
 * author: sunlei
 * date: 2014.08.22
***/

#include "rediscounter.h"
/***
 * Global variables
 *
 * @brief aof_number
 *  number of aof files to be created.
 *
 * @brief aof_filename,
 * prefix of aof filename.
 *
 * @brief REDISCOUNTER_RDB_BLOCK
 * default read block size.
 *
 * @brief dump_aof
 * -1 for don't save aof, 1 for save aof.
 *
 * @brief _time_begin
 * @brief _time_counter
 * time recoders.
 *
 * @brief print_count
 * print state every PRINT_BLOCK keys
 */
int aof_number = 1;
char * aof_filename = "output.aof";
long long REDISCOUNTER_RDB_BLOCK = 10240;
int dump_aof = -1;
long _time_begin, _time_counter;
unsigned int print_count = 0;

/**
 * @brief show_state
 * Show time used and msg.
 * @param msg
 */
void show_state(char * msg){
    long now = clock();
    fprintf(stdout, "now=%ld, time_total=%lfs, msg=%s",
            now,
            (double)(now - _time_begin) / CLOCKS_PER_SEC,
            msg);
}


 /****************************
 * Aof module
 *
 * struct Aof
 * Maintain the data of each aof files.
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
**/
typedef struct Aof{
    int index;
    char *filename;
    FILE *fp;
    sds buffer;
}Aof;

int init_aof(Aof * aof_obj, int index, char *filename){
    aof_obj->index = index;
    aof_obj->filename = (char *)strdup(filename);
    if(!filename)
        goto err;
    if((aof_obj->fp = fopen(aof_obj->filename, "ab+")) == NULL)
        goto err;
    if((aof_obj->buffer = sdsnewlen(NULL, REDISCOUNTER_RDB_BLOCK)) == NULL)
        goto err;
    return COUNTER_OK;
err:
    free(aof_obj->filename);
    fclose(aof_obj->fp);
    sdsfree(aof_obj->buffer);
    fprintf(stderr, "init_aof filename: %s error: %s\n",
            filename, strerror(errno));
    return COUNTER_ERR;
}

int save_aof(Aof * aof_obj){
    if(strlen(aof_obj->buffer) <= 0)
        return COUNTER_OK;
    char buf[512];
    // show save state
    if(print_count > PRINT_BLOCK){
        print_count = 0;
        sprintf(buf, "save buffer, filename=%s, len=%ld\n", aof_obj->filename, (long unsigned int)strlen(aof_obj->buffer));
        show_state(buf);
    }
    if(fwrite(aof_obj->buffer, strlen(aof_obj->buffer), 1, aof_obj->fp) != 1)
        goto err;
    // clear the buffer
    aof_obj->buffer[0] = '\0';
    return COUNTER_OK;
err:
    fprintf(stderr, "save_aof error: %s\n",strerror(errno));
    return COUNTER_ERR;
}

int add_aof(Aof * aof_obj, char * item){
    if(aof_obj->buffer == NULL || item == NULL){
        fprintf(stderr, "add_aof error\n");
        return COUNTER_ERR;
    }
    // dump buffer to file if it's too large
    if(strlen(aof_obj->buffer) + strlen(item) >= AOF_BUFFER_SIZE)
        save_aof(aof_obj);

    strcat(aof_obj->buffer, item);
    return COUNTER_OK;
}

// init all the Aof objects with aof_number and aof_filename
Aof *set_aofs(){
    Aof * aof_set = (Aof *)malloc(sizeof(Aof) * aof_number);
    if(aof_number <= 0 || !aof_filename || !aof_set){
        fprintf(stderr, "wrong aof_number or aof_filename\n");
        return NULL;
    }

    char buf[1024];
    int i;
    for(i = 0; i < aof_number; i++){
        sprintf(buf, "%s.%09d", aof_filename, i);
        if(init_aof(aof_set + i, i, buf) == COUNTER_ERR){
            return NULL;
        }
    }
    return aof_set;
}

unsigned char read_byte(FILE * fp){
    unsigned char number;
    if (fread(&number, 1, 1, fp) == 0) return -1;
    return number;
}

/* Load an encoded length from the DB, see the REDIS_RDB_* defines on the top
 * of this file for a description of how this are stored on disk.
 *
 * isencoded is set to 1 if the readed length is not actually a length but
 * an "encoding type", check the above comments for more info */
uint32_t rdb_load_len(FILE *fp, int *isencoded) {
	unsigned char buf[2];
	uint32_t len;
	int type;

     if (isencoded) *isencoded = 0;
	if (fread(buf,1,1,fp) == 0) return REDIS_RDB_LENERR;
	type = (buf[0]&0xC0)>>6;
	if (type == REDIS_RDB_6BITLEN) {
		/* Read a 6 bit len */
		return buf[0]&0x3F;
    }
	else if (type == REDIS_RDB_14BITLEN) {
		/* Read a 14 bit len */
		if (fread(buf+1,1,1,fp) == 0) return REDIS_RDB_LENERR;
		return ((buf[0]&0x3F)<<8)|buf[1];
	} else {
		/* Read a 32 bit len */
		if (fread(&len,4,1,fp) == 0) return REDIS_RDB_LENERR;
		return ntohl(len);
	}
}
sds rdb_generic_load_string_object(FILE*fp, int encode) {
    int isencoded;
    uint32_t len;
    sds val;

    if (encode)
        encode = 0;// avoid warning
    len = rdb_load_len(fp,&isencoded);
    if (len == REDIS_RDB_LENERR) return NULL;
    val = sdsnewlen(NULL,len);
    if (len && fread(val,len,1,fp) == 0) {
        sdsfree(val);
        return NULL;
    }
    return val;
}

sds rdb_load_string_object(FILE *fp) {
    return rdb_generic_load_string_object(fp,0);
}

sds rdb_load_encoded_string_object(FILE *fp) {
    return rdb_generic_load_string_object(fp,1);
}

/* For information about double serialization check rdbSaveDoubleValue() */
int rdb_load_double_value(FILE *fp, double *val) {
    char buf[128];
    unsigned char len;

    if (fread(&len,1,1,fp) == 0) return -1;
    if(len > 252){
        fprintf(stderr, "wrong number in rdbLoadDoubleValue\n");
        return 0;
    }
    if (fread(buf,len,1,fp) == 0) return -1;
    buf[len] = '\0';
    sscanf(buf, "%lg", val);
    return 0;

}

/* check if value is prime, return 1 if it is, 0 otherwise */
static int _is_prime(unsigned long value)
{
    unsigned long i;
    if (value <= 1)
        return 0;
    if (value == 2)
        return 1;
    if (value % 2 == 0)
        return 0;

    for (i = 3; i < sqrt(value) + 1; ++i)
        if (value % i == 0)
            return 0;

    return 1;
}
/**
 * @brief init_rdb_state
 * Read head of rdb file, fill info data to state
 * @param state, rdb_state to be filled.
 * @param fp, file pointer for current rdb file.
 * @return success:COUNTER_OK, failure:COUNTER_ERR.
 */
int init_rdb_state(rdb_state * state, FILE * fp){
    char buf[1024];
    int rdbver;
    sds sdstemp;
    double val;

    if (fread(buf,9,1,fp) == 0) goto eoferr;
    buf[9] = '\0';
    /*-------redis signature-------*/
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(fp);
        fprintf(stderr,"Wrong signature trying to load DB from file\n");
        goto eoferr;
    }
    /*-------redis version check-------*/
    rdbver = atoi(buf+5);
    if (rdbver >= 2  && rdbver <= 5) {
        /*-------rdb file name & offset-------*/
        if (!(sdstemp = rdb_load_string_object(fp)) || rdb_load_double_value(fp, &val)) {
            fprintf(stderr, "Failed to get aof name or offset\n");
            goto eoferr;
        }
        state->offset = val;
        /*
        fprintf(stdout, "RDB position: %s-%lld\n",
                    state->rdb_filename, state->offset);
                    */
    } else if (rdbver != 1) {
        fprintf(stderr, "Can't handle RDB format version %d\n",rdbver);
        goto eoferr;
    }
    /*------- RDB created by 1.1.0(with LRU support) added a type field -------*/
    if (rdbver == 4) {
        if (rdb_load_double_value(fp, &val) == -1) {
            fprintf(stderr, "Error getting type info from RDB");
            goto eoferr;
        }
    }
    fprintf(stdout, "Loading dict information...\n");
    /*------- load db size, used, deleted_slots, key_size, entry_size -------*/
    if (rdb_load_double_value(fp, &val) == -1) goto eoferr;
    state->size = (unsigned long)val;
    if (rdb_load_double_value(fp, &val) == -1) goto eoferr;
    state->used = (unsigned long)val;
    if (rdb_load_double_value(fp, &val) == -1) goto eoferr;
    state->deleted = (unsigned long)val;
    if (rdb_load_double_value(fp, &val) == -1) goto eoferr;
    state->key_size = (unsigned int)val;
    if (rdb_load_double_value(fp, &val) == -1) goto eoferr;
    state->entry_size = (unsigned int)val;
    fprintf(stdout,  "RDB info: size-%lld, used-%lld, deleted-%lld, key-size-%lld, "
                "entry-size-%lld\n", state->size, state->used, state->deleted, state->key_size, state->entry_size);
    state->value_size = state->entry_size - state->key_size;

    /*------- check if values are valid -------*/
    if (!_is_prime(state->size) ||
        state->used > state->size ||
        state->deleted > state->size ||
        state->key_size < 1)
    {
        fprintf(stderr, "Invalid values got from RDB: size-%lld, "
                "used-%lld, deleted-%lld, key_size-%lld\n",
                state->size, state->used, state->deleted, state->key_size);
        fprintf(stderr, "Error loading from DB. Aborting now.\n");
        exit(1);
        goto eoferr;;
    }

    sdsfree(sdstemp);
    return COUNTER_OK;
eoferr:
    if(sdslen(sdstemp) > 0)
        sdsfree(sdstemp);
    fprintf(stderr, "init_rdb_state failed\n");
    return COUNTER_ERR;
}

/*-------block : read buffer size-------*/
void adjust_block_size(long long entry_size){
    fprintf(stdout, "REDISCOUNTER_RDB_BLOCK=%lld\n", REDISCOUNTER_RDB_BLOCK);
    REDISCOUNTER_RDB_BLOCK = entry_size * ((REDISCOUNTER_RDB_BLOCK + entry_size - 1) / entry_size);
    fprintf(stdout, "REDISCOUNTER_RDB_BLOCK=%lld\n", REDISCOUNTER_RDB_BLOCK);
}

/**
 * @brief rdb_load_dict
 * Parse the data section of rdb file.
 * @param fp
 * @param state
 * State of rdb file.
 * @param aof_set
 * Aof file state set.
 * @return
 */
int rdb_load_dict(FILE *fp, rdb_state state, Aof * aof_set, format_kv_handler format_handler) {
    adjust_block_size(state.entry_size);
    long long total = state.size * state.entry_size,
            count = total / REDISCOUNTER_RDB_BLOCK,
            rest = total % REDISCOUNTER_RDB_BLOCK,
            key_count,
            read_offset = 0,
            ndeleted_key = 0,
            nother_key = 0,
            boundary = REDISCOUNTER_RDB_BLOCK,
            saved_key = 0;
    sds empty_key = sdsnewlen(NULL, state.key_size),
            deleted_key = sdsnewlen(NULL, state.key_size),
            buf = sdsnewlen(NULL, REDISCOUNTER_RDB_BLOCK);
    int i, j, value;
    char *key = (char *)malloc(sizeof(char) * state.key_size),
            value_buf[4],
            *tmp,
            state_buf[1024];
    for(i = 0; i < state.key_size; i++){
        empty_key[i] = '\0';
        deleted_key[i] = 'F';
    }
    fprintf(stdout, "total=%lld, count=%lld, rest=%lld", total, count, rest);
    for(i = 0; i <= count; i++){
        if(i < count){
            key_count = REDISCOUNTER_RDB_BLOCK / state.entry_size;
            if (fread(buf,REDISCOUNTER_RDB_BLOCK,1,fp) == 0) {
                sdsfree(buf);
                fprintf(stderr, "rdbLoadDict error: %s\n",strerror(errno));
                return COUNTER_ERR;
            }
        }
        else{
            boundary = rest;
            key_count = rest / state.entry_size;
            if (fread(buf,rest,1,fp) == 0) {
                sdsfree(buf);
                fprintf(stderr, "rdbLoadDict error: %s\n",strerror(errno));
                return COUNTER_ERR;
            }
        }
        // show state
        if(print_count > PRINT_BLOCK){
            print_count = 0;
            sprintf(state_buf, "get dict, block_id=%d block_size=%lfM\n"
                    "key_count=%lld\n", i, REDISCOUNTER_RDB_BLOCK / 1024.0 / 1024.0, key_count);
            show_state(state_buf);
        }

        // time used...
        read_offset = 0;
        for(j = 0; j < key_count && read_offset < boundary; j++){
            // get a value
            if(strncpy(value_buf, buf + read_offset, 4) == NULL){
                fprintf(stderr, "rdbLoadDict reading value_buf error: %s\n",strerror(errno));
                goto err;
            }
            read_offset += 4;

            // convert octonary number to long long
            value = *(long long *)value_buf;
            // get a key
            if(strncpy(key, buf + read_offset, state.key_size) == NULL){
                fprintf(stderr, "rdbLoadDict reading buf error: %s\n",strerror(errno));
                goto err;
            }
            read_offset += state.key_size;

            // check key
            if(memcmp(key, empty_key, state.key_size) == 0){
                continue;
            }
            if(memcmp(key, deleted_key, state.key_size) == 0){
                ndeleted_key++;
                continue;
            }
            if(value < 1 || value > 100000000){
                nother_key++;
                continue;
            }
            int hashed_key = 0;
            tmp = format_handler(key, strlen(key), value, &hashed_key);
            // write aof files if dump_aof is 1
            if(dump_aof == 1 && add_aof(aof_set + hashed_key, tmp) == COUNTER_ERR)
                fprintf(stderr, "add_aof error\n");
            saved_key++;
            free(tmp);
            print_count += key_count;
        }
        // show parse
        if(print_count > PRINT_BLOCK){
            print_count = 0;
            sprintf(buf, "\nblock_id=%d block_size=%lfM\n"
                    "key_count=%lld saved_key=%lld\n"
                    "deleted_key=%lld other_key=%lld\n"
                    "%lf%% finished\n\n",
                    i, REDISCOUNTER_RDB_BLOCK / 1024.0 / 1024.0,
                    key_count, saved_key,
                    ndeleted_key, nother_key,
                    count > i ? i * 100 / (double)count : 100);
            show_state(buf);
        }
    }

    // save the data in buffer
    for(i = 0; i < aof_number; i++){
        // write aof files if dump_aof is 1
        if(dump_aof == 1 && save_aof(aof_set + i) == COUNTER_ERR)
            fprintf(stderr, "save_aof error\n");
        sdsfree((aof_set + i)->buffer);
        free((aof_set + i)->filename);
        fclose((aof_set + i)->fp);
    }

    //check if all the keys parsed
    if(state.used != ndeleted_key + nother_key + saved_key){
        fprintf(stderr, "parsed error, didn't read all the keys, still got:%lld\n",state.used - (ndeleted_key + nother_key + saved_key));
        goto err;
    }

    // show all done info
    sprintf(buf, "all done: saved_key=%lld deleted_key=%lld other_key=%lld\n", saved_key, ndeleted_key, nother_key);
    show_state(buf);

    free(key);
    sdsfree(empty_key);
    sdsfree(deleted_key);
    sdsfree(buf);
    return COUNTER_OK;

err:
    free(key);
    sdsfree(empty_key);
    sdsfree(deleted_key);
    sdsfree(buf);
    return COUNTER_ERR;
}

/**
 * @brief rdb_load
 * Main function of this file
 * @param filename
 * @return
 */
int rdb_load(char *filename, format_kv_handler format_handler){
    // init time recoders
    _time_begin = clock();
    show_state("parse begin...\n");

    if(!filename){
        fprintf(stderr, "Invalid filename\n");
        return COUNTER_ERR;
    }
    FILE * fp;
    Aof * aof_set  = NULL;

    fp = fopen(filename,"r");
    if (!fp) {
        fprintf(stderr, "Error opening %s for loading, probably need an absolute path. error: %s\n",
            filename, strerror(errno));
        goto err;
    }

    rdb_state state;
    /*------ parse header section of rdb file ------*/
    if(init_rdb_state(&state, fp) == COUNTER_ERR){
        fprintf(stderr, "init_rdb_state failed\n");
        goto err;
    }
    aof_set = set_aofs();
    if(!aof_set){
        fprintf(stderr, "aof_set failed\n");
        goto err;
    }

    /*------ parse data section of rdb file ------*/
    if(rdb_load_dict(fp, state, aof_set, format_handler) == COUNTER_ERR){
        fprintf(stderr, "rdbLoadDict failed\n");
        goto err;
    }
    // end of function
    fclose(fp);
    fp = NULL;
    free(aof_set);
    return COUNTER_OK;

err:
    fclose(fp);
    free(aof_set);
    return COUNTER_ERR;
}

