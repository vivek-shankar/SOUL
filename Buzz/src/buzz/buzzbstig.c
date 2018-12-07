#include "buzzbstig.h"
#include "buzzmsg.h"
#include "buzzvm.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

/****************************************/
/****************************************/

#define function_register(FNAME)                                        \
   buzzvm_dup(vm);                                                      \
   buzzvm_pushs(vm, buzzvm_string_register(vm, #FNAME, 1));             \
   buzzvm_pushcc(vm, buzzvm_function_register(vm, buzzbstig_ ## FNAME)); \
   buzzvm_tput(vm);

#define id_get()                                          \
   buzzvm_lload(vm, 0);                                   \
   buzzvm_pushs(vm, buzzvm_string_register(vm, "id", 1)); \
   buzzvm_tget(vm);                                       \
   uint16_t id = buzzvm_stack_at(vm, 1)->i.value;

#define add_field(FIELD, VAL, METHOD)                       \
   buzzvm_dup(vm);                                          \
   buzzvm_pushs(vm, buzzvm_string_register(vm, #FIELD, 1)); \
   buzzvm_ ## METHOD(vm, VAL->FIELD);                       \
   buzzvm_tput(vm);

/****************************************/
/****************************************/

int buzzbstig_register(struct buzzvm_s* vm) {
   /* Push 'stigmergy' table */
   buzzvm_pushs(vm, buzzvm_string_register(vm, "bstigmergy", 1));
   buzzvm_pusht(vm);
   /* Add 'create' function */
   buzzvm_dup(vm);
   buzzvm_pushs(vm, buzzvm_string_register(vm, "create", 1));
   buzzvm_pushcc(vm, buzzvm_function_register(vm, buzzbstig_create));
   buzzvm_tput(vm);
   /* Register the 'stigmergy' table */
   buzzvm_gstore(vm);
   return vm->state;
}

/****************************************/
/****************************************/

uint32_t buzzbstig_key_hash(const void* key) {
   return buzzobj_hash(*(buzzobj_t*)key);
}

int buzzbstig_key_cmp(const void* a, const void* b) {
   return buzzobj_cmp(*(buzzobj_t*)a, *(buzzobj_t*)b);
}

/****************************************/
/****************************************/

buzzblob_chunk_t buzzbstig_chunk_new(uint32_t hash, // Hash of chunk
                                     char* chunk) {
   buzzblob_chunk_t e = (buzzblob_chunk_t)malloc(sizeof(struct buzzblob_chunk_s));
   e->hash = hash;
   uint32_t len = strlen(chunk);
   char* chunk_cpy = (char*)malloc(len * sizeof(char) + 1);
   if (len > 0) memcpy(chunk_cpy, chunk, len * sizeof(char));
   *(chunk_cpy + len)=0;
   e->chunk = chunk_cpy;
   return e;
}

/****************************************/
/****************************************/

buzzbstig_elem_t buzzbstig_elem_new(buzzobj_t data,
                                    uint16_t timestamp,
                                    uint16_t robot) {
   buzzbstig_elem_t e = (buzzbstig_elem_t)malloc(sizeof(struct buzzbstig_elem_s));
   e->data = data;
   e->timestamp = timestamp;
   e->robot = robot;
   return e;
}

/****************************************/
/****************************************/

void buzzblob_slot_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   buzzdict_destroy( &((*(buzzblob_elem_t*)data)->data) );
   buzzdarray_destroy( &((*(buzzblob_elem_t*)data)->available_list) );
   buzzdarray_destroy( &((*(buzzblob_elem_t*)data)->locations) );
   free(*(buzzblob_elem_t*)data);
   free(data);
}

/****************************************/
/****************************************/

void buzzblob_chunk_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   free((*(buzzblob_chunk_t*)data)->chunk);
   free(*(buzzblob_chunk_t*)data);
   free(data);
}

void buzzblob_location_destroy(uint32_t pos, void* data, void* params) {
   buzzblob_location_t l = *(buzzblob_location_t*)data;
   free(l);
}
/****************************************/
/****************************************/

buzzblob_elem_t buzzchunk_slot_new(uint32_t hash, uint32_t size) {
   buzzblob_elem_t x = (buzzblob_elem_t)malloc(sizeof(struct buzzblob_elem_s));
   x->data = buzzdict_new(
      10,
      sizeof(uint16_t),
      sizeof(buzzblob_chunk_t),
      buzzdict_uint16keyhash,
      buzzdict_uint16keycmp,
      buzzblob_chunk_destroy);
   x->hash=hash;
   x->size=size;
   x->available_list = buzzdarray_new(10, sizeof(uint16_t),
                                          NULL);
   x->locations = buzzdarray_new(10, sizeof(buzzblob_location_t),
                                          buzzblob_location_destroy);
   x->priority = 1;
   x->status=BUZZBLOB_BUFFERING;
   x->relocstate=BUZZBLOB_OPEN; 
   return x;
}

/****************************************/
/****************************************/
buzzdict_t buzzbstig_blob_slot_new(){
   buzzdict_t s = buzzdict_new(10,
                             sizeof(uint16_t),
                             sizeof(buzzblob_elem_t),
                             buzzdict_uint16keyhash,
                             buzzdict_uint16keycmp,
                             buzzblob_slot_destroy);
   return s;
}

/****************************************/
/****************************************/

void buzz_blob_slot_holders_new(buzzvm_t vm, uint16_t id, uint16_t k, uint32_t blob_size, uint32_t hash){
  
   const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
   if(s){ /* Bstig exsists */
      /* Look for blob key in blob bstig slot*/
      const buzzblob_elem_t* v_blob = buzzdict_get(*s, &(k), buzzblob_elem_t);
      if(v_blob){ /* Found, destroy it */
         buzzdict_remove(*s, &(k));
      } 
      /* create new blob chunk slot */
      buzzblob_elem_t blb = buzzchunk_slot_new(hash, blob_size);
      buzzdict_set(*s, &(k), &blb);
      /* Create chunk stigmergy */
      //buzzbstig_create_generic(vm, k->i.value);
      /* Look for chunk stig for refreshing */ /* has to be just the key */
      // uint32_t chunk_stig_index =  buzzdarray_find(vm->chunk_stig, buzzdict_uint16keycmp, &(k->i.value));
      // if(chunk_stig_index == buzzdarray_size(vm->chunk_stig)){
      //    /* not found, add chunk sting refresh */
      //    buzzdarray_push(vm->chunk_stig, &(k->i.value));
      // }
   }
}

/****************************************/
/****************************************/

buzzbstig_elem_t buzzbstig_blob_elem_new(buzzvm_t vm, uint16_t id, buzzobj_t k, buzzobj_t data,
                                    uint16_t timestamp,
                                    uint16_t robot) {
   buzzbstig_elem_t e = (buzzbstig_elem_t)malloc(sizeof(struct buzzbstig_elem_s));
   /* Hash the blob*/
   uint32_t* hash = buzzbstig_md5(data->s.value.str, strlen(data->s.value.str));
   // char hash_buffer[9];
   // sprintf(hash_buffer,"%8X",hash[0]);
   /* Store the hash of the blob */
   buzzobj_t hash_k = buzzheap_newobj(vm, BUZZTYPE_INT);
   e->data = hash_k;
   e->timestamp = timestamp;
   e->robot = robot;
   hash_k->i.value = hash[0];
   //uint16_t id = k->i.value; 
   buzz_blob_slot_holders_new(vm,id,k->i.value,strlen(data->s.value.str),hash_k->i.value);
   /* Get the blob location from its slot */
   buzzdict_t s = *(buzzdict_get(vm->blobs, &id, buzzdict_t));
   /* Look for blob key in blob bstig slot*/
   buzzblob_elem_t blb = *(buzzdict_get(s, &(k->i.value), buzzblob_elem_t));
   /* Chunk the blob into fragments and add it into respective data slots */
   buzzblob_split_put_bstig(vm, id, data, blb, k, e);
   fprintf(stderr, "[DEBUG] [ROBOT %u] my image hash : %u \n", vm->robot, hash_k->i.value);
   free(hash);
   return e;
}

/****************************************/
/****************************************/

buzzbstig_elem_t buzzbstig_elem_clone(buzzvm_t vm, const buzzbstig_elem_t e) {
   buzzbstig_elem_t x = (buzzbstig_elem_t)malloc(sizeof(struct buzzbstig_elem_s));
   x->data      = buzzheap_clone(vm, e->data);
   x->timestamp = e->timestamp;
   x->robot     = e->robot;
   return x;
}

/****************************************/
/****************************************/

void buzzbstig_elem_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   free(*(buzzbstig_elem_t*)data);
   free(data);
}

/****************************************/
/****************************************/

buzzbstig_t buzzbstig_new() {
   buzzbstig_t x = (buzzbstig_t)malloc(sizeof(struct buzzbstig_s));
   x->data = buzzdict_new(
      10,
      sizeof(buzzobj_t),
      sizeof(buzzbstig_elem_t),
      buzzbstig_key_hash,
      buzzbstig_key_cmp,
      buzzbstig_elem_destroy);
   x->getter = BUZZBLOB_GETTER_OPEN;
   x->onconflict = NULL;
   x->onconflictlost = NULL;
   return x;
}

/****************************************/
/****************************************/

void buzzbstig_destroy(buzzbstig_t* vs) {
   buzzdict_destroy(&((*vs)->data));
   free((*vs)->onconflict);
   free((*vs)->onconflictlost);
   free(*vs);
}

/****************************************/
/****************************************/

void buzzbstig_elem_serialize(buzzmsg_payload_t buf,
                              const buzzobj_t key,
                              const buzzbstig_elem_t data) {
   buzzobj_serialize    (buf, key);
   buzzobj_serialize    (buf, data->data);
   buzzmsg_serialize_u16(buf, data->timestamp);
   buzzmsg_serialize_u16(buf, data->robot);
}

/****************************************/
/****************************************/

int64_t buzzbstig_elem_deserialize(buzzobj_t* key,
                                   buzzbstig_elem_t* data,
                                   buzzmsg_payload_t buf,
                                   uint32_t pos,
                                   struct buzzvm_s* vm) {
   /* Initialize the position */
   int64_t p = pos;
   /* Create a new bstig entry */
   /* Deserialize the key */
   p = buzzobj_deserialize(key, buf, p, vm);
   if(p < 0) return -1;
   /* Deserialize the data */
   p = buzzobj_deserialize(&((*data)->data), buf, p, vm);
   if(p < 0) return -1;
   /* Deserialize the timestamp */
   p = buzzmsg_deserialize_u16(&((*data)->timestamp), buf, p);
   if(p < 0) return -1;
   /* Deserialize the robot */
   p = buzzmsg_deserialize_u16(&((*data)->robot), buf, p);
   if(p < 0) return -1;
   return p;
}

void buzzbstig_chunk_serialize(buzzmsg_payload_t buf, buzzblob_chunk_t cdata){
   buzzmsg_serialize_string(buf, cdata->chunk);
}

int64_t buzzbstig_chunk_deserialize(char** cdata,
                                     buzzmsg_payload_t buf,
                                     uint32_t pos){
 return buzzmsg_deserialize_string(cdata,buf,pos);
}
/****************************************/
/****************************************/

void buzzbstig_loop_bs_chunk_slot(uint32_t pos, void* data, void* params){
   buzzvm_t vm = *(buzzvm_t*) params;
   buzzobj_t k = buzzobj_new(BUZZTYPE_INT);  // TODO : check for blob size and if non null refresh
   k->i.value = 0;
   buzzbstig_get_generic( vm , *(uint16_t*) data, k);
   buzzobj_destroy(&k);
}

/****************************************/
/****************************************/

void buzzbstig_chunkstig_update(buzzvm_t vm){
   /* For each chunk bstig ids, add a get message */
   buzzdarray_foreach(vm->chunk_stig, buzzbstig_loop_bs_chunk_slot, &vm);
}

/****************************************/
/****************************************/

void buzzbstig_create_generic(buzzvm_t vm, uint16_t id){
   /* Look for bstigmergy */
   const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   if(vs) {
      /* Found, destroy it */
      buzzdict_remove(vm->bstigs, &id);
   }
   /* Create a new bstigmergy */
   buzzbstig_t nvs = buzzbstig_new();
   buzzdict_set(vm->bstigs, &id, &nvs);  
    /* Look for bstig slot in VM */
   const buzzdict_t* blob_bstig_slot = buzzdict_get(vm->blobs, &id, buzzdict_t);
   if(blob_bstig_slot){ 
      /* Found, destroy it */
      buzzdict_remove(vm->blobs, &id);
   } 
   buzzdict_t s = buzzbstig_blob_slot_new();
   buzzdict_set(vm->blobs, &id, &s);
}

/****************************************/
/****************************************/

int buzzbstig_create(buzzvm_t vm) {
   buzzvm_lnum_assert(vm, 1);
   /* Get bstig id */
   buzzvm_lload(vm, 1);
   buzzvm_type_assert(vm, 1, BUZZTYPE_INT);
   uint16_t id = buzzvm_stack_at(vm, 1)->i.value;
   buzzvm_pop(vm);
   /* Call the generic function to register with vm */
   buzzbstig_create_generic(vm,id);
   /* Create a table */
   buzzvm_pusht(vm);
   /* Add data and methods */
   buzzvm_dup(vm);
   buzzvm_pushs(vm, buzzvm_string_register(vm, "id", 1));
   buzzvm_pushi(vm, id);
   buzzvm_tput(vm);
   function_register(foreach);
   function_register(size);
   function_register(put);
   function_register(putblob);
   function_register(get);
   function_register(getblob);
   function_register(blobstatus);
   function_register(setgetter);
   function_register(getblobseqdone);
   function_register(onconflict);
   function_register(onconflictlost);
   /* Return the table */
   return buzzvm_ret1(vm);
}

/****************************************/
/****************************************/

void buzzbstig_put_generic(buzzvm_t vm, uint16_t id, buzzobj_t k, buzzobj_t v){            
   /* Look for blob stigmergy */                            
   const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);  
   if(vs) {
      /* Look for the element */
      const buzzbstig_elem_t* x = buzzbstig_fetch(*vs, &k);
      if(x) {
         /* Element found */
         if(v->o.type != BUZZTYPE_NIL) {
            /* New value is not nil, update the existing element */
            (*x)->data = v;
            ++((*x)->timestamp);
            (*x)->robot = vm->robot;
            /* Append a PUT message to the out message queue */
            buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, *x, 0);
         }
         else {
            /* New value is nil, must delete the existing element */
            /* Make a new element with nil as value to update neighbors */
            buzzbstig_elem_t y = buzzbstig_elem_new(
               buzzobj_new(BUZZTYPE_NIL), // nil value
               (*x)->timestamp + 1,       // new timestamp
               vm->robot);                // robot id
            /* Append a PUT message to the out message queue with nil in it */
            buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, y, 0);
            /* Delete the existing element */
            buzzbstig_remove(*vs, &k);
         }
      }
      else if(v->o.type != BUZZTYPE_NIL) {
         /* Element not found and new value is not nil, store it */
         buzzbstig_elem_t y = buzzbstig_elem_new(v, 1, vm->robot);
         buzzbstig_store(*vs, &k, &y);
         /* Append a PUT message to the out message queue */
         buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, y, 0);
      }
   }
}

/****************************************/
/****************************************/

int buzzbstig_put(buzzvm_t vm) {
   buzzvm_lnum_assert(vm, 2);
   /* Get bstig id */
   id_get();
   /* Get key */
   buzzvm_lload(vm, 1);
   buzzobj_t k = buzzvm_stack_at(vm, 1);
   /* Get value */
   buzzvm_lload(vm, 2);
   buzzobj_t v = buzzvm_stack_at(vm, 1);
   /* Put the tuple into bstig*/
   buzzbstig_put_generic(vm,id,k,v);
   /* Return */
   return buzzvm_ret0(vm);
}

/****************************************/
/****************************************/

int buzzbstig_putblob(buzzvm_t vm) {
   buzzvm_lnum_assert(vm, 2);
   /* Get bstig id */
   id_get();
   /* Get key */
   buzzvm_lload(vm, 1);
   buzzobj_t k = buzzvm_stack_at(vm, 1);
   /* Get value */
   buzzvm_lload(vm, 2);
   buzzobj_t v = buzzvm_stack_at(vm, 1);
   /* Look for blob stigmergy */
   const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   if(vs) {
      /* Look for the element */
      const buzzbstig_elem_t* x = buzzbstig_fetch(*vs, &k);
      if(x) {
         /* Element found */
         if(v->o.type != BUZZTYPE_NIL) {
            printf("[UnImplemented feature] Requested blob put with an exsisting key\n");
            // TODO: To be implemented
            /* New value is not nil, update the existing element */
            /* Hash the blob*/
            /*uint32_t* hash = buzzbstig_md5(v->s.value.str, strlen(v->s.value.str));
	         char hash_buffer[9];
	         sprintf(hash_buffer,"%8X",hash[0]);*/
            /* Store the blob with its hash*/
            /*buzzvm_pushs(vm, buzzvm_string_register(vm, hash_buffer,1));
            buzzvm_pushi(vm, k->i.value);*/
            //buzzvm_pushs(vm, buzzvm_string_register(vm, v->s.value.str,1));
           /* v = buzzvm_stack_at(vm, 1);
            buzzobj_t hash_k = buzzvm_stack_at(vm, 2); 
   	      buzzvm_gstore(vm);
            free(hash);
            (*x)->data = hash_k;
	         //(*x)->blob = v;
            ++((*x)->timestamp);
            (*x)->robot = vm->robot;*/
            /* Append a PUT message to the out message queue */
            /*buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, *x, vs_size);*/
         }
         else {
            /* New value is nil, must delete the existing element */
            /* Make a new element with nil as value to update neighbors */
            buzzbstig_elem_t y = buzzbstig_elem_new(
               buzzobj_new(BUZZTYPE_NIL), // nil value
               (*x)->timestamp + 1,       // new timestamp
               vm->robot);                // robot id
            /* Append a PUT message to the out message queue with nil in it */
            buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, y, 1);
            /* Delete the existing element */
            buzzbstig_remove(*vs, &k);
            /* Delete the blob struct */
            /* Get the blob location from its slot */
            buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
            buzzblob_elem_t* blb = NULL;

            if(s){
               /* Look for blob key in blob bstig slot*/
               blb = buzzdict_get(*s, &(k->i.value), buzzblob_elem_t);
            }
            if(blb){
               (vm->cmonitor->chunknum)-= buzzdarray_size((*blb)->available_list);
               //buzzdarray_clear((*blb)->available_list, 10);
               buzzdict_remove(*s,&(k->i.value));
               printf("buzz removed key \n");

            } 
         }        
      }
      else if(v->o.type != BUZZTYPE_NIL) {
         /* Element not found and new value is not nil, store it */
         buzzbstig_elem_t y = buzzbstig_blob_elem_new(vm, id, k, v, 1, vm->robot);
         buzzbstig_store(*vs, &k, &y);
         /* Append a PUT message to the out message queue */
         buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, y, 1);
      }
   }
   /* Return */
   return buzzvm_ret0(vm);
}

/****************************************/
/****************************************/

struct buzzbstig_foreach_params {
   buzzvm_t vm;
   buzzobj_t fun;
};

void buzzbstig_foreach_entry(const void* key, void* data, void* params) {
   /* Cast params */
   struct buzzbstig_foreach_params* p = (struct buzzbstig_foreach_params*)params;
   if(p->vm->state != BUZZVM_STATE_READY) return;
   /* Push closure and params (key, value, robot) */
   buzzvm_push(p->vm, p->fun);
   buzzvm_push(p->vm, *(buzzobj_t*)key);
   buzzvm_push(p->vm, (*(buzzbstig_elem_t*)data)->data);
   buzzvm_pushi(p->vm, (*(buzzbstig_elem_t*)data)->robot);
   /* Call closure */
   p->vm->state = buzzvm_closure_call(p->vm, 3);
}

int buzzbstig_foreach(struct buzzvm_s* vm) {
   /* Make sure you got one argument */
   buzzvm_lnum_assert(vm, 1);
   /* Get bstig id */
   id_get();
   /* Look for virtual stigmergy */
   const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   if(vs) {
      /* Virtual stigmergy found */
      /* Get closure */
      buzzvm_lload(vm, 1);
      buzzvm_type_assert(vm, 1, BUZZTYPE_CLOSURE);
      buzzobj_t c = buzzvm_stack_at(vm, 1);
      /* Go through the elements and apply the closure */
      struct buzzbstig_foreach_params p = { .vm = vm, .fun = c };
      buzzdict_foreach((*vs)->data, buzzbstig_foreach_entry, &p);
   }
   /* Return */
   return buzzvm_ret0(vm);
}

/****************************************/
/****************************************/

int buzzbstig_size(buzzvm_t vm) {
   buzzvm_lnum_assert(vm, 0);
   /* Get bstig id */
   id_get();
   /* Look for virtual stigmergy */
   const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   if(vs) {
      /* Virtual stigmergy found, return its size */
      buzzvm_pushi(vm, buzzdict_size((*vs)->data));
   }
   else {
      /* Virtual stigmergy not found, return 0 */
      buzzvm_pushi(vm, 0);
   }
   /* Return */
   return buzzvm_ret1(vm);
}

/****************************************/
/****************************************/

void buzzbstig_get_generic(buzzvm_t vm, uint16_t id, buzzobj_t k){
   /* Look for virtual stigmergy */
   const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   if(vs) {
      /* Virtual stigmergy found */
      /* Look for key and push result */
      const buzzbstig_elem_t* e = buzzbstig_fetch(*vs, &k);
      if(e) {
         /* Key found */
         buzzvm_push(vm, (*e)->data);
         /*if ((*e)->blob->s.value.str){
            //fprintf(stderr, "[DEBUG] [ROBOT %u] my image %s\n", vm->robot,(*e)->blob->s.value.str);
         }   
         else
         fprintf(stderr, "[DEBUG] [ROBOT %u] my image [null] \n", vm->robot);*/
         /* Look for blob stig slot in VM */
         const buzzdict_t* blob_bstig_slot = buzzdict_get(vm->blobs, &id, buzzdict_t);
         const buzzblob_elem_t* v_blob;
         uint8_t blob_entry=0;
         if(blob_bstig_slot){ /* Blob found in  */
            /* Look for blobkey in blob bstig slot*/
            v_blob = buzzdict_get(*blob_bstig_slot, &(k->i.value), buzzblob_elem_t);
            if(v_blob) blob_entry=1;
         } 
            /* Append the message to the out message queue */
            buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_QUERY, id, k, *e, blob_entry);
      }
      else {
         /* Key not found, make a new one containing nil */
         buzzvm_pushnil(vm);
         buzzbstig_elem_t x =
            buzzbstig_elem_new(buzzvm_stack_at(vm, 1), // nil value
                               0,                      // timestamp
                               vm->robot);             // robot id
         if(vm->robot == 9 ) printf("robot %u sent a query message ",vm->robot);
         /* Append the message to the out message queue */
         buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_QUERY, id, k, x, 0);
         free(x);
      }
   }
   else {
      /* No virtual stigmergy found, just push false */
      /* If this happens, its a bug */
      buzzvm_pushnil(vm);
   }
}

int buzzbstig_get(buzzvm_t vm) {
   buzzvm_lnum_assert(vm, 1);
   /* Get bstig id */
   id_get();
   /* Get key */
   buzzvm_lload(vm, 1);
   buzzobj_t k = buzzvm_stack_at(vm, 1);
   /* call the generic get function */
   buzzbstig_get_generic(vm, id, k);
   /* Return the value found */
   return buzzvm_ret1(vm);
}


int buzzbstig_blobstatus(buzzvm_t vm) {
   buzzvm_lnum_assert(vm, 1);
   /* Get bstig id */
   id_get();
   /* Get key */
   buzzvm_lload(vm, 1);
   buzzobj_t k = buzzvm_stack_at(vm, 1);
   /* Look for blob stigmergy */
   const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   if(vs) {
      /* Virtual stigmergy found */
      /* Look for key and push result */
      const buzzbstig_elem_t* e = buzzbstig_fetch(*vs, &k);
      if(!e){
         /* Key not found, make a new one containing nil */
         buzzvm_pushnil(vm);
         buzzbstig_elem_t x =
            buzzbstig_elem_new(buzzvm_stack_at(vm, 1), // nil value
                               0,                      // timestamp
                               vm->robot);             // robot id
         /* Append the message to the out message queue */
         buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_QUERY, id, k, x, 0);
         free(x);
      }
   }
   /* look for blob slot */
   const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
   if(s){
      /* Look for blob key in blob bstig slot*/
      const buzzblob_elem_t* v_blob = buzzdict_get(*s, &k->i.value, buzzblob_elem_t);
      if(v_blob){
         if((*v_blob)->relocstate != BUZZBLOB_SINK){
            (*v_blob)->relocstate = BUZZBLOB_SINK;
            /* Find the size of the location list if it is equal the number of chunks then bidding is done ask location holders */
            /* Number of chunks in total for this blob */
            uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
            uint16_t avilable = 0; 
            for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
               /* Get the first location element of lowest priority blob */
               buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
               avilable+=lowloc->availablespace;                  
            }
            if(avilable >= chunk_num){
               /* Send a message to all locations  asking for blob*/
               for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                  /* Get the first location element of lowest priority blob */
                  buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
                  // printf("Requested blob for : id: %u key %u avilable %u chunk_num %u location %u\n",id, k->i.value,avilable, chunk_num, lowloc->rid);    
                  buzzoutmsg_queue_append_blob_request(vm,
                                                        BUZZMSG_BSTIG_BLOB_REQUEST,
                                                        id,
                                                        k->i.value,
                                                        lowloc->rid,
                                                        vm->robot);   
               }
            }
            else{
               /* If the location list does not equal the avilable size then go for state based allocation by broadcasting the source */
               // printf("Requested blob by setting status msg : id: %u key %u status %u\n",id, k->i.value,(*v_blob)->relocstate );
               buzzoutmsg_queue_append_blob_status(vm, BUZZMSG_BSTIG_STATUS, id,
                                                k->i.value, (*v_blob)->relocstate,vm->robot);
            }
            (*v_blob)->request_time = 400;
         }
         if((*v_blob)->status == BUZZBLOB_READY){
            buzzvm_pushi(vm, 1);    // blob available
            /* Return the value found */
            return buzzvm_ret1(vm);
         } 
         else{ // blob not ready
            if( (*v_blob)->request_time > 1){
               (*v_blob)->request_time--;
            } 
            else{
               uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
               uint16_t avilable = 0; 
               for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                  /* Get the first location element of lowest priority blob */
                  buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
                  avilable+=lowloc->availablespace;                  
               }
               if(avilable >= chunk_num){
                  /* Send a message to all locations  asking for blob*/
                  for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                     /* Get the first location element of lowest priority blob */
                     buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
                     // printf("Requested blob for : id: %u key %u avilable %u chunk_num %u location %u\n",id, k->i.value,avilable, chunk_num, lowloc->rid);    
                     buzzoutmsg_queue_append_blob_request(vm,
                                                           BUZZMSG_BSTIG_BLOB_REQUEST,
                                                           id,
                                                           k->i.value,
                                                           lowloc->rid,
                                                           vm->robot);   
                  }
               }
               (*v_blob)->request_time = 400;
            }
            buzzvm_pushi(vm, 0);    // blob available
            /* Return the value found */
            return buzzvm_ret1(vm);
         }
      } // This key is not a blob or no key found
   }
   buzzvm_pushnil(vm); // No BS or key
   /* Return the value found */
   return buzzvm_ret1(vm);
}

int buzzbstig_getblobseqdone(buzzvm_t vm){
   buzzvm_lnum_assert(vm, 1);
   /* Get bstig id */
   id_get();
   /* Get key */
   buzzvm_lload(vm, 1);
   buzzobj_t k = buzzvm_stack_at(vm, 1);
   /* look for bs */
   const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
   if(s){
      /* Look for blob key in blob bstig slot*/
      const buzzblob_elem_t* v_blob = buzzdict_get(*s, &k->i.value, buzzblob_elem_t);
      if(v_blob){
         /* Find the size of the location list if it is equal the number of chunks then bidding is done */
         /* Number of chunks in total for this blob */
         uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
         uint16_t avilable = 0; 
         for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
            /* Get the location elements and calculate size */
            buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
            avilable+=lowloc->availablespace;                  
         }
         if(avilable >= chunk_num && buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P])){
             buzzvm_pushi(vm, 1);    // blob sequence done
            /* Return the value found */
            return buzzvm_ret1(vm);
         }
      }
   }
   buzzvm_pushi(vm, 0);    // blob sequence not done
   /* Return the value found */
   return buzzvm_ret1(vm);
}

int buzzbstig_setgetter(buzzvm_t vm) {
   /* Get bstig id */
   id_get();
   /* Look for blob stigmergy */
   const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   if(vs){
      (*vs)->getter = BUZZBLOB_GETTER;
      /* Inform the neighbours about this */
      buzzoutmsg_queue_append_blob_status(vm, BUZZMSG_BSTIG_STATUS, id,
                                                BROADCAST_MESSAGE_CONSTANT, BUZZBLOB_SETGETTER,vm->robot);
      /* Add an elemnt in refersher to monitor and maintain when robots are moving */
      buzzchunk_reloc_elem_t newelem =(buzzchunk_reloc_elem_t)malloc(sizeof(struct buzzchunk_reloc_elem_s));
      newelem->id = id;
      newelem->key = 0;
      newelem->cid = BUZZBLOB_GETTER;
      newelem->bidsize = 0; 
      newelem->time_to_wait = TIME_TO_REFRESH_GETTER_STATE;
      newelem->time_to_destroy = 0;
      newelem->checkednids = buzzdarray_new(1,sizeof(buzzblob_bidder_t),buzzbstig_blob_bidderelem_destroy);
      buzzdarray_push(vm->cmonitor->getters,&newelem);
   }
   return buzzvm_ret0(vm);
}

int buzzbstig_getblob(buzzvm_t vm) {
   buzzvm_lnum_assert(vm, 1);
   /* Get bstig id */
   id_get();
   /* Get key */
   buzzvm_lload(vm, 1);
   buzzobj_t k = buzzvm_stack_at(vm, 1);
   /* Check blob size and return */
   const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
   if(s){
      /* Look for blob key in blob bstig slot*/
      const buzzblob_elem_t* v_blob = buzzdict_get(*s, &k->i.value, buzzblob_elem_t);
      if(v_blob){
         uint16_t vs_size = buzzdict_size((*v_blob)->data);
         uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
         // printf("[Debug ] rid : 8, vs_size : %u, blob element size : %u \n",vs_size, chunk_num );
         if(vs_size == chunk_num){
            if(buzzbstig_construct_blob(vm,*v_blob)){
               /* Reconstruction successful */
               /* Return the value found */
               return buzzvm_ret1(vm);
            }
            else {
               /* Reconstruction failed */
               fprintf(stderr, "[BUG] [ROBOT %u] Blog reconstruction failed, BS id :%u, Key : %u \n", vm->robot, id, k->i.value);
            }
         }
      }
   }
   else{
      // No BS this is a bug
   }
   buzzvm_pushnil(vm); 
   /* Return the value found */
   return buzzvm_ret1(vm);
}
/****************************************/
/****************************************/

int buzzbstig_onconflict(struct buzzvm_s* vm) {
   buzzvm_lnum_assert(vm, 1);
   /* Get bstig id */
   id_get();
   /* Look for virtual stigmergy */
   const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   if(vs) {
      /* Virtual stigmergy found */
      /* Get closure */
      buzzvm_lload(vm, 1);
      buzzvm_type_assert(vm, 1, BUZZTYPE_CLOSURE);
      /* Clone the closure */
      if((*vs)->onconflict) free((*vs)->onconflict);
      (*vs)->onconflict = buzzheap_clone(vm, buzzvm_stack_at(vm, 1));
   }
   else {
      /* No virtual stigmergy found, just push false */
      /* If this happens, its a bug */
      buzzvm_pushnil(vm);
      fprintf(stderr, "[BUG] [ROBOT %u] Can't find virtual stigmergy %u\n", vm->robot, id);
   }
   /* Return the value found */
   return buzzvm_ret0(vm);
}

/****************************************/
/****************************************/

int buzzbstig_onconflictlost(struct buzzvm_s* vm) {
   buzzvm_lnum_assert(vm, 1);
   /* Get bstig id */
   id_get();
   /* Look for virtual stigmergy */
   const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   if(vs) {
      /* Virtual stigmergy found */
      /* Get closure */
      buzzvm_lload(vm, 1);
      buzzvm_type_assert(vm, 1, BUZZTYPE_CLOSURE);
      /* Clone the closure */
      if((*vs)->onconflictlost) free((*vs)->onconflictlost);
      (*vs)->onconflictlost = buzzheap_clone(vm, buzzvm_stack_at(vm, 1));
   }
   else {
      /* No virtual stigmergy found, just push false */
      /* If this happens, its a bug */
      buzzvm_pushnil(vm);
      fprintf(stderr, "[BUG] [ROBOT %u] Can't find virtual stigmergy %u\n", vm->robot, id);
   }
   /* Return the value found */
   return buzzvm_ret0(vm);
}

/****************************************/
/****************************************/

buzzbstig_elem_t buzzbstig_onconflict_call(buzzvm_t vm,
                                           buzzbstig_t vs,
                                           buzzobj_t k,
                                           buzzbstig_elem_t lv,
                                           buzzbstig_elem_t rv) {
   /* Was a conflict manager defined? */
   if(vs->onconflict) {
      /* Push closure */
      buzzvm_push(vm, vs->onconflict);
      /* Push key */
      buzzvm_push(vm, k);
      /* Make table for local value */
      buzzvm_pusht(vm);
      add_field(robot, lv, pushi);
      add_field(data, lv, push);
      add_field(timestamp, lv, pushi);
      /* Make table for remote value */
      buzzvm_pusht(vm);
      add_field(robot, rv, pushi);
      add_field(data, rv, push);
      add_field(timestamp, rv, pushi);
      /* Call closure (key, lv, rv on the stack) */
      buzzvm_closure_call(vm, 3);
      /* Make new entry with return value */
      /* Make sure it's a table */
      if(buzzvm_stack_at(vm, 1)->o.type != BUZZTYPE_TABLE) {
         fprintf(stderr, "[WARNING] [ROBOT %u] virtual stigmergy onconflict(): Return value type is %s, expected table\n", vm->robot, buzztype_desc[buzzvm_stack_at(vm, 1)->o.type]);
         return NULL;
      }
      /* Get the robot id */
      buzzobj_t ret = buzzvm_stack_at(vm, 1);
      buzzvm_pushs(vm, buzzvm_string_register(vm, "robot", 1));
      buzzvm_tget(vm);
      if(buzzvm_stack_at(vm, 1)->o.type != BUZZTYPE_INT)
         return NULL;
      uint16_t robot = buzzvm_stack_at(vm, 1)->i.value;
      buzzvm_pop(vm);
      /* Get the data */
      buzzvm_push(vm, ret);
      buzzvm_pushs(vm, buzzvm_string_register(vm, "data", 1));
      buzzvm_tget(vm);
      buzzobj_t data = buzzvm_stack_at(vm, 1);
      buzzvm_pop(vm);
      /* Make new entry */
      return buzzbstig_elem_new(data, lv->timestamp, robot);
   }
   else {
      /* No conflict manager, use default behavior */
      /* If both values are not nil, keep the value of the robot with the higher id */
      if(((lv->data->o.type == BUZZTYPE_NIL) && (rv->data->o.type == BUZZTYPE_NIL)) ||
         ((lv->data->o.type != BUZZTYPE_NIL) && (rv->data->o.type != BUZZTYPE_NIL))) {
         if(lv->robot > rv->robot)
            return buzzbstig_elem_clone(vm, lv);
         else
            return buzzbstig_elem_clone(vm, rv);
      }
      /* If my value is not nil, keep mine */
      if(lv->data->o.type != BUZZTYPE_NIL)
         return buzzbstig_elem_clone(vm, lv);
      /* If the other value is not nil, keep that one */
      if(rv->data->o.type != BUZZTYPE_NIL)
         return buzzbstig_elem_clone(vm, rv);
      /* Otherwise nothing to do */
      return NULL;
   }
}

/****************************************/
/****************************************/

void buzzbstig_onconflictlost_call(buzzvm_t vm,
                                   buzzbstig_t vs,
                                   buzzobj_t k,
                                   buzzbstig_elem_t lv) {
   /* Was a conflict manager defined? */
   if(vs->onconflictlost) {
      /* Push closure */
      buzzvm_push(vm, vs->onconflictlost);
      /* Push key */
      buzzvm_push(vm, k);
      /* Make table for local value */
      buzzvm_pusht(vm);
      add_field(robot, lv, pushi);
      add_field(data, lv, push);
      add_field(timestamp, lv, pushi);
      /* Call closure (key and table are on the stack) */
      buzzvm_closure_call(vm, 2);
   }
}

void buzzbstig_allocate_chunks(buzzvm_t vm,
                                uint16_t id,
                                uint16_t key,
                                uint16_t bidderid,
                                uint16_t bidsize){

   /* Look for blob holder and create a new if none exsists */
   const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
   const buzzblob_elem_t* v_blob = NULL;
   /*Nothing to do, if bs id doesnot exsist*/
   if(s){
      /* Look for blob key in blob bstig slot*/
      v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
      if(v_blob){
         /* Add the chunks from the availabilty list */
         if(bidsize <= buzzdarray_size( (*v_blob)->available_list ) ){
            /* Create an element for key */
            buzzobj_t k = buzzobj_new(BUZZTYPE_INT);  // TODO : try to unify args and hence avoid creation of buzzobj
            k->i.value = key;
            /* Look for virtual stigmergy */
            const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
            /* Fetch the element */
            const buzzbstig_elem_t* l = buzzbstig_fetch(*vs, &k);
            // printf("allocating chunk : id : %u , key : %u \n",id,k->i.value ); 
            for(int i=0;i < bidsize;i++){

               uint16_t cid = buzzdarray_last((*v_blob)->available_list,uint16_t);
               const buzzblob_chunk_t* cdata = buzzdict_get((*v_blob)->data, &cid, buzzblob_chunk_t);

               // printf("Allocating chunk: %i , cid : %u\n",i,cid  );
               /* Add to P2P Queue */
               buzzoutmsg_queue_append_chunk(vm,
                                             BUZZMSG_BSTIG_CHUNK_PUT,
                                             id,
                                             k,
                                             *l,
                                             (*v_blob)->size,
                                             cid,
                                             *cdata,
                                             bidderid);
               
               
               /* Delete the existing element */
               buzzbstig_remove( (*v_blob), &cid); 
               /* Remove the last element from the avilable list */
               buzzdarray_pop((*v_blob)->available_list);
               /* Decrease the size in cmon */
               (vm->cmonitor->chunknum)--;
            }
            /* Add to location list */
            struct buzzblob_bidder_s locationcmp = {.rid = bidderid, .availablespace = bidsize};
            buzzblob_location_t ploccmp = &locationcmp;
            /* Does the location already exsists */
            uint16_t index = buzzdarray_find((*v_blob)->locations,buzzbstig_blob_bidderelem_key_cmp,&(ploccmp));
            if(index == buzzdarray_size((*v_blob)->locations)){
               buzzblob_location_t locationelem = (buzzblob_location_t)malloc(sizeof(struct buzzblob_bidder_s));
               locationelem->rid = bidderid;
               locationelem->availablespace = bidsize;
               buzzdarray_push((*v_blob)->locations, &(locationelem) );
               /* Add a message indicating the location of the chunks */
               buzzoutmsg_queue_append_chunk_reloc(vm,
                                                  BUZZMSG_BSTIG_CHUNK_STATUS_QUERY,
                                                  id,
                                                  key,
                                                  bidderid,
                                                  BROADCAST_MESSAGE_CONSTANT,
                                                  BUZZ_ADD_BLOB_LOCATION,
                                                  bidsize);
            }
            buzzobj_destroy(&k);
         }
         else{
            /* It is a bug, requested to allocate more than avilable */
            printf("[BUG] [RID: %u ] Requested to allocate more chunks than avilable for bidder : %u\n",vm->robot, bidderid);
         }
      }
   }
}

/****************************************/
/****************************************/  

int buzzbstig_blob_bidderelem_key_cmp(const void* a, const void* b) {
   buzzblob_bidder_t c = *(buzzblob_bidder_t*) a;
   buzzblob_bidder_t d = *(buzzblob_bidder_t*) b;
   if (c->rid < d->rid) return -1;
   if (c->rid > d->rid) return 1;
   return 0;
}

void buzzbstig_blob_bidderelem_destroy(uint32_t pos, void* data, void* param) {
   buzzblob_bidder_t d = *(buzzblob_bidder_t*) data;
   free(d);
}


void buzzbstig_relocation_bider_add(buzzvm_t vm,
                                    uint16_t id,
                                    uint16_t key,
                                    uint16_t availablespace,
                                    uint32_t blob_size,
                                    uint32_t hash,
                                    uint8_t subtype,
                                    uint16_t receiver){
   /* Append to bidder to intiate the bid for this blob */
   buzzchunk_reloc_elem_t newelem =(buzzchunk_reloc_elem_t)malloc(sizeof(struct buzzchunk_reloc_elem_s));
   newelem->id = id;
   newelem->key = key;
   newelem->cid = subtype;
   newelem->bidsize = availablespace; 
   if(subtype != BUZZBSTIG_BID_NEW)
      newelem->time_to_wait = MAX_TIME_TO_HOLD_SPACE_FOR_A_BID;
   else   
      newelem->time_to_wait = MAX_TIME_FOR_THE_BIDDER;
   newelem->time_to_destroy = 0;
   const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   uint8_t getter = BUZZBLOB_GETTER_OPEN;
   if(vs){
      getter = (*vs)->getter;
   }
   /* Create a list to store the bid list */
   newelem->checkednids = buzzdarray_new(10,sizeof(buzzblob_bidder_t),buzzbstig_blob_bidderelem_destroy);
   buzzdarray_push(vm->cmonitor->bidder,&newelem);
   buzzoutmsg_queue_append_bid(vm,
                                BUZZMSG_BSTIG_BLOB_BID,
                                id,
                                key,
                                vm->robot,
                                availablespace,
                                blob_size,
                                hash,
                                getter,
                                (uint16_t)subtype,
                                receiver);
}
   
void buzzblob_split_put_bstig(buzzvm_t vm,
                              uint16_t id,
                              buzzobj_t blob,
                              buzzblob_elem_t blb_struct,
                              buzzobj_t key,
                              buzzbstig_elem_t e){
   /* Chunk the blob */
   uint32_t blb_size  = strlen(blob->s.value.str);
   uint32_t chunk_num = ceil(((float)blb_size/(float)BLOB_CHUNK_SIZE));
   printf(" [DEBUG split] bstig  size : %u, number of chunks: %d \n", blb_size, chunk_num );
   uint32_t temp_size=0;
   for(uint32_t i=0; i< chunk_num;i++){
      uint32_t size_to_chunk = (blb_size-i*BLOB_CHUNK_SIZE > BLOB_CHUNK_SIZE) ?
                                 BLOB_CHUNK_SIZE : blb_size -(i*BLOB_CHUNK_SIZE);
      char chunk_block[size_to_chunk+1];
      /* Copy the chunk */
      memcpy(chunk_block, blob->s.value.str + temp_size, size_to_chunk * sizeof(char));
      /* Set termination character */
      *(chunk_block + size_to_chunk) = 0;
      /* Hash the chunk */
      uint32_t* chunk_hash = buzzbstig_md5(chunk_block,size_to_chunk);
      /* Store the blob chunk with its hash */
      buzzblob_chunk_t cdata = buzzbstig_chunk_new(chunk_hash[0], chunk_block);
      /* Set the chunk status to ready */
      cdata->status=BUZZCHUNK_READY;
      /* Store the blob */
      buzzdict_set(blb_struct->data, &i, &cdata);
      /* Add to available list */
      buzzdarray_push(blb_struct->available_list, &i);
      /* Increase the size of cmon */   
      (vm->cmonitor->chunknum)++;
      // buzzoutmsg_queue_append_chunk(vm,
      //                              BUZZMSG_BSTIG_CHUNK_PUT,
      //                              id,
      //                              key,
      //                              e,
      //                              blb_size,
      //                              i,
      //                              cdata,
      //                              BROADCAST_MESSAGE_CONSTANT);
      //buzzbstig_put_generic(vm, t_stig_id, chunk_key, buzz_obj_hash);
      // printf(" [DEBUG split] chunk : %u, hash: %u, key: %d \n", i, chunk_hash[0], key->i.value );
      free(chunk_hash);
      temp_size+=size_to_chunk;
   }
   /* Set the blob status and relocstatus */
   blb_struct->status=BUZZBLOB_READY;
   blb_struct->relocstate=BUZZBLOB_SOURCE;
   /* Add to relocation bidder */
   buzzbstig_relocation_bider_add(vm,
                                   id,
                                   key->i.value,
                                   0,
                                   blb_size,
                                   blb_struct->hash,
                                   BUZZBSTIG_BID_NEW,
                                   BROADCAST_MESSAGE_CONSTANT);
}

buzzobj_t buzzbstig_construct_blob(buzzvm_t vm, buzzblob_elem_t v_blob){ 
    uint32_t blb_size  = v_blob->size;
    uint32_t chunk_num = ceil(((float)blb_size/(float)BLOB_CHUNK_SIZE));
    // printf(" [DEBUG construct] rid: %u, bstig  size : %u, number of chunks: %d \n", vm->robot, blb_size, chunk_num );
    char blob [blb_size + 1];
    uint32_t cpy_size = 0;
    for(uint32_t i=0; i<chunk_num; i++){
      const buzzblob_chunk_t* cdata = buzzdict_get(v_blob->data, &i, buzzblob_chunk_t);
      uint16_t chunk_size = strlen((*cdata)->chunk);
      memcpy(blob + cpy_size , (*cdata)->chunk, chunk_size * sizeof(char));
      cpy_size+=chunk_size;
    }
    /* Set termination character */
    *(blob + cpy_size) = 0;
    /* Hash the chunk */
    uint32_t* blob_hash = buzzbstig_md5(blob,blb_size);
    if(blob_hash[0] == v_blob->hash){
      printf(" [DEBUG construct] Hash verification successful \
       rid: %u, bstig  size : %u, number of chunks: %d, actual hash : %u , calculated hash %u \n", vm->robot, blb_size, chunk_num, v_blob->hash, blob_hash[0] );
      free(blob_hash);
      buzzvm_pushs(vm, buzzvm_string_register(vm, blob,1));
      //buzzvm_pushnil(vm);
      return buzzvm_stack_at(vm, 1);
    }
    else {
      printf(" [DEBUG construct] Hash verification failed \
       rid: %u, bstig  size : %u, number of chunks: %d, actual hash : %u , calculated hash %u \n", vm->robot, blb_size, chunk_num, v_blob->hash, blob_hash[0] );
      
      free(blob_hash);
      buzzvm_pushnil(vm);
      return buzzvm_stack_at(vm, 1);
   }
   
}


/****************************************/
/****************************************/

int buzzbstig_store_chunk(buzzvm_t vm,
                          uint16_t id,
                          buzzobj_t k,
                          buzzbstig_elem_t v,
                          uint32_t blob_size,
                          uint16_t chunk_index,
                          buzzblob_chunk_t cdata){
   /* Fetch blob bstig slot */
   const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
   if(s){
      /* Look for blob key in blob bstig slot*/
      const buzzblob_elem_t* v_blob = buzzdict_get(*s, &(k->i.value), buzzblob_elem_t);
      if(v_blob){
         /* Look for chunk */
         const buzzblob_chunk_t* blob_chunk = 
                     buzzdict_get((*v_blob)->data, &(chunk_index), buzzblob_chunk_t);
         if(!blob_chunk){ /* Chunk does not exsist, store it */
            if((*v_blob)->relocstate == BUZZBLOB_OPEN || 
               (*v_blob)->relocstate == BUZZBLOB_SINK ||
               (*v_blob)->relocstate == BUZZBLOB_HOST ||
               (*v_blob)->relocstate == BUZZBLOB_HOST_FORWARDING){
               /* Set the chunk status to ready */
               cdata->status=BUZZCHUNK_READY;
               buzzdict_set((*v_blob)->data, &(chunk_index), &cdata);
               // buzzoutmsg_queue_append_chunk(vm,
               //                        BUZZMSG_BSTIG_CHUNK_PUT,
               //                        id,
               //                        k,
               //                        v,
               //                        blob_size,
               //                        chunk_index,
               //                        cdata,
               //                        BROADCAST_MESSAGE_CONSTANT);
               // struct buzzblob_bidder_s locationcmp = {.rid = vm->robot, .availablespace = 0};
               // buzzblob_location_t ploccmp = &locationcmp;
               // /* Does the location already exsists, then the size is already reserved else add*/
               // uint16_t index = buzzdarray_find((*v_blob)->locations,buzzbstig_blob_bidderelem_key_cmp,&(ploccmp));
               if((*v_blob)->relocstate == BUZZBLOB_SINK){
                  (vm->cmonitor->chunknum)++;
               }
               /* Add it to the avilable list */
               buzzdarray_push((*v_blob)->available_list,&chunk_index); 
               /* If the robot got all the chunks then change the status to ready */
               uint16_t vs_size = buzzdict_size((*v_blob)->available_list);
               uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
               if(vs_size == chunk_num){
                  (*v_blob)->status=BUZZBLOB_READY;
               }
               return 1;
            }
            // else if((*v_blob)->relocstate == BUZZBLOB_HOST_FORWARDING ){
            //    /* Set the chunk status to ready */
            //    cdata->status=BUZZCHUNK_READY;
            //    buzzdict_set((*v_blob)->data, &(chunk_index), &cdata);
            //    buzzoutmsg_queue_append_chunk(vm,
            //                           BUZZMSG_BSTIG_CHUNK_PUT,
            //                           id,
            //                           k,
            //                           v,
            //                           blob_size,
            //                           chunk_index,
            //                           cdata,
            //                           BROADCAST_MESSAGE_CONSTANT);
            //    /* Add it to the avilable list */
            //    buzzdarray_push((*v_blob)->available_list,&chunk_index); 
            //    /* If the robot got all the chunks then change the status to ready */
            //    uint16_t vs_size = buzzdict_size((*v_blob)->available_list);
            //    uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
            //    if(vs_size == chunk_num){
            //       (*v_blob)->status=BUZZBLOB_READY;
            //    }
            //    return 1;
            // }
            else if ((*v_blob)->relocstate == BUZZBLOB_FORWARDING){
               /* Create a dummy blob and store */
               char chunk[1];
               *chunk=0;
               buzzblob_chunk_t dum = buzzbstig_chunk_new(0,chunk);
               /* Add it to relocated list */
               //buzzdarray_push((*v_blob)->relocated_list,&chunk_index);
               /* Store the dummy blob */
               buzzdict_set((*v_blob)->data, &(chunk_index), &dum);
               /* Set the chunk status to relocated */
               cdata->status=BUZZCHUNK_RELOCATED;
               /* Forward the blob */
               buzzoutmsg_queue_append_chunk(vm,
                                      BUZZMSG_BSTIG_CHUNK_PUT,
                                      id,
                                      k,
                                      v,
                                      blob_size,
                                      chunk_index,
                                      cdata,
                                      BROADCAST_MESSAGE_CONSTANT);

            }
         } 
      }
   }
   return 0; /* The blob was not stored */
}

/****************************************/
/****************************************/
int buzzbstig_priority_cmp(const void* a, const void* b){
   struct buzzbstig_blobpriority_list_param* c = *(struct buzzbstig_blobpriority_list_param**) a;
   struct buzzbstig_blobpriority_list_param* d = *(struct buzzbstig_blobpriority_list_param**) b;
   if(c->id == d->id && c->key == d->key) return 0;
   return -1;
}

struct buzzbstig_blobpriority_loop2_param
{
   uint16_t id;
   buzzdarray_t plist;
   uint16_t option;  //  0 -> order including reserved and marked 
                     //  1 -> order with marked only
                     //  2 -> order exculding reserved and marked
};
void buzzpriority_struct_destroy(uint32_t pos, void* data, void* params){
 struct buzzbstig_blobpriority_loop_param* m = *(struct buzzbstig_blobpriority_loop_param**) data;
 free(m);  
}
void buzzbstig_blob_forcealloc_destroy(uint32_t pos, void* data, void* params){
   buzzdarray_destroy((buzzdarray_t*) data);
}
struct buzzbstig_blobneigh_loop_param
{
   uint16_t id;
   uint32_t key;      // Here it is assumed blobs are inserted in an assending order
   uint8_t priority;
};

void buzzbstig_blobstatus_loop2(const void* key, void* data, void* params){
   buzzblob_elem_t v_blob = *(buzzblob_elem_t*) data;
   if( v_blob->status == BUZZBLOB_BUFFERED ){
      // if(buzzdarray_size(v_blob->relocated_list)==0){
      //    v_blob->status = BUZZBLOB_READY;
      // }
   }
   else if(v_blob->status == BUZZBLOB_READY){
      // if(buzzdarray_size(v_blob->relocated_list)>0){
      //    v_blob->status = BUZZBLOB_BUFFERED;
      // }
   }  
}

void buzzbstig_blobstatus_loop1(const void* key, void* data, void* params){
   buzzdict_foreach(*(buzzdict_t*)data, buzzbstig_blobstatus_loop2, NULL);
}
uint16_t buzzbstig_priority_cmp_fun(struct buzzbstig_blobpriority_list_param* a, const struct buzzbstig_blobpriority_list_param* b){
return (a->stat && !b->stat) ||
       (a->stat && b->stat && a->priority < b->priority )  || 
       (a->stat && b->stat && a->priority == b->priority && a->key < b->key) ||
       (!a->stat && !b->stat && a->priority < b->priority )||
       (!a->stat && !b->stat && a->priority == b->priority && a->key < b->key);
}
void buzzbstig_blobpriority_loop2(const void* key, void* data, void* params){ // Key loop 
   /* List to store the priority */
   struct buzzbstig_blobpriority_loop2_param* p = (struct buzzbstig_blobpriority_loop2_param*) params;
   buzzblob_elem_t v_blob = *(buzzblob_elem_t*) data;
   // uint32_t available_chunk = buzzdict_size(v_blob->data);
   // uint32_t relocated_chunk = buzzdarray_size((v_blob)->relocated_list);
   // uint32_t marked_list = buzzdarray_size((v_blob)->marked_list);
   // uint32_t reserved_list = buzzdarray_size((v_blob)->reserved_list);
   // uint32_t sizeconsidered =0;
   // if(p->option == 0 ) sizeconsidered = (available_chunk - relocated_chunk - marked_list - reserved_list);
   // else if(p->option == 1 ) sizeconsidered = (available_chunk - relocated_chunk - marked_list);
   // else if(p->option == 2 ) sizeconsidered = (available_chunk - relocated_chunk );
   // /* Does it have atleast one chunk to remove ? */
   // if( sizeconsidered > 0){ // should have atleast one chunk to relocate
      /* This blob is a removal canditate */
      /* Zeroing the memory taken care of calloc */
   struct buzzbstig_blobpriority_list_param* pe = (struct buzzbstig_blobpriority_list_param*)calloc(1,sizeof(struct buzzbstig_blobpriority_list_param));
   pe->id = p->id;
   pe->key =  *(uint16_t*)key;
   pe->priority = v_blob->priority;  
   pe->stat = 0;
   // if(v_blob->relocstate == BUZZBLOB_FORWARDING || // if the blob is moved some where else remove it without checking priority
   //          v_blob->relocstate == BUZZBLOB_SOURCE){
   //    pe->stat = 1;  
   // }
   // else{
   //    pe->stat = 0;
   // }
   uint16_t size = buzzdarray_size(p->plist);
   if(size == 0 ){
      buzzdarray_insert(p->plist,0,&pe);
   }
   else{
      uint16_t in =0;
      for( uint16_t i=0; i<size; i++){
         const struct buzzbstig_blobpriority_list_param* c = buzzdarray_get((p->plist), i, struct buzzbstig_blobpriority_list_param*);       
         if(buzzbstig_priority_cmp_fun(pe,c)){
            buzzdarray_insert(p->plist,i,&pe);
            in=1;
            break;
         }
      }
      if(!in){
         buzzdarray_insert(p->plist,size,&pe);
      } 
   }   
}

void buzzbstig_blobpriority_loop1(const void* key, void* data, void* params){ // Id loop 
   buzzdict_t m_dict = *(buzzdict_t*) data;
   // printf("priority loop 1 : id : %u\n", *(uint16_t*)key);
   struct buzzbstig_blobpriority_loop2_param* p = (struct buzzbstig_blobpriority_loop2_param*) params;
   p->id = *(uint16_t*)key;
   buzzdict_foreach(m_dict, buzzbstig_blobpriority_loop2, params);
}

void buzzbstig_neigh_loop(const void* key, void* data, void* params){ // nid get loop
   buzzdarray_t nida = *(buzzdarray_t*) params;
   uint16_t nid = *(uint16_t*) key;
   buzzdarray_push(nida,&nid);
}

buzzdarray_t buzzbstig_get_prioritylist(buzzvm_t vm){
   /* create a darray for storing the id and key in accending order of priority */
   buzzdarray_t priority = buzzdarray_new(5, sizeof(struct buzzbstig_blobpriority_list_param*), 
                                          buzzpriority_struct_destroy);
   struct buzzbstig_blobpriority_loop2_param p = {.id =0, .plist=priority, .option = 2};
   /* Update the priority list of blobs */
   buzzdict_foreach(vm->blobs, buzzbstig_blobpriority_loop1, &p);
   return priority;
}

int buzzbstig_blob_bidderpriority_key_cmp(const void* a, const void* b) {
   buzzblob_bidder_t c = *(buzzblob_bidder_t*) a;
   buzzblob_bidder_t d = *(buzzblob_bidder_t*) b;
   if (c->role < d->role || (c->role == d->role && c->rid < d->rid) ) return -1;
   else if (c->role > d->role || (c->role == d->role && c->rid > d->rid) ) return  1;
   return 0;
}

void buzzbstig_blobstatus_update(buzzvm_t vm){
   for(int i = 0; i< buzzdarray_size(vm->cmonitor->bidder); i++){
      const buzzchunk_reloc_elem_t celem = 
                        buzzdarray_get(vm->cmonitor->bidder,i,buzzchunk_reloc_elem_t);
      // printf("rid %u Cmon elem %i , k: %u, id: %u type : %u  time : %u \n",vm->robot,i,celem->key,celem->id,celem->cid,celem->time_to_wait);
      // for(int y=0; y< buzzdarray_size(celem->checkednids); y++){
      //    const buzzblob_bidder_t bidelem = 
      //                      buzzdarray_get(celem->checkednids,y,buzzblob_bidder_t);
      //    printf("Bid : %i, bidderid: %u, bidsize: %u, bidder role: %u time to allocate: %u \n", y ,bidelem->rid, bidelem->availablespace, bidelem->role, celem->time_to_wait);
      // }
      if(celem->time_to_wait > 0){
         celem->time_to_wait--;
         if(celem->cid == BUZZCHUNK_BID_ALLOCATION && (celem->time_to_wait == MAX_TIME_TO_HOLD_SPACE_FOR_A_BID/4 || 
                                                      celem->time_to_wait == MAX_TIME_TO_HOLD_SPACE_FOR_A_BID/3  ||
                                                      celem->time_to_wait == MAX_TIME_TO_HOLD_SPACE_FOR_A_BID/2) ){
            /* Look for blob holder */
            const buzzdict_t* s = buzzdict_get(vm->blobs, &(celem->id), buzzdict_t);
            const buzzblob_elem_t* v_blob = NULL;
            /*Nothing to do, if bs id doesnot exsist*/
            if(s){
               /* Look for blob key in blob bstig slot*/
               v_blob = buzzdict_get(*s, &(celem->key), buzzblob_elem_t);
            }
            if(v_blob){
               for(int b = 0; b< buzzdarray_size(celem->checkednids);b++){
                  printf("[RID %u ]Resending a new alloc message due to no responce \n",vm->robot);
                  buzzblob_bidder_t bidelem = buzzdarray_get(celem->checkednids,b,buzzblob_bidder_t);
                  /* ReSend an allocation messsage due to no responce */
                  buzzoutmsg_queue_append_bid(vm,
                                               BUZZMSG_BSTIG_BLOB_BID,
                                               celem->id,
                                               celem->key,
                                               bidelem->rid,
                                               bidelem->availablespace,
                                               (*v_blob)->size,
                                               (*v_blob)->hash,
                                               bidelem->role,
                                               (uint16_t) BUZZCHUNK_BID_ALLOCATION,
                                               bidelem->rid);
               }
            }
         }
      }
      else{
         if(celem->cid == BUZZBSTIG_BID_NEW ){
            /* Check for queue clogging */
            /* If yes, delay allcoaiton */
            if(buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_BLOB_BID]) > QUEUE_CLOGGING_PROTECTOR_QUEUE_SIZE ){
                celem->time_to_wait = MAX_TIME_FOR_THE_BIDDER;

            }
            else{
               uint16_t allocsize=0;
               /* Look for blob holder */
               const buzzdict_t* s = buzzdict_get(vm->blobs, &(celem->id), buzzdict_t);
               const buzzblob_elem_t* v_blob = NULL;
               /*Nothing to do, if bs id doesnot exsist*/
               if(s){
                  /* Look for blob key in blob bstig slot*/
                  v_blob = buzzdict_get(*s, &(celem->key), buzzblob_elem_t);
               }
               if(v_blob){
                  uint16_t chunkpieces =  ceil(((float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE));
                  /* Create a new element inside cmon bidder for allocation monitoring */
                  buzzchunk_reloc_elem_t newelem =(buzzchunk_reloc_elem_t)malloc(sizeof(struct buzzchunk_reloc_elem_s));
                  newelem->id = celem->id;
                  newelem->key = celem->key;
                  newelem->cid = BUZZCHUNK_BID_ALLOCATION;
                  newelem->bidsize = 0; 
                  newelem->time_to_wait = MAX_TIME_TO_HOLD_SPACE_FOR_A_BID;
                  newelem->time_to_destroy = 0;
                  newelem->checkednids = buzzdarray_new(10,sizeof(buzzblob_bidder_t),buzzbstig_blob_bidderelem_destroy);
                  buzzdarray_push(vm->cmonitor->bidder,&newelem);
                  if(!buzzdarray_isempty(celem->checkednids))
                  /* Sort the allocation array in the accending order */
                  buzzdarray_sort(celem->checkednids,buzzbstig_blob_bidderpriority_key_cmp);
                  /* Time for bidders is done, start allocating */
                  for(int j = buzzdarray_size(celem->checkednids)-1; j >= 0 && allocsize < chunkpieces; j--){
                     const buzzblob_bidder_t bidelem = 
                              buzzdarray_get(celem->checkednids,j,buzzblob_bidder_t);
                     uint16_t allocated_size = (allocsize + bidelem->availablespace < chunkpieces) ? bidelem->availablespace : chunkpieces - allocsize;  
                     /* Highest rid gets bid size, send an allocation message */
                     buzzblob_bidder_t addbider = (buzzblob_bidder_t)malloc(sizeof(struct buzzblob_bidder_s));
                     addbider->rid = bidelem->rid;
                     addbider->role =  bidelem->role; 
                     addbider->availablespace = allocated_size;
                     /* Add the size to allocated size */
                     allocsize+=allocated_size;
                     buzzdarray_push(newelem->checkednids, &(addbider) );
                     /* Send an allocation messsage */
                     buzzoutmsg_queue_append_bid(vm,
                                                  BUZZMSG_BSTIG_BLOB_BID,
                                                  celem->id,
                                                  celem->key,
                                                  bidelem->rid,
                                                  allocated_size,
                                                  (*v_blob)->size,
                                                  (*v_blob)->hash,
                                                  bidelem->role,
                                                  (uint16_t) BUZZCHUNK_BID_ALLOCATION,
                                                  bidelem->rid);
                  }
                  celem->time_to_wait = MAX_UINT16; // this will not trigger an allocation round again, TODO: Can be done better.
                  if(allocsize < chunkpieces){
                     /* Could not allocate all chunks, intiate the priority policy and remove the oldest blob */
                     /* create a darray for storing the id and key in accending order of priority */
                     buzzdarray_t priority = buzzdarray_new(5, sizeof(struct buzzbstig_blobpriority_list_param*), 
                                                            buzzpriority_struct_destroy);
                     struct buzzbstig_blobpriority_loop2_param p = {.id =0, .plist=priority, .option = 2};
                     /* Update the priority list of blobs */
                     buzzdict_foreach(vm->blobs, buzzbstig_blobpriority_loop1, &p);
                     /* Get the first blob and try force allocation */
                      if(!buzzdarray_isempty(priority) ){
                        for(uint16_t pi=0; pi < buzzdarray_size(priority) && 
                            allocsize < chunkpieces; pi++){
                           const struct buzzbstig_blobpriority_list_param* pe = buzzdarray_get(priority, pi, struct buzzbstig_blobpriority_list_param*);
                           /* Fetch the entries in order form priority list */
                           const buzzdict_t* s = buzzdict_get(vm->blobs, &(pe->id), buzzdict_t);
                           if(s){
                              /* Look for blob key in blob bstig slot*/
                              const buzzblob_elem_t* v_blob = buzzdict_get(*s, &(pe->key), buzzblob_elem_t);
                              if(v_blob){
                                 /* Create a chunk montior for force allocation */
                                 buzzchunk_reloc_elem_t newelem =(buzzchunk_reloc_elem_t)malloc(sizeof(struct buzzchunk_reloc_elem_s));
                                 newelem->id = celem->id;
                                 newelem->key = celem->key;
                                 newelem->cid = BUZZCHUNK_BID_FORCE_ALLOCATION;
                                 newelem->bidsize = 0;  // temp hold the last id from priority list
                                 newelem->time_to_wait = MAX_TIME_TO_HOLD_SPACE_FOR_A_BID;
                                 newelem->time_to_destroy = 0; // temp hold the last key from priority list
                                 newelem->checkednids = buzzdarray_new(10,sizeof(buzzblob_bidder_t),buzzbstig_blob_bidderelem_destroy);
                           
                                 /* Add the allocaiton element into the bidder */
                                 buzzdarray_push(vm->cmonitor->bidder,&newelem);
                                 for(int l=0;l<buzzdarray_size((*v_blob)->locations) && 
                                       allocsize < chunkpieces; l++){
                                    /* Get the first location element of lowest priority blob */
                                    buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,l, buzzblob_location_t);
                                    /* Allocate if I am not allocating to myself */
                                    if(lowloc->rid != vm->robot){
                                       /* Force allocate the chunk */
                                       uint16_t allocated_size = (allocsize + lowloc->availablespace < chunkpieces) ? lowloc->availablespace : chunkpieces - allocsize;  
                                       buzzblob_bidder_t addbider = (buzzblob_bidder_t)malloc(sizeof(struct buzzblob_bidder_s));
                                       addbider->rid = lowloc->rid;
                                       addbider->role =  0; 
                                       addbider->availablespace = allocated_size;
                                       /* Add the size to allocated size */
                                       allocsize+=allocated_size;
                                       buzzdarray_push(newelem->checkednids, &(addbider) );
                                       /* Send a force allocation messsage */
                                       buzzoutmsg_queue_append_bid(vm,
                                                                    BUZZMSG_BSTIG_BLOB_BID,
                                                                    celem->id,
                                                                    celem->key,
                                                                    lowloc->rid,
                                                                    allocated_size,
                                                                    pe->id,
                                                                    pe->key,
                                                                    0,
                                                                    (uint16_t) BUZZCHUNK_BID_FORCE_ALLOCATION,
                                                                    lowloc->rid);
                                    }
                                 }
                                 newelem->bidsize = pe->id;  // temp hold the last id 
                                 newelem->time_to_destroy = pe->key; // temp hold the last key
                              }

                           }

                        }
                     }
                     else{
                        printf("[RID: %u] ERROR, Trying to force allocate but no blob exsist to remove, try increasing available size\n", vm->robot );   
                     }
                     if(allocsize < chunkpieces)
                        printf("[RID: %u] ERROR, Trying to force allocate but no blob exsist to remove, try increasing available size\n", vm->robot );
                     buzzdarray_destroy(&priority);  
                  }
               }
               else{
                  /* It is a bug, bid intiated without a blob*/
               }
            }

         }
         else if(celem->cid == BUZZBSITG_BID_REPLY){
            /* MAX time for allocation expired, simply remove the entry  */
            buzzdarray_remove(vm->cmonitor->bidder,i);  
            return;       

         }
         else if(celem->cid == BUZZCHUNK_BID_ALLOCATION){
            /* Max time to allocate exceded force a robot to eject a blob and place the new one instead */   
            printf("[RID %u] [ERROR] Max time to allocate blocks exceeded, try increasing max time to allocate \n", vm->robot);
            //buzzdarray_remove(vm->cmonitor->bidder,i); 
            return;       
         }
         else if(celem->cid == BUZZCHUNK_BID_ALLOC_ACCEPT){
            /* MAX time for allocation expired, simply remove the entry  */
            buzzdarray_remove(vm->cmonitor->bidder,i);         
            return;
         }
         else if(celem->cid == BUZZCHUNK_BID_ALLOC_REJECT){
            /* MAX time for allocation expired, simply remove the entry  */
            buzzdarray_remove(vm->cmonitor->bidder,i);         
            return;
         }
      }
      if(celem->cid == BUZZCHUNK_BID_ALLOCATION && buzzdarray_isempty(celem->checkednids) ){
         /* Clear all bid info related to this blob once allocation complete */
         uint16_t id = celem->id, key = celem->key;
         buzzdarray_remove(vm->cmonitor->bidder,i);
         struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZBSTIG_BID_NEW};
         buzzchunk_reloc_elem_t newelem = &cmpelem;
         uint16_t cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
         if(cmonindex !=buzzdarray_size(vm->cmonitor->bidder)){
            buzzdarray_remove(vm->cmonitor->bidder,cmonindex);
            return;
         }
      }
      else if(celem->cid == BUZZCHUNK_BID_FORCE_ALLOCATION && buzzdarray_isempty(celem->checkednids)){
         
         buzzdarray_remove(vm->cmonitor->bidder,i);            
         return;   
      }

   }
}

void buzzbstig_blobgetters_update(buzzvm_t vm){
   for(uint16_t ch = 0; ch< buzzdarray_size(vm->cmonitor->getters);ch++){
      const buzzchunk_reloc_elem_t getterelem = buzzdarray_get(vm->cmonitor->getters,ch,buzzchunk_reloc_elem_t);
      if(getterelem->time_to_wait>0){
         getterelem->time_to_wait--;
         //  const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &(getterelem->id), buzzbstig_t);
         // printf("RID %u role %u\n", vm->robot, (*vs)->getter );
      }
      else{
         if(getterelem->cid == BUZZBLOB_GETTER_NEIGH ){
            /* Look for blob stigmergy */
            const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &(getterelem->id), buzzbstig_t);
            if(vs){
               (*vs)->getter = BUZZBLOB_GETTER_OPEN;
            }
            buzzdarray_remove(vm->cmonitor->getters,ch);
         }
         else if(getterelem->cid == BUZZBLOB_GETTER){
            /* Look for blob stigmergy */
            const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &(getterelem->id), buzzbstig_t);
            if(vs){
               /* Inform the neighbours about this */
               buzzoutmsg_queue_append_blob_status(vm, BUZZMSG_BSTIG_STATUS, getterelem->id,
                                                         BROADCAST_MESSAGE_CONSTANT, BUZZBLOB_SETGETTER,vm->robot); 
               getterelem->time_to_wait = TIME_TO_REFRESH_GETTER_STATE;
            }
            else{
               buzzdarray_remove(vm->cmonitor->getters,ch);
            }

         }  
      }
   }
}
void buzzbstig_blobrequest_update(buzzvm_t vm){
   for(uint16_t ch = 0; ch< buzzdarray_size(vm->cmonitor->blobrequest);ch++){
      const buzzchunk_reloc_elem_t requestelem = buzzdarray_get(vm->cmonitor->blobrequest,ch,buzzchunk_reloc_elem_t);
      // printf("rid %u Cmon elem %i , k: %u, id: %u type : %u  time : %u \n",vm->robot,ch,requestelem->key,requestelem->id,requestelem->cid,requestelem->time_to_wait);
      
      if(requestelem->time_to_wait > 0)
            requestelem->time_to_wait--;
      else{
         if(requestelem->cid == BUZZBLOB_REQUESTED_LOCATIONS ){
               buzzdarray_remove(vm->cmonitor->blobrequest,ch);
         }
         else if(requestelem->cid == BUZZBLOB_WAITING_TO_REQUEST_LOCATIONS ){
            /* Check for allocation */
            /* look for bs */
            const buzzdict_t* s = buzzdict_get(vm->blobs, &(requestelem->id), buzzdict_t);
            if(s){
               /* Look for blob key in blob bstig slot*/
               const buzzblob_elem_t* v_blob = buzzdict_get(*s, &(requestelem->key), buzzblob_elem_t);
               if(v_blob){
                  uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
                  uint16_t avilable = 0; 
                  for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                     /* Get thelocation elements and calculate the space */
                     buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
                     avilable+=lowloc->availablespace;                  
                  }
                  /* Is the blob under transport ? then append a request to locations */
                  if(avilable >= chunk_num){
                     /* Send a message to all locations  asking for blob*/
                     for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                        /* Get the first location element of lowest priority blob */
                        buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
                        // printf("[ROBOT %u] BUZZBLOB_WAITING_TO_REQUEST_LOCATIONS Request monitor Requested blob for requester : %u id: %u key %u avilable %u chunk_num %u location %u\n",
                        //    vm->robot,requestelem->bidsize,requestelem->id, requestelem->key,avilable, chunk_num, lowloc->rid);    
                        buzzoutmsg_queue_append_blob_request(vm,
                                                        BUZZMSG_BSTIG_BLOB_REQUEST,
                                                        requestelem->id,
                                                        requestelem->key,
                                                        lowloc->rid,
                                                        requestelem->bidsize);   
                     }
                     buzzdarray_remove(vm->cmonitor->blobrequest,ch);
                  }
                  else{
                     requestelem->time_to_wait = TIME_TO_FORGET_BLOB_REQUEST;
                  }
               }
               else{
                  buzzdarray_remove(vm->cmonitor->blobrequest,ch);
               }
            }
         }
         else if(requestelem->cid == BUZZBLOB_STATUS_CHANGE_DONE ){
               buzzdarray_remove(vm->cmonitor->blobrequest,ch);

         }
         else if(requestelem->cid == BUZZBLOB_WAITING_FOR_BLOB_AVILABILITY ){
            /* Find whether you have all the chunks */
            /* Look for blob holder and create a new if none exsists */
            const buzzdict_t* s = buzzdict_get(vm->blobs, &(requestelem->id), buzzdict_t);
            const buzzblob_elem_t* v_blob = NULL;
            /*Nothing to do, if bs id doesnot exsist*/
            if(s){
               /* Look for blob key in blob bstig slot*/
               v_blob = buzzdict_get(*s, &(requestelem->key), buzzblob_elem_t);
               if(v_blob){   
                  uint16_t available_size=0;
                  for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                     /* Get thelocation elements and calculate the space */
                     buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
                     if(lowloc->rid == vm->robot){
                        available_size = lowloc->availablespace;
                     }                  
                  }
                  /* All chunks are avilable unicast it */
                  if(buzzdarray_size((*v_blob)->available_list) >= available_size){
                      /* Create an element for key */
                     buzzobj_t k = buzzobj_new(BUZZTYPE_INT);  // TODO : try to unify args and hence avoid creation of buzzobj
                     k->i.value = requestelem->key;
                     /* Look for virtual stigmergy */
                     const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &(requestelem->id), buzzbstig_t);
                     /* Fetch the element */
                     const buzzbstig_elem_t* l = buzzbstig_fetch(*vs, &k);
                     for(int i=0;i < buzzdarray_size((*v_blob)->available_list);i++){
                        uint16_t cid = buzzdarray_get((*v_blob)->available_list,i,uint16_t);
                        const buzzblob_chunk_t* cdata = buzzdict_get((*v_blob)->data, &cid, buzzblob_chunk_t);

                        // printf("Delayed unicasting id: %u k: %u on request from %u chunk: %i , cid : %u\n",requestelem->id, requestelem->key, requestelem->bidsize, i,cid  );
                        /* Add to P2P Queue */
                        buzzoutmsg_queue_append_chunk(vm,
                                                      BUZZMSG_BSTIG_CHUNK_PUT,
                                                      requestelem->id,
                                                      k,
                                                      *l,
                                                      (*v_blob)->size,
                                                      cid,
                                                      *cdata,
                                                      requestelem->bidsize);
                     }
                     buzzobj_destroy(&k);
                     buzzdarray_remove(vm->cmonitor->blobrequest,ch);
                  }
                  else{
                     requestelem->time_to_wait = TIME_TO_REFRESH_GETTER_STATE;
                  }
               }
               else{
                  buzzdarray_remove(vm->cmonitor->blobrequest,ch);
               }
            }
         }
      }
   }
}

buzzdarray_t buzzbstig_getlocations(buzzvm_t vm, uint16_t id, uint16_t key){
    const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
   if(s){
      /* Look for blob key in blob bstig slot*/
      const buzzblob_elem_t* v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
      if(v_blob){
         return (*v_blob)->locations;
      }
   }
   return NULL;
}
/*****************************************/
/*****************************************/
int bstig_get_next_nid(buzzvm_t vm, uint16_t cnid){
   if(buzzdict_size(vm->active_neighbors) != 0){
      buzzdarray_t nid = buzzdarray_new(10,
                                       sizeof(uint16_t),
                                       NULL);
      buzzdict_foreach(vm->active_neighbors, buzzbstig_neigh_loop, &nid);
      if(!buzzdarray_isempty(nid))
      buzzdarray_sort(nid,buzzdict_uint16keycmp);
      buzzdarray_destroy(&nid);
   }

return -1;
}

/****************************************/
/****************************************/
   
uint32_t* buzzbstig_md5(const char* in_blob, uint32_t size) {
   uint8_t* blob = (uint8_t*) malloc(sizeof(char)*size);
   memcpy(blob,in_blob,sizeof(char)*size);
   /* Generate md5 hash */    
   uint32_t* h = (uint32_t*) malloc(sizeof(uint32_t)*4); 
   uint8_t *msg = NULL;
   /* All variables are unsigned 32 bit and wrap modulo 2^32 when calculating,
    * r specifies the per-round shift amounts
    */ 
   uint32_t r[] = {7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
                   5,  9, 14, 20, 5,  9, 14, 20, 5,  9, 14, 20, 5,  9, 14, 20,
                   4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
                   6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21};
   /* Use binary integer part of the sines of integers (in radians) as constants
    * Initialize variables:
    */
   uint32_t k[] = {
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee,
        0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
        0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
        0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
        0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa,
        0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
        0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
        0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
        0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
        0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05,
        0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
        0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039,
        0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
        0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391};
 
   h[0] = 0x67452301;
   h[1] = 0xefcdab89;
   h[2] = 0x98badcfe;
   h[3] = 0x10325476;
 
   /* Pre-processing: adding a single 1 bit
    *  append "1" bit to message    
    *  Notice: the input bytes are considered as bits strings,
    *  where the first bit is the most significant bit of the byte.[37] 
    */
   int new_len;
   for(new_len = size*8 + 1; new_len%512!=448; new_len++);
   new_len /= 8;
   /* append 0bits and allocate 64 extra bytes */ 
   msg = calloc(new_len + 64, 1); 
   memcpy(msg, blob, size);
   msg[size] = 128; 
   /* append length at the end of the buffer*/
   uint32_t bits_len = 8*size; 
   memcpy(msg + new_len, &bits_len, 4);         
   /*Process messages in 512-bit chunks */
   int offset;
   for(offset=0; offset<new_len; offset += (512/8)) {
      /* Break chunk into sixteen 32-bit words w[j], 0 ≤ j ≤ 15 */
      uint32_t *w = (uint32_t *) (msg + offset);
      /* Initialize hash value for this chunk */
      uint32_t a = h[0];
      uint32_t b = h[1];
      uint32_t c = h[2];
      uint32_t d = h[3];
      for(uint32_t i = 0; i<64; i++) {
         uint32_t f, g;
         if (i < 16) {
            f = (b & c) | ((~b) & d);
            g = i;
         }
         else if (i < 32) {
            f = (d & b) | ((~d) & c);
            g = (5*i + 1) % 16;
         } 
         else if (i < 48) {
            f = b ^ c ^ d;
            g = (3*i + 5) % 16;          
         }
         else {
            f = c ^ (b | (~d));
            g = (7*i) % 16;
         }
         uint32_t temp = d;
         d = c;
         c = b;
         b = b + LEFTROTATE((a + f + k[i] + w[g]), r[i]);
         a = temp;
      }   
      /* Add this chunk's hash to the result */
      h[0] += a;
      h[1] += b;
      h[2] += c;
      h[3] += d;
   }
   free(msg);
   free(blob);
   return h; 
}

/****************************************/
/****************************************/

void buzzbtigs_serialize_key_foreach(const void* key, void* data, void* params){
    buzzdarray_t bm = *(buzzdarray_t*) params;
    buzzbstig_elem_t belem = *(buzzbstig_elem_t*)((buzzbstig_elem_t*)data);
    buzzobj_t k = *(buzzobj_t*)((buzzobj_t*)key);
    // printf("Serializing key %u \n", k->i.value);
    buzzbstig_elem_serialize(bm,k,belem);
}
void buzzbtigs_serialize_id_foreach(const void* key, void* data, void* params){
    buzzdarray_t bm = *(buzzdarray_t*) params;
    buzzbstig_t bst = *(buzzbstig_t*)((buzzbstig_t*)data);
    uint16_t id = *(uint16_t*)key;
    uint32_t bsize = buzzdict_size(bst->data);
    // printf("Bstigs key dict size %u\n",bsize );
    buzzmsg_serialize_u32(bm, bsize);
    buzzmsg_serialize_u16(bm, id);
    buzzdict_foreach(bst->data,buzzbtigs_serialize_key_foreach, params);
}
void buzzbtigs_blobs_serialize_key_foreach(const void* key, void* data, void* params){
    buzzdarray_t bm = *(buzzdarray_t*) params;
    buzzblob_elem_t bst = *(buzzblob_elem_t*)((buzzblob_elem_t*)data);
    uint16_t k = *(uint16_t*)key;
    // printf("serialization key %u\n",k );
    buzzmsg_serialize_u16(bm, k);
    buzzmsg_serialize_u32(bm, bst->hash);
    buzzmsg_serialize_u32(bm, bst->size);
    buzzmsg_serialize_u8(bm, bst->priority);
    uint32_t locations_size = buzzdarray_size(bst->locations);
    buzzmsg_serialize_u32(bm, locations_size);
    // printf("hash : %u size : %u priority %u location size %u \n",bst->hash,bst->size, bst->priority,locations_size);
    for(int i =0 ;i< locations_size;i++){
      buzzblob_location_t l = buzzdarray_get(bst->locations,i, buzzblob_location_t);
      buzzmsg_serialize_u16(bm, l->rid);
      buzzmsg_serialize_u16(bm, l->availablespace);
      // printf("\t loc : rid %u , avilable %u \n",l->rid, l->availablespace);
    }

}
void buzzbtigs_blobs_serialize_id_foreach(const void* key, void* data, void* params){
    buzzdarray_t bm = *(buzzdarray_t*) params;
    buzzdict_t bst = *(buzzdict_t*)((buzzdict_t*)data);
    uint16_t id = *(uint16_t*)key;
    uint32_t bsize = buzzdict_size(bst);
    // printf("Bstigs key dict size %u\n",bsize );
    buzzmsg_serialize_u32(bm, bsize);
    buzzmsg_serialize_u16(bm, id);
    buzzdict_foreach(bst,buzzbtigs_blobs_serialize_key_foreach, params);
}

void buzzbstig_seralize_blb_stigs(buzzvm_t vm, buzzdarray_t bm){
   uint32_t bstigs_size = buzzdict_size(vm->bstigs);
   // printf("Bstigs id dict size %u\n",bstigs_size );
   buzzmsg_serialize_u32(bm, bstigs_size);
   buzzdict_foreach(vm->bstigs,buzzbtigs_serialize_id_foreach, &bm);
    // printf("Size of after bstig serialization is : %d\n",buzzdarray_size(bm));
   uint32_t blobs_size = buzzdict_size(vm->blobs);
   // printf("Bblobs id dict size %u\n",blobs_size );
   buzzmsg_serialize_u32(bm, blobs_size);
   buzzdict_foreach(vm->blobs,buzzbtigs_blobs_serialize_id_foreach, &bm);

}
/****************************************/
/****************************************/

int64_t buzzbstig_deseralize_blb_stigs_set(buzzvm_t vm,buzzdarray_t a,int64_t pos){
   /* Deserialize and set bstig entiries */
   uint32_t bstigs_size;
   pos = buzzmsg_deserialize_u32(&bstigs_size,a,pos);
   for(int i = 0; i < bstigs_size; ++i){
      uint16_t id;
      uint32_t k_size;
      pos = buzzmsg_deserialize_u32(&k_size,a,pos);
      const buzzbstig_t* vs = NULL ;
      if(k_size!=0){
         pos = buzzmsg_deserialize_u16(&id,a,pos);
         vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
      }
      for(int j = 0; j < k_size; ++j){
         buzzobj_t k;
         buzzbstig_elem_t v = (buzzbstig_elem_t)malloc(sizeof(struct buzzbstig_elem_s));
         pos = buzzbstig_elem_deserialize(&k, &v, a, pos, vm);
         if(pos < 0) {
            fprintf(stderr, "[WARNING] [ROBOT %u] Malformed Buzz blob deserialization of bstig\n", vm->robot);
            free(v);
         }
         else if(vs){
            buzzbstig_store(*vs, &k, &v);
            // printf("Bstig Deser setting key %u, value %u \n",k->i.value,v->data->i.value);
         }
      }
      // printf("Size after bstigs deser %d\n",pos );
   }
   /* Deserialize and set blob entires */
   uint32_t bsize;
   pos = buzzmsg_deserialize_u32(&bsize,a,pos);
   for(int i = 0; i < bsize; ++i){
      uint16_t id;
      uint32_t k_size;
      pos = buzzmsg_deserialize_u32(&k_size,a,pos);
      const buzzdict_t* s = NULL;
      if(k_size!=0){
         pos = buzzmsg_deserialize_u16(&id,a,pos);
         s = buzzdict_get(vm->blobs, &id, buzzdict_t);
      }
      for(int j = 0; j < k_size; ++j){
         uint16_t k;
         uint32_t hash, size,locations_size;
         uint8_t priority;
         pos = buzzmsg_deserialize_u16(&k,a,pos);
         pos = buzzmsg_deserialize_u32(&hash,a,pos);
         pos = buzzmsg_deserialize_u32(&size,a,pos);
         pos = buzzmsg_deserialize_u8(&priority,a,pos);
         pos = buzzmsg_deserialize_u32(&locations_size,a,pos);
         buzzblob_elem_t v_blob = buzzchunk_slot_new(hash,size);
         v_blob->priority = priority;
         // printf("blob deserialization key %u hash : %u size : %u priority %u location size %u \n",
               // k,hash,size,priority,locations_size);
         for(int k=0; k<locations_size ; k++){
            uint16_t locrid, locavilablespace;
            pos = buzzmsg_deserialize_u16(&locrid,a,pos);
            pos = buzzmsg_deserialize_u16(&locavilablespace,a,pos);
            buzzblob_location_t locationelem = (buzzblob_location_t)malloc(sizeof(struct buzzblob_bidder_s));
            locationelem->rid = locrid;
            locationelem->availablespace = locavilablespace;
            buzzdarray_push(v_blob->locations, &(locationelem) );
            // printf("\t loc : rid %u , avilable %u \n",locrid, locavilablespace);
         }
         if(s){
            const buzzblob_elem_t* l = buzzdict_get(*s, &(k), buzzblob_elem_t);
            if(l){
               if( buzzdarray_size((*l)->available_list) > 0){
                  fprintf(stderr, 
                          "[WARNING] [ROBOT %u] Buzz blob deserialization of blobs, removing non null blob available_list to set new blob\n",
                          vm->robot);   
               }
            } 
            buzzdict_set(*s, &k, &v_blob);
            // printf("Bstig Deser setting key %u\n",k);
         }
         else{
            buzzdict_destroy( &(v_blob->data) );
            buzzdarray_destroy( &(v_blob->available_list) );
            buzzdarray_destroy( &(v_blob->locations) );
            free(v_blob);
         }
      }
      // printf("Size after bstigs deser %d\n",pos );
   }
   return pos;
}

/****************************************/
/****************************************/