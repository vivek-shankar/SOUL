#include "buzzvm.h"
#include "buzzvstig.h"
#include "buzzbstig.h"
#include "buzzswarm.h"
#include "buzzmath.h"
#include "buzzio.h"
#include "buzzstring.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>

/****************************************/
/****************************************/

const char *buzzvm_state_desc[] = { "no code", "ready", "done", "error", "stopped" };

const char *buzzvm_error_desc[] = { "none", "unknown instruction", "stack error", "wrong number of local variables", "pc out of range", "function id out of range", "type mismatch", "unknown string id", "unknown swarm id" };

const char *buzzvm_instr_desc[] = {"nop", "done", "pushnil", "dup", "pop", "ret0", "ret1", "add", "sub", "mul", "div", "mod", "pow", "unm", "and", "or", "not", "eq", "neq", "gt", "gte", "lt", "lte", "gload", "gstore", "pusht", "tput", "tget", "callc", "calls", "pushf", "pushi", "pushs", "pushcn", "pushcc", "pushl", "lload", "lstore", "jump", "jumpz", "jumpnz"};

static uint16_t SWARM_BROADCAST_PERIOD = 10;

/****************************************/
/****************************************/

void buzzvm_dump(buzzvm_t vm) {
   int64_t i, j;
   fprintf(stderr, "============================================================\n");
   fprintf(stderr, "state: %d\terror: %d\n", vm->state, vm->error);
   fprintf(stderr, "code size: %u\tpc: %d\n", vm->bcode_size, vm->pc);
   fprintf(stderr, "stacks: %" PRId64 "\tcur elem: %" PRId64 " (size %" PRId64 ")\n", buzzdarray_size(vm->stacks), buzzvm_stack_top(vm), buzzvm_stack_top(vm));
   for(i = buzzdarray_size(vm->stacks)-1; i >= 0 ; --i) {
      fprintf(stderr, "===== stack: %" PRId64 " =====\n", i);
      for(j = buzzdarray_size(buzzdarray_get(vm->stacks, i, buzzdarray_t)) - 1; j >= 0; --j) {
         fprintf(stderr, "\t%" PRId64 "\t", j);
         buzzobj_t o = buzzdarray_get(buzzdarray_get(vm->stacks, i, buzzdarray_t), j, buzzobj_t);
         switch(o->o.type) {
            case BUZZTYPE_NIL:
               fprintf(stderr, "[nil]\n");
               break;
            case BUZZTYPE_INT:
               fprintf(stderr, "[int] %d\n", o->i.value);
               break;
            case BUZZTYPE_FLOAT:
               fprintf(stderr, "[float] %f\n", o->f.value);
               break;
            case BUZZTYPE_TABLE:
               fprintf(stderr, "[table] %d elements\n", buzzdict_size(o->t.value));
               break;
            case BUZZTYPE_CLOSURE:
               if(o->c.value.isnative) {
                  fprintf(stderr, "[n-closure] %d\n", o->c.value.ref);
               }
               else {
                  fprintf(stderr, "[c-closure] %d\n", o->c.value.ref);
               }
               break;
            case BUZZTYPE_STRING:
               fprintf(stderr, "[string] %d:'%s'\n", o->s.value.sid, o->s.value.str);
               break;
            default:
               fprintf(stderr, "[TODO] type = %d\n", o->o.type);
         }
      }
   }
   fprintf(stderr, "============================================================\n\n");
}

/****************************************/
/****************************************/

const char* buzzvm_strerror(buzzvm_t vm) {
   static const char* noerr = "No error";
   return vm->state == BUZZVM_STATE_ERROR ?
      vm->errormsg
      :
      noerr;
}

/****************************************/
/****************************************/

#define BUZZVM_STACKS_INIT_CAPACITY  20
#define BUZZVM_STACK_INIT_CAPACITY   20
#define BUZZVM_LSYMTS_INIT_CAPACITY  20
#define BUZZVM_SYMS_INIT_CAPACITY    20
#define BUZZVM_STRINGS_INIT_CAPACITY 20

/****************************************/
/****************************************/

buzzvm_lsyms_t buzzvm_lsyms_new(uint8_t isswarm,
                                buzzdarray_t syms) {
   buzzvm_lsyms_t s = (buzzvm_lsyms_t)malloc(sizeof(struct buzzvm_lsyms_s));
   s->isswarm = isswarm;
   s->syms = syms;
   return s;
}

void buzzvm_lsyms_destroy(uint32_t pos,
                          void* data,
                          void* params) {
   buzzvm_lsyms_t s = *(buzzvm_lsyms_t*)data;
   buzzdarray_destroy(&(s->syms));
   free(s);
}

/****************************************/
/****************************************/

void buzzvm_vstig_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   buzzvstig_destroy((buzzvstig_t*)data);
   free(data);
}

/****************************************/
/****************************************/

void buzzvm_bstig_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   buzzbstig_destroy((buzzbstig_t*)data);
   free(data);
}

/****************************************/
/****************************************/

void buzzvm_blobs_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   buzzdict_destroy((buzzdict_t*)data);
   free(data);
}


/****************************************/
/****************************************/

void buzzvm_neighbors_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   buzzneighbour_chunk_t n_struct = *(buzzneighbour_chunk_t*) data; 
   buzzdict_destroy(&(n_struct->chunks_on));
   free(n_struct);
   free(data);
}

/****************************************/
/****************************************/

void buzzvm_cmonitor_chunks_destroy(uint32_t pos, void* data, void* param) {
   buzzchunk_reloc_elem_t d = *(buzzchunk_reloc_elem_t*) data;
   buzzdarray_destroy(&(d->checkednids));
   free(d);
}

/****************************************/
/****************************************/

void buzzvm_nt_chunks_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   buzzdarray_destroy((buzzdarray_t*) data);
   free(data);
}

/****************************************/
/****************************************/

void buzzvm_id_nt_chunks_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   buzzdict_destroy((buzzdict_t*) data);
   free(data);
}

/****************************************/
/****************************************/

// void buzzvm_cmonitor_reloc_elem_destroy(const void* key, void* data, void* params) {
//    free((void*)key);
//    buzzchunk_reloc_elem_t c = *(buzzchunk_reloc_elem_t*) data;
//    buzzdarray_destroy(&(c->neigh));
//    free(c);
//    free(data);
// }

int buzzvm_cmonitor_reloc_elem_key_cmp(const void* a, const void* b) {
   buzzchunk_reloc_elem_t c = *(buzzchunk_reloc_elem_t*) a;
   buzzchunk_reloc_elem_t d = *(buzzchunk_reloc_elem_t*) b;
   if(c->id == d->id && c->key == d->key && c->cid == d->cid)
      return 0;
   else return -1;
}

int buzzvm_cmonitor_blobrequest_requester_key_cmp(const void* a, const void* b) {
   buzzchunk_reloc_elem_t c = *(buzzchunk_reloc_elem_t*) a;
   buzzchunk_reloc_elem_t d = *(buzzchunk_reloc_elem_t*) b;
   if(c->id == d->id && c->key == d->key && c->cid == d->cid && c->bidsize == d->bidsize)
      return 0;
   else return -1;
}

int buzzvm_cmonitor_blobrequest_key_cmp(const void* a, const void* b) {
   buzzchunk_reloc_elem_t c = *(buzzchunk_reloc_elem_t*) a;
   buzzchunk_reloc_elem_t d = *(buzzchunk_reloc_elem_t*) b;
   if(c->id == d->id && c->key == d->key && c->bidsize == d->bidsize)
      return 0;
   else return -1;
}

/****************************************/
/****************************************/

void buzzvm_outmsg_destroy(uint32_t pos, void* data, void* param) {
   fprintf(stderr, "[TODO] %s:%d\n", __FILE__, __LINE__);
}

void buzzvm_process_inmsgs(buzzvm_t vm) {
   /* Go through the messages */
   while(!buzzinmsg_queue_isempty(vm->inmsgs)) {
      /* Make sure the VM is in the right state */
      if(vm->state != BUZZVM_STATE_READY) return;
      /* Extract the message data */
      uint16_t rid;
      buzzmsg_payload_t msg;
      buzzinmsg_queue_extract(vm, &rid, &msg);
      /* Mark the neighbor active */
      /* Fetch the neighbor dict */
      const buzzneighbour_chunk_t* n = 
         buzzdict_get(vm->active_neighbors, &rid, buzzneighbour_chunk_t);
         buzzneighbour_chunk_t nt = NULL;
      if(n){ /* Neighbor exsist reset time to forget */
         nt = *n;
         (*n)->timetoforget = TIME_TO_FORGET_NEIGHBOUR;
      }
      else{ /* New neighbor, create a new struct for this neighbor */
         /* reset adapt timer */
         //vm->nchange = TIME_TO_ADAPT_NEIGHBOUR_CHANGE;
         nt = (buzzneighbour_chunk_t)malloc(sizeof(struct buzzneighbour_chunk_s));
         nt->timetoforget = TIME_TO_FORGET_NEIGHBOUR;
         nt->chunks_on = buzzdict_new(10,
                                sizeof(uint16_t),
                                sizeof(buzzdict_t),
                                buzzdict_uint16keyhash,
                                buzzdict_uint16keycmp,
                                buzzvm_id_nt_chunks_destroy);
          
         buzzdict_set(vm->active_neighbors,&rid,&nt);
      }
      /* Dispatch the message wrt its type in msg->payload[0] */
      switch(buzzmsg_payload_get(msg, 0)) {
         case BUZZMSG_BROADCAST: {
            /* Deserialize the topic */
            buzzobj_t topic;
            int64_t pos = buzzobj_deserialize(&topic, msg, 1, vm);
            /* Make sure there's a listener to call */
            const buzzobj_t* l = buzzdict_get(vm->listeners, &topic->s.value.sid, buzzobj_t);
            if(!l) {
               /* No listener, ignore message */
               break;
            }
            /* Deserialize value */
            buzzobj_t value;
            pos = buzzobj_deserialize(&value, msg, pos, vm);
            /* Make an object for the robot id */
            buzzobj_t rido = buzzheap_newobj(vm, BUZZTYPE_INT);
            rido->i.value = rid;
            /* Call listener */
            buzzvm_push(vm, *l);
            buzzvm_push(vm, topic);
            buzzvm_push(vm, value);
            buzzvm_push(vm, rido);
            buzzvm_closure_call(vm, 3);
            break;
         }
         case BUZZMSG_VSTIG_PUT: {
            /* Deserialize the vstig id */
            uint16_t id;
            int64_t pos = buzzmsg_deserialize_u16(&id, msg, 1);
            if(pos < 0) {
               fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_VSTIG_PUT message received\n", vm->robot);
               break;
            }
            /* Look for virtual stigmergy */
            const buzzvstig_t* vs = buzzdict_get(vm->vstigs, &id, buzzvstig_t);
            if(!vs) break;
            /* Virtual stigmergy found */
            /* Deserialize key and value from msg */
            buzzobj_t k;          // key
            buzzvstig_elem_t v =  // value
               (buzzvstig_elem_t)malloc(sizeof(struct buzzvstig_elem_s));
            if(buzzvstig_elem_deserialize(&k, &v, msg, pos, vm) < 0) {
               fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_VSTIG_PUT message received\n", vm->robot);
               free(v);
               break;
            }
            /* Deserialization successful */
            /* Fetch local vstig element */
            const buzzvstig_elem_t* l = buzzvstig_fetch(*vs, &k);
            if((!l)                             || /* Element not found */
               ((*l)->timestamp < v->timestamp)) { /* Local element is older */
               /* Local element must be updated */
               /* Store element */
               buzzvstig_store(*vs, &k, &v);
               buzzoutmsg_queue_append_vstig(vm, BUZZMSG_VSTIG_PUT, id, k, v);
            }
            else if(((*l)->timestamp == v->timestamp) && /* Same timestamp */
                    ((*l)->robot != v->robot)) {         /* Different robot */
               /* Conflict! */
               /* Call conflict manager */
               buzzvstig_elem_t c =
                  buzzvstig_onconflict_call(vm, *vs, k, *l, v);
               if(!c) {
                  fprintf(stderr, "[WARNING] [ROBOT %u] Error resolving PUT conflict\n", vm->robot);
                  break;
               }
               /* Get rid of useless vstig element */
               free(v);
               /* Did this robot lose the conflict? */
               if((c->robot != vm->robot) &&
                  ((*l)->robot == vm->robot)) {
                  /* Yes */
                  /* Save current local entry */
                  buzzvstig_elem_t ol = buzzvstig_elem_clone(vm, *l);
                  /* Store winning value */
                  buzzvstig_store(*vs, &k, &c);
                  /* Call conflict lost manager */
                  buzzvstig_onconflictlost_call(vm, *vs, k, ol);
               }
               else {
                  /* This robot did not lose the conflict */
                  /* Just propagate the PUT message */
                  buzzvstig_store(*vs, &k, &c);
               }
               buzzoutmsg_queue_append_vstig(vm, BUZZMSG_VSTIG_PUT, id, k, c);
            }
            else {
               /* Remote element is older, ignore it */
               /* Get rid of useless vstig element */
               free(v);
            }
            break;
         }
         case BUZZMSG_VSTIG_QUERY: {
            /* Deserialize the vstig id */
            uint16_t id;
            int64_t pos = buzzmsg_deserialize_u16(&id, msg, 1);
            if(pos < 0) {
               fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_VSTIG_QUERY message received (1)\n", vm->robot);
               break;
            }
            /* Deserialize key and value from msg */
            buzzobj_t k;         // key
            buzzvstig_elem_t v = // value
               (buzzvstig_elem_t)malloc(sizeof(struct buzzvstig_elem_s));
            if(buzzvstig_elem_deserialize(&k, &v, msg, pos, vm) < 0) {
               fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_VSTIG_QUERY message received (2)\n", vm->robot);
               free(v);
               break;
            }
            /* Look for virtual stigmergy */
            const buzzvstig_t* vs = buzzdict_get(vm->vstigs, &id, buzzvstig_t);
            if(!vs) {
               /* Virtual stigmergy not found, simply propagate the message */
               buzzoutmsg_queue_append_vstig(vm, BUZZMSG_VSTIG_QUERY, id, k, v);
               free(v);
               break;
            }
            /* Virtual stigmergy found */
            /* Fetch local vstig element */
            const buzzvstig_elem_t* l = buzzvstig_fetch(*vs, &k);
            if(!l) {
               /* Element not found */
               if(v->data->o.type == BUZZTYPE_NIL) {
                  /* This robot knows nothing about the query, just propagate it */
                  buzzoutmsg_queue_append_vstig(vm, BUZZMSG_VSTIG_QUERY, id, k, v);
                  free(v);
               }
               else {
                  /* Store element and propagate PUT message */
                  buzzvstig_store(*vs, &k, &v);
                  buzzoutmsg_queue_append_vstig(vm, BUZZMSG_VSTIG_PUT, id, k, v);
               }
               break;
            }
            /* Element found */
            if((*l)->timestamp < v->timestamp) {
               /* Local element is older */
               /* Store element */
               buzzvstig_store(*vs, &k, &v);
               buzzoutmsg_queue_append_vstig(vm, BUZZMSG_VSTIG_PUT, id, k, v);
            }
            else if((*l)->timestamp > v->timestamp) {
               /* Local element is newer */
               /* Append a PUT message to the out message queue */
               buzzoutmsg_queue_append_vstig(vm, BUZZMSG_VSTIG_PUT, id, k, *l);
               free(v);
            }
            else if(((*l)->timestamp == v->timestamp) && /* Same timestamp */
                    ((*l)->robot != v->robot)) {         /* Different robot */
               /* Conflict! */
               /* Call conflict manager */
               buzzvstig_elem_t c =
                  buzzvstig_onconflict_call(vm, *vs, k, *l, v);
               free(v);
               /* Make sure conflict manager returned with an element to process */
               if(!c) break;
               /* Did this robot lose the conflict? */
               if((c->robot != vm->robot) &&
                  ((*l)->robot == vm->robot)) {
                  /* Yes */
                  /* Save current local entry */
                  buzzvstig_elem_t ol = buzzvstig_elem_clone(vm, *l);
                  /* Store winning value */
                  buzzvstig_store(*vs, &k, &c);
                  /* Call conflict lost manager */
                  buzzvstig_onconflictlost_call(vm, *vs, k, ol);
               }
               else {
                  /* This robot did not lose the conflict */
                  /* Just propagate the PUT message */
                  buzzvstig_store(*vs, &k, &c);
               }
               buzzoutmsg_queue_append_vstig(vm, BUZZMSG_VSTIG_PUT, id, k, c);
            }
            else {
               /* Remote element is same as local, ignore it */
               /* Get rid of useless vstig element */
               free(v);
            }
            break;
         }
         case BUZZMSG_BSTIG_PUT: {
            /* Determine whether the entry is a blob */
            uint8_t blob_entry;
            int64_t pos = buzzmsg_deserialize_u8(&blob_entry, msg, 1);
            if(!blob_entry){
               /* Deserialize the bstig id */
               uint16_t id;
               pos = buzzmsg_deserialize_u16(&id, msg, pos);
               /* Look for virtual stigmergy */
               const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
               if(!vs) break;
               /* Virtual stigmergy found */
               /* Deserialize key and value from msg */
               buzzobj_t k;          // key
               buzzbstig_elem_t v =  // value
                  (buzzbstig_elem_t)malloc(sizeof(struct buzzbstig_elem_s));
               pos = buzzbstig_elem_deserialize(&k, &v, msg, pos, vm);

               /*if(id==1){
                  printf(" [ RID %u]Got a chunk stig put message key %u \n", vm->robot,k->i.value);
               }*/

               if(pos < 0) {
                  fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_PUT message received\n", vm->robot);
                  free(v);
                  break;
               }
               /* Deserialization successful */
               /* Deserialize the received bstig size */
               //fprintf(stderr, "[DEBUG] [PUT, RID: %u, key: %u , BS ID: %u, recv Size: %u, cur Size: %u]\n", vm->robot, (uint16_t)k->i.value, id, size,(uint16_t)buzzdict_size((*vs)->data));
               /* Fetch local bstig element */
               const buzzbstig_elem_t* l = buzzbstig_fetch(*vs, &k);
               if((!l)                             || /* Element not found */
                  ((*l)->timestamp < v->timestamp)) { /* Local element is older */
                  /* Local element must be updated */
                  /* Store element */
                  // if(id==1){
                  //    printf(" [ RID %u]put a chunk stig put message key %u \n", vm->robot,k->i.value);
                  // }
                  buzzbstig_store(*vs, &k, &v);
                  buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v, 0);
               }
               else if(((*l)->timestamp == v->timestamp) && /* Same timestamp */
                       ((*l)->robot != v->robot)) {         /* Different robot */
                  /* Conflict! */
                  /* Call conflict manager */
                  buzzbstig_elem_t c =
                     buzzbstig_onconflict_call(vm, *vs, k, *l, v);
                  if(!c) {
                     fprintf(stderr, "[WARNING] [ROBOT %u] Error resolving PUT conflict\n", vm->robot);
                     break;
                  }
                  /* Get rid of useless bstig element */
                  free(v);
                  /* Did this robot lose the conflict? */
                  if((c->robot != vm->robot) &&
                     ((*l)->robot == vm->robot)) {
                     /* Yes */
                     /* Save current local entry */
                     buzzbstig_elem_t ol = buzzbstig_elem_clone(vm, *l);
                     /* Store winning value */
                     buzzbstig_store(*vs, &k, &c);
                     /* Call conflict lost manager */
                     buzzbstig_onconflictlost_call(vm, *vs, k, ol);
                  }
                  else {
                     /* This robot did not lose the conflict */
                     /* Just propagate the PUT message */
                     buzzbstig_store(*vs, &k, &c);
                  }
                  buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, c, 0);
               }
               else {
                  /* Remote element is older, ignore it */
                  /* Get rid of useless bstig element */
                  free(v);
               }
               break;
            }
            else{
               //printf("[ RID: %u Bstig put msg received]\n", vm->robot);
               /* Entry is a blob */
               /* Deserialize the blob size and bstig id */
               uint16_t id;
               uint32_t blob_size;
               pos = buzzmsg_deserialize_u32(&blob_size, msg, pos);
               pos = buzzmsg_deserialize_u16(&id, msg, pos);
               /* Look for virtual stigmergy */
               const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
               if(!vs) break;
               /* Virtual stigmergy found */
               /* Deserialize key and value from msg */
               buzzobj_t k;          // key
               buzzbstig_elem_t v =  // value
                  (buzzbstig_elem_t)malloc(sizeof(struct buzzbstig_elem_s));
               pos = buzzbstig_elem_deserialize(&k, &v, msg, pos, vm);
               if(pos < 0) {
                  fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_PUT message received\n", vm->robot);
                  free(v);
                  break;
               }
               /* Deserialization successful */
               /* Deserialize the received bstig size */
               //fprintf(stderr, "[DEBUG] [PUT, RID: %u, key: %u , BS ID: %u, recv Size: %u, cur Size: %u]\n", vm->robot, (uint16_t)k->i.value, id, size,(uint16_t)buzzdict_size((*vs)->data));
               /* Fetch local bstig element */
               const buzzbstig_elem_t* l = buzzbstig_fetch(*vs, &k);
               if(!l)  {
                  /* Element not found */
                  if(v->data->o.type == BUZZTYPE_NIL) {
                     /* This robot knows nothing about the query, just propagate it */
                     //buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v, 1);
                     free(v);
                  }
                  else{
                     /* Local element must be updated */
                     /* Store element */
                     buzzbstig_store(*vs, &k, &v);
                     /* Look for blob holder */
                     const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
                     const buzzblob_elem_t* v_blob = NULL;
                     if(s){
                        /* Look for blob key in blob bstig slot*/
                        v_blob = buzzdict_get(*s, &k->i.value, buzzblob_elem_t);
                     }
                     if(!v_blob){
                        /* Create data holders to host the blob */
                        buzz_blob_slot_holders_new(vm, id, k->i.value, blob_size,v->data->i.value);
                         
                     }
                     /* Append a blob put message */
                     buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v,1);
                  }
                  
               }
               else if((*l)->timestamp < v->timestamp){ /* Local element is old */
                  if(v->data->o.type == BUZZTYPE_NIL){
                     /* Delete the existing element */
                     buzzbstig_remove(*vs, &k);
                     /* Append a PUT message to the out message queue with the received nil */
                     buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v, 1);
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

                     }
                     free(v); 
                  }
                  else{
                     /* Local element must be updated */
                     /* Store element */
                     buzzbstig_store(*vs, &k, &v);
                     /* Look for blob holder */
                     const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
                     const buzzblob_elem_t* v_blob = NULL;
                     if(s){
                        /* Look for blob key in blob bstig slot*/
                        v_blob = buzzdict_get(*s, &k->i.value, buzzblob_elem_t);
                     }
                     if(!v_blob){
                        /* Create data holders to host the blob */
                        buzz_blob_slot_holders_new(vm, id, k->i.value, blob_size,v->data->i.value);
                         
                     }
                     /* Append a blob put message */
                     buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v,1);
                  }
               }
               else if(((*l)->timestamp == v->timestamp) && /* Same timestamp */
                       ((*l)->robot != v->robot)) {         /* Different robot */
                  /* Conflict! */
                  /* Call conflict manager */
                  printf("[Bstig Unimplemented feature] versioning blob received\n");
                  buzzbstig_elem_t c =
                     buzzbstig_onconflict_call(vm, *vs, k, *l, v);
                  if(!c) {
                     fprintf(stderr, "[WARNING] [ROBOT %u] Error resolving PUT conflict\n", vm->robot);
                     break;
                  }
                  /* Get rid of useless bstig element */
                  free(v);
                  /* Did this robot lose the conflict? */
                  if((c->robot != vm->robot) &&
                     ((*l)->robot == vm->robot)) {
                     /* Yes */
                     /* Save current local entry */
                     buzzbstig_elem_t ol = buzzbstig_elem_clone(vm, *l);
                     /* Store winning value */
                     buzzbstig_store(*vs, &k, &c);
                     /* Call conflict lost manager */
                     buzzbstig_onconflictlost_call(vm, *vs, k, ol);
                  }
                  else {
                     /* This robot did not lose the conflict */
                     /* Just propagate the PUT message */
                     buzzbstig_store(*vs, &k, &c);
                  }
                  buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, c, 1);
               }
               else {
                  /* Remote element is older, ignore it */
                  /* Get rid of useless bstig element */
                  free(v);
               }
               break;
            }   
         }
         case BUZZMSG_BSTIG_QUERY: {
            /* Determine whether the entry is a blob */
            uint8_t blob_entry;
            int64_t pos = buzzmsg_deserialize_u8(&blob_entry, msg, 1);
            if(!blob_entry){
               /* Deserialize the bstig id */
               uint16_t id;
               pos = buzzmsg_deserialize_u16(&id, msg, pos);
               if(pos < 0) {
                  fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_QUERY message received (1)\n", vm->robot);
                  break;
               }
               /* Deserialize key and value from msg */
               buzzobj_t k;         // key
               buzzbstig_elem_t v = // value
                  (buzzbstig_elem_t)malloc(sizeof(struct buzzbstig_elem_s));
                pos = buzzbstig_elem_deserialize(&k, &v, msg, pos, vm);
               if(pos < 0) {
                  fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_QUERY message received (2)\n", vm->robot);
                  free(v);
                  break;
               }
               /*if(id==1){
                  printf(" [ RID %u]Got a chunk stig query message key %u \n", vm->robot,k->i.value);
               }*/
               /* Look for virtual stigmergy */
               const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   	         //fprintf(stderr, "[DEBUG] [GET, RID: %u, key: %u, BS ID: %u, recv Size: %u, cur size: %u]\n", 
   	         //		vm->robot, k->i.value, id, size, (uint16_t)buzzdict_size((*vs)->data));
               if(!vs) {
                  /* Virtual stigmergy not found, simply propagate the message */
                  buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_QUERY, id, k, v, 0);
                  free(v);
                  break;
               }
               /* Virtual stigmergy found */
               /* Fetch local bstig element */
               const buzzbstig_elem_t* l = buzzbstig_fetch(*vs, &k);
               if(!l) {
                  /* Element not found */
                  if(v->data->o.type == BUZZTYPE_NIL) {
                     /* This robot knows nothing about the query, just propagate it */
                     buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_QUERY, id, k, v, 0); 
                     free(v);
                  }
                  else {
                    /* if(id==1){
                        printf(" [ RID %u] put a chunk stig query message key %u \n", vm->robot,k->i.value);
                     }*/
                     /* Store element and propagate PUT message */
                     buzzbstig_store(*vs, &k, &v);
                     buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v, 0);
                  }
                  break;
               }
               /* Element found */
               if((*l)->timestamp < v->timestamp) {
                  /* Local element is older */
                  /* Store element */
                  buzzbstig_store(*vs, &k, &v);
                  /* Check whether this is a blob */
                  buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v, 0);
               }
               else if((*l)->timestamp > v->timestamp) {
                  /* Local element is newer */
                  /* Check whether this is a blob */
                   /* Look for blob holder */
                     const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
                     const buzzblob_elem_t* v_blob = NULL;
                     if(s){
                        /* Look for blob key in blob bstig slot*/
                        v_blob = buzzdict_get(*s, &k->i.value, buzzblob_elem_t);
                     }
                     if(v_blob){
                        buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, *l, 1);
                     }
                     else{
                        buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k,*l, 0);
                     }
                  free(v);
               }
               else if(((*l)->timestamp == v->timestamp) && /* Same timestamp */
                       ((*l)->robot != v->robot)) {         /* Different robot */
                  /* Conflict! */
                  /* Call conflict manager */
                  buzzbstig_elem_t c =
                     buzzbstig_onconflict_call(vm, *vs, k, *l, v);
                  free(v);
                  /* Make sure conflict manager returned with an element to process */
                  if(!c) break;
                  /* Did this robot lose the conflict? */
                  if((c->robot != vm->robot) &&
                     ((*l)->robot == vm->robot)) {
                     /* Yes */
                     /* Save current local entry */
                     buzzbstig_elem_t ol = buzzbstig_elem_clone(vm, *l);
                     /* Store winning value */
                     buzzbstig_store(*vs, &k, &c);
                     /* Call conflict lost manager */
                     buzzbstig_onconflictlost_call(vm, *vs, k, ol);
                  }
                  else {
                     /* This robot did not lose the conflict */
                     /* Just propagate the PUT message */
                     buzzbstig_store(*vs, &k, &c);
                  }
                  buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, c, 0);
               }
               else {
                  /* Remote element is same as local, ignore it */
                  /* Get rid of useless bstig element */
                  free(v);
               }
               break;
            }
            else{
               /* Deserialize the bstig id */
               uint16_t id;
               uint32_t blob_size;
               pos = buzzmsg_deserialize_u32(&blob_size, msg, pos);
               pos = buzzmsg_deserialize_u16(&id, msg, pos);
               if(pos < 0) {
                  fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_QUERY message received (1)\n", vm->robot);
                  break;
               }
               /* Deserialize key and value from msg */
               buzzobj_t k;         // key
               buzzbstig_elem_t v = // value
                  (buzzbstig_elem_t)malloc(sizeof(struct buzzbstig_elem_s));
                pos = buzzbstig_elem_deserialize(&k, &v, msg, pos, vm);
               if(pos < 0) {
                  fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_QUERY message received (2)\n", vm->robot);
                  free(v);
                  break;
               }
               /* Look for virtual stigmergy */
               const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
               //fprintf(stderr, "[DEBUG] [GET, RID: %u, key: %u, BS ID: %u, recv Size: %u, cur size: %u]\n", 
               //    vm->robot, k->i.value, id, size, (uint16_t)buzzdict_size((*vs)->data));
               if(!vs) {
                  /* Virtual stigmergy not found, simply propagate the message */
                  buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_QUERY, id, k, v, 1);
                  free(v);
                  break;
               }
               /* Virtual stigmergy found */
               /* Fetch local bstig element */
               const buzzbstig_elem_t* l = buzzbstig_fetch(*vs, &k);
               if(!l) {
                  if(v->data->o.type == BUZZTYPE_NIL) {
                     /* This robot knows nothing about the query, just propagate it */
                     //buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_QUERY, id, k, v, 1);
                     free(v);
                  }
                  else {
                     /* Store element and propagate PUT message */
                     buzzbstig_store(*vs, &k, &v);
                     /* Look for blob holder */
                     const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
                     const buzzblob_elem_t* v_blob = NULL;
                     if(s){
                        /* Look for blob key in blob bstig slot*/
                        v_blob = buzzdict_get(*s, &k->i.value, buzzblob_elem_t);
                     }
                     if(!v_blob){
                        /* Create data holders to host the blob */
                        buzz_blob_slot_holders_new(vm, id, k->i.value, blob_size,v->data->i.value);
                     }
                     buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v, 1);
                  }
                  break;
               }
               /* Element found */
               if((*l)->timestamp < v->timestamp) {
                  /* Local element is older */
                  if(v->data->o.type == BUZZTYPE_NIL){
                     /* Delete the existing element */
                     buzzbstig_remove(*vs, &k);
                     /* Append a PUT message to the out message queue with the received nil */
                     buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v, 1);
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

                     }
                     free(v); 
                  }
                  else{
                     /* Store element */
                     buzzbstig_store(*vs, &k, &v);
                     /* Look for blob holder */
                     const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
                     const buzzblob_elem_t* v_blob = NULL;
                     if(s){
                        /* Look for blob key in blob bstig slot*/
                        v_blob = buzzdict_get(*s, &k->i.value, buzzblob_elem_t);
                     }
                     if(!v_blob){
                        /* Create data holders to host the blob */
                        buzz_blob_slot_holders_new(vm, id, k->i.value, blob_size,v->data->i.value);
                     }
                     buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v, 1);
                  }
               }
               else if((*l)->timestamp > v->timestamp) {
                  /* Local element is newer */
                  /* Append a PUT message to the out message queue */
                  buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, *l, 1);
                  free(v);
               }
               else if(((*l)->timestamp == v->timestamp) && /* Same timestamp */
                       ((*l)->robot != v->robot)) {         /* Different robot */
                  /* Conflict! */
                  /* Call conflict manager */
                  buzzbstig_elem_t c =
                     buzzbstig_onconflict_call(vm, *vs, k, *l, v);
                  free(v);
                  /* Make sure conflict manager returned with an element to process */
                  if(!c) break;
                  /* Did this robot lose the conflict? */
                  if((c->robot != vm->robot) &&
                     ((*l)->robot == vm->robot)) {
                     /* Yes */
                     /* Save current local entry */
                     buzzbstig_elem_t ol = buzzbstig_elem_clone(vm, *l);
                     /* Store winning value */
                     buzzbstig_store(*vs, &k, &c);
                     /* Call conflict lost manager */
                     buzzbstig_onconflictlost_call(vm, *vs, k, ol);
                  }
                  else {
                     /* This robot did not lose the conflict */
                     /* Just propagate the PUT message */
                     buzzbstig_store(*vs, &k, &c);
                  }
                  buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, c, 1);
               }
               else {
                  /* Remote element is same as local, ignore it */
                  /* Get rid of useless bstig element */
                  free(v);
               }
               break;
            }
         }
         case BUZZMSG_SWARM_LIST: {
            /* Deserialize number of swarm ids */
            uint16_t nsids;
            int64_t pos = buzzmsg_deserialize_u16(&nsids, msg, 1);
            if(pos < 0) {
               fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_SWARM_LIST message received\n", vm->robot);
               break;
            }
            if(nsids < 1) break;
            /* Deserialize swarm ids */
            buzzdarray_t sids = buzzdarray_new(nsids, sizeof(uint16_t), NULL);
            uint16_t i;
            for(i = 0; i < nsids; ++i) {
               pos = buzzmsg_deserialize_u16(buzzdarray_makeslot(sids, i), msg, pos);
               if(pos < 0) {
                  fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_SWARM_LIST message received\n", vm->robot);
                  break;
               }
            }
            /* Update the information */
            buzzswarm_members_refresh(vm->swarmmembers, rid, sids);
            break;
         }
         case BUZZMSG_SWARM_JOIN: {
            /* Deserialize swarm id */
            uint16_t sid;
            int64_t pos = buzzmsg_deserialize_u16(&sid, msg, 1);
            if(pos < 0) {
               fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_SWARM_JOIN message received\n", vm->robot);
               break;
            }
            /* Update the information */
            buzzswarm_members_join(vm->swarmmembers, rid, sid);
            break;
         }
         case BUZZMSG_SWARM_LEAVE: {
            /* Deserialize swarm id */
            uint16_t sid;
            int64_t pos = buzzmsg_deserialize_u16(&sid, msg, 1);
            if(pos < 0) {
               fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_SWARM_LEAVE message received\n", vm->robot);
               break;
            }
            /* Update the information */
            buzzswarm_members_leave(vm->swarmmembers, rid, sid);
            break;
         }
         case BUZZMSG_BSTIG_CHUNK_QUERY: {
            //printf("[ RID: %u Bstig put msg received]\n", vm->robot);
            /* Entry is a blob */
            /* Deserialize the blob size and bstig id */
            uint16_t id;
            uint32_t blob_size;
            int64_t pos = buzzmsg_deserialize_u32(&blob_size, msg, 1);
            pos = buzzmsg_deserialize_u16(&id, msg, pos);
            /* Look for virtual stigmergy */
            const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
            if(!vs) break;
            /* Virtual stigmergy found */
            /* Deserialize key and value from msg */
            buzzobj_t k;          // key
            buzzbstig_elem_t v =  // value
               (buzzbstig_elem_t)malloc(sizeof(struct buzzbstig_elem_s));
            pos = buzzbstig_elem_deserialize(&k, &v, msg, pos, vm);
            if(pos < 0) {
               fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_CHUNK_QUERY message received\n", vm->robot);
               free(v);
               break;
            }
            //fprintf(stderr, "[DEBUG] [PUT, RID: %u, key: %u , BS ID: %u, recv Size: %u, cur Size: %u]\n", vm->robot, (uint16_t)k->i.value, id, size,(uint16_t)buzzdict_size((*vs)->data));
            /* Fetch local bstig element */
            const buzzbstig_elem_t* l = buzzbstig_fetch(*vs, &k);
            if((!l)){                              /* Element not found */
               /* Local element must be updated */
               buzzbstig_elem_t val_copy = buzzbstig_elem_clone(vm, v);
               /* Store element */
               buzzbstig_store(*vs, &k, &val_copy);
               /* Create data holders to host the blob */
               buzz_blob_slot_holders_new(vm, id, k->i.value, blob_size,v->data->i.value);
               /* Append a blob put message */
               buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v,1);
            }
            else if(((*l)->timestamp == v->timestamp) && /* Same timestamp */
                    ((*l)->robot != v->robot)) {         /* Different robot */
               /* Conflict! */
               /* Call conflict manager */
               printf("[Bstig Unimplemented feature] versioning blob msg received \
                  sametime stamp different robot \n");
               
            }
            else if (v->timestamp < (*l)->timestamp ){
               /* Remote element is older, ignore it TODO */
               printf("[Bstig Unimplemented feature] versioning blob msg received \
                  remote older \n");
               /* Get rid of useless bstig element */
               // free(v);
            }
            /* If you reach here, blob slots should exsist */
            /* Deserialize the chunk */
            uint16_t chunk_index;
            pos = buzzmsg_deserialize_u16(&chunk_index, msg, pos);
            if(pos < 0) {
               fprintf(stderr,
                "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_CHUNK_QUERY message received at chunk index Deserialize\n",
                 vm->robot);
               free(v);
               break;
            }
            /* Check blob send it if present */
            const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
            if(s){
               /* Look for blob key in blob bstig slot*/
               const buzzblob_elem_t* v_blob = buzzdict_get(*s, &k->i.value, buzzblob_elem_t);
               if(v_blob){         
                  const buzzblob_chunk_t* cdata = 
                                 buzzdict_get((*v_blob)->data, &chunk_index, buzzblob_chunk_t);
                  if(cdata){ /* Chunk found forward */
                        
                     buzzoutmsg_queue_append_chunk(vm,
                                BUZZMSG_BSTIG_CHUNK_PUT,
                                id,
                                k,
                                v,
                                (*v_blob)->size,
                                chunk_index,
                                *cdata,
                                BROADCAST_MESSAGE_CONSTANT);
                  }
                  else{ /* Chunk not found propagate query */
                        /* Create a empty chunk */
                        char chunk[1];
                        *chunk=0;
                        struct buzzblob_chunk_s nullchunk = { .chunk=chunk, .hash=0};
                        /* chunk not found, add a query message */
                        buzzoutmsg_queue_append_chunk(vm,
                                                      BUZZMSG_BSTIG_CHUNK_QUERY,
                                                      id,
                                                      k,
                                                      v,
                                                      (*v_blob)->size,
                                                      chunk_index,
                                                      &nullchunk,
                                                      BROADCAST_MESSAGE_CONSTANT);
                  }
                 
               }

            }
            free(v);
            break; 
         }
         case BUZZMSG_BSTIG_CHUNK_PUT: {
            //printf("[ RID: %u Bstig put msg received]\n", vm->robot);
            /* Entry is a blob */
            /* Deserialize the blob size and bstig id */
           
            uint16_t id;
            uint32_t blob_size;
            int64_t pos = buzzmsg_deserialize_u32(&blob_size, msg, 1);
            pos = buzzmsg_deserialize_u16(&id, msg, pos);
            /* Look for virtual stigmergy */
            const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
            if(!vs) break;
            /* Virtual stigmergy found */

            /* Deserialize key and value from msg */
            buzzobj_t k;          // key
            buzzbstig_elem_t v =  // value
               (buzzbstig_elem_t)malloc(sizeof(struct buzzbstig_elem_s));
            pos = buzzbstig_elem_deserialize(&k, &v, msg, pos, vm);
            if(pos < 0) {
               fprintf(stderr, "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_CHUNK message received\n", vm->robot);
               free(v);
               break;
            }
            /* Fetch local bstig element */
            const buzzbstig_elem_t* l = buzzbstig_fetch(*vs, &k);

            if(!l){                              /* Element not found */
               /* Local element does not exsit */
               buzzbstig_elem_t val_copy = buzzbstig_elem_clone(vm, v);
                /* Store element */
               buzzbstig_store(*vs, &k, &val_copy);
               /* Look for blob holder */
               const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
               const buzzblob_elem_t* v_blob = NULL;
               if(s){
                  /* Look for blob key in blob bstig slot*/
                  v_blob = buzzdict_get(*s, &k->i.value, buzzblob_elem_t);
               }
               if(!v_blob){
                  /* Create data holders to host the blob */
                  buzz_blob_slot_holders_new(vm, id, k->i.value, blob_size,v->data->i.value);
                   
               }
               /* Append a blob put message */
               buzzoutmsg_queue_append_bstig(vm, BUZZMSG_BSTIG_PUT, id, k, v,1);
            }
            else if(((*l)->timestamp == v->timestamp) && /* Same timestamp */
                    ((*l)->robot != v->robot)) {         /* Different robot */
               /* Conflict! */
               /* Call conflict manager */
               printf("[Bstig Unimplemented feature] versioning blob msg received \
                  sametime stamp different robot \n");
               
            }
            else if (v->timestamp < (*l)->timestamp ){
               /* Remote element is older, ignore it TODO */
               printf("[Bstig Unimplemented feature] versioning blob msg received \
                  remote older \n");
               /* Get rid of useless bstig element */
               // free(v);
            }
            /* If you reach here, blob slots should exsist */
            /* Deserialize the chunk */
            uint16_t chunk_index;
            pos = buzzmsg_deserialize_u16(&chunk_index, msg, pos);
            buzzblob_chunk_t cdata =
                  (buzzblob_chunk_t)malloc(sizeof(struct buzzblob_chunk_s));
            pos = buzzbstig_chunk_deserialize(&(cdata->chunk), msg, pos);
            if(pos < 0) {
               fprintf(stderr,
                "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_CHUNK message received at chunk Deserialize , index: %u, sender %u \n",
                 vm->robot, chunk_index, rid);
               free(v);
               break;
            }

            /* Update neigh table */
            // if( !buzzdict_exists(nt->chunks_on, &id) ){
            //    buzzdict_t ntkslt = buzzdict_new(10,
            //                                       sizeof(uint16_t),
            //                                       sizeof(buzzdarray_t),
            //                                       buzzdict_uint16keyhash,
            //                                       buzzdict_uint16keycmp,
            //                                       buzzvm_nt_chunks_destroy);
            //    buzzdict_set(nt->chunks_on, &id, &ntkslt);
            // }
            // const buzzdict_t* ntkslt = buzzdict_get(nt->chunks_on, &id, buzzdict_t);
            // if(!buzzdict_exists(*ntkslt, &(k->i.value))){
            //    buzzdarray_t ntkarry = buzzdarray_new(10, 
            //                                        sizeof(uint16_t),
            //                                        NULL);
            //    buzzdict_set(*ntkslt, &(k->i.value), &ntkarry);
            // } 
            // const buzzdarray_t* ntkarry = buzzdict_get(*ntkslt, &(k->i.value), buzzdarray_t);
            // uint16_t index = buzzdarray_find(*ntkarry, buzzdict_uint16keycmp, &chunk_index);
            // if(index == buzzdarray_size(*ntkarry))
            //    buzzdarray_push(*ntkarry, &chunk_index);
            // /* If neighbour structure changes forward packets */
            // if(vm->nchange !=0) {
            //    /* Forward the blob */
            //    buzzoutmsg_queue_append_chunk(vm,
            //                           BUZZMSG_BSTIG_CHUNK_PUT,
            //                           id,
            //                           k,
            //                           v,
            //                           blob_size,
            //                           chunk_index,
            //                           cdata,
            //                           BROADCAST_MESSAGE_CONSTANT);
            // }
            if (buzzbstig_store_chunk(vm,id,k,v,blob_size,chunk_index,cdata)){
               free(v);
               break; /* Chunk was accepted and stored */
            }
            else { /* Chunk was not accepted and not stored */
               free(cdata->chunk);
               free(cdata);
               free(v);
               break;
            }  
         }
         case BUZZMSG_BSTIG_STATUS:{
            uint16_t id,key,requester;
            uint8_t status;
            int64_t pos = buzzmsg_deserialize_u16(&id, msg, 1);
            pos = buzzmsg_deserialize_u16(&key, msg, pos);
            pos = buzzmsg_deserialize_u8(&status, msg, pos);
            pos = buzzmsg_deserialize_u16(&requester, msg, pos);
            // printf("status msg received : id: %u key %u status %u\n",id, key, status );
            if(pos < 0) {
               fprintf(stderr,
                "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_STATUS message received from %u",
                 vm->robot, rid);
               break;
            }
            if(status == BUZZBLOB_SETGETTER ){
               /* Look for blob stigmergy */
               const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
               if(vs){
                  if((*vs)->getter != BUZZBLOB_GETTER){
                     (*vs)->getter = BUZZBLOB_GETTER_NEIGH;
                     struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZBLOB_GETTER_NEIGH};
                     buzzchunk_reloc_elem_t newelem = &cmpelem;
                     uint16_t cmonindex = buzzdarray_find(vm->cmonitor->getters,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                     if(cmonindex == buzzdarray_size(vm->cmonitor->getters)){
                        /* Add an elemnt in refersher to monitor and maintain when robots are moving */
                        buzzchunk_reloc_elem_t newelem =(buzzchunk_reloc_elem_t)malloc(sizeof(struct buzzchunk_reloc_elem_s));
                        newelem->id = id;
                        newelem->key = 0;
                        newelem->cid = BUZZBLOB_GETTER_NEIGH;
                        newelem->bidsize = 0; 
                        newelem->time_to_wait = TIME_TO_REFRESH_GETTER_STATE * 2;
                        newelem->time_to_destroy = 0;
                        newelem->checkednids = buzzdarray_new(1,sizeof(buzzblob_bidder_t),buzzbstig_blob_bidderelem_destroy);
                        buzzdarray_push(vm->cmonitor->getters,&newelem);
                     }
                     else{
                        const buzzchunk_reloc_elem_t celem = 
                                                         buzzdarray_get(vm->cmonitor->getters,cmonindex,buzzchunk_reloc_elem_t);
                        celem->time_to_wait = TIME_TO_REFRESH_GETTER_STATE;
                     }
                  }
               }
            }  
            else{
               /* look for bs */
               const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
               if(s){
                  /* Look for blob key in blob bstig slot*/
                  const buzzblob_elem_t* v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
                  if(v_blob){
                     if( (*v_blob)->relocstate == BUZZBLOB_HOST  && status == BUZZBLOB_SINK ){
                        uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
                        uint16_t avilable = 0; 
                        for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                           /* Get thelocation elements and calculate the space */
                           buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
                           avilable+=lowloc->availablespace;                  
                        }
                        if(avilable >= chunk_num){
                           /* I have the complete location, send the location to the setter */
                           for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                           buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);            
                           buzzoutmsg_queue_append_chunk_reloc(vm,
                                                              BUZZMSG_BSTIG_CHUNK_STATUS_QUERY,
                                                              id,
                                                              key,
                                                              lowloc->rid,
                                                              BROADCAST_MESSAGE_CONSTANT,
                                                              BUZZ_ADD_BLOB_LOCATION,
                                                              lowloc->availablespace);  
                           }
                        }
                        else{
                           (*v_blob)->relocstate = BUZZBLOB_HOST_FORWARDING;
                           buzzoutmsg_queue_append_blob_status(vm, BUZZMSG_BSTIG_STATUS, id,
                                                               key, status,requester);
                        
                        }
                     }
                     /* For the robots forwarding */
                     if((*v_blob)->relocstate == BUZZBLOB_OPEN && status == BUZZBLOB_SINK){
                        uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
                        uint16_t avilable = 0; 
                        for(int i=0; i< buzzdarray_size((*v_blob)->locations);i++){
                           /* Get the location elements and calculate the space */
                           buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
                           avilable+=lowloc->availablespace;                  
                        }
                        if(avilable >= chunk_num){
                           /* I have the complete location, send the location to the setter */
                           for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                           buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);            
                           buzzoutmsg_queue_append_chunk_reloc(vm,
                                                              BUZZMSG_BSTIG_CHUNK_STATUS_QUERY,
                                                              id,
                                                              key,
                                                              lowloc->rid,
                                                              BROADCAST_MESSAGE_CONSTANT,
                                                              BUZZ_ADD_BLOB_LOCATION,
                                                              lowloc->availablespace);  
                           }
                        }
                        else{
                           (*v_blob)->relocstate = BUZZBLOB_FORWARDING;
                           buzzoutmsg_queue_append_blob_status(vm, BUZZMSG_BSTIG_STATUS, id,
                                                            key, status,requester);
                        }
                     }

                     /* For the source, if you are the source check your bidding state */
                     if((*v_blob)->relocstate == BUZZBLOB_SOURCE){
                        struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .bidsize=requester};
                        buzzchunk_reloc_elem_t newelem = &cmpelem;
                        uint16_t cmonindex = buzzdarray_find(vm->cmonitor->blobrequest,buzzvm_cmonitor_blobrequest_key_cmp,&newelem);
                        /* Check for an exsisting request from the same robot */
                        if(cmonindex == buzzdarray_size(vm->cmonitor->blobrequest)){
                           /* New request */
                           uint16_t newbidindex = 0;
                           buzzchunk_reloc_elem_t newbidelem = NULL;
                           buzzchunk_reloc_elem_t alloctionbidelem = NULL;
                           buzzchunk_reloc_elem_t forceallocbidelem = NULL;
                           /* Did I already finsh my bidding allocation */
                           for(int i = 0; i< buzzdarray_size(vm->cmonitor->bidder); i++){
                              const buzzchunk_reloc_elem_t celem = 
                                                buzzdarray_get(vm->cmonitor->bidder,i,buzzchunk_reloc_elem_t);
                              if(celem->cid == BUZZBSTIG_BID_NEW){
                                 newbidelem = celem;
                                 newbidindex = i;
                              }
                              else if(celem->cid == BUZZCHUNK_BID_ALLOCATION){
                                 alloctionbidelem = celem;
                                 
                              }
                              else if(celem->cid == BUZZCHUNK_BID_FORCE_ALLOCATION){
                                 forceallocbidelem = celem;
                                 
                              }
                           }
                           /* Allocation done, don't interven this process  */
                           uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
                           uint16_t avilable = 0; 
                           for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                              /* Get thelocation elements and calculate the space */
                              buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
                              avilable+=lowloc->availablespace;                  
                           }
                           if(alloctionbidelem || forceallocbidelem || avilable >= chunk_num){
                              /* Is the blob under transport ? then append a request to locations */
                              if(avilable >= chunk_num){
                                 /* Send a message to all locations  asking for blob*/
                                 for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                                    /* Get the first location element of lowest priority blob */
                                    buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
                                    // printf("[ROBOT %u] Vm in msg Requested blob for requester : %u id: %u key %u avilable %u chunk_num %u location %u\n",
                                    //    vm->robot,requester,id, key,avilable, chunk_num, lowloc->rid);    
                                    buzzoutmsg_queue_append_blob_request(vm,
                                                                    BUZZMSG_BSTIG_BLOB_REQUEST,
                                                                    id,
                                                                    key,
                                                                    lowloc->rid,
                                                                    requester);   
                                 }
                                 /* Add an element indicating this request to avoid rerequests */
                                 buzzchunk_reloc_elem_t newelem =(buzzchunk_reloc_elem_t)malloc(sizeof(struct buzzchunk_reloc_elem_s));
                                 newelem->id = id;
                                 newelem->key = key;
                                 newelem->cid = BUZZBLOB_REQUESTED_LOCATIONS;
                                 newelem->bidsize = requester; 
                                 newelem->time_to_wait = TIME_TO_FORGET_BLOB_REQUEST;
                                 newelem->time_to_destroy = 0;
                                 newelem->checkednids = buzzdarray_new(1,sizeof(buzzblob_bidder_t),buzzbstig_blob_bidderelem_destroy);
                                 buzzdarray_push(vm->cmonitor->blobrequest,&newelem);
                              }
                              else{
                                 /* Allocation in progress, store the request and wait for the allocation to be done */
                                 buzzchunk_reloc_elem_t newelem =(buzzchunk_reloc_elem_t)malloc(sizeof(struct buzzchunk_reloc_elem_s));
                                 newelem->id = id;
                                 newelem->key = key;
                                 newelem->cid = BUZZBLOB_WAITING_TO_REQUEST_LOCATIONS;
                                 newelem->bidsize = requester; 
                                 newelem->time_to_wait = TIME_TO_REFRESH_GETTER_STATE;
                                 newelem->time_to_destroy = 0;
                                 newelem->checkednids = buzzdarray_new(1,sizeof(buzzblob_bidder_t),buzzbstig_blob_bidderelem_destroy);
                                 buzzdarray_push(vm->cmonitor->blobrequest,&newelem);
                              }
                           }
                           else if(newbidelem && newbidelem->time_to_wait < MAX_TIME_FOR_THE_BIDDER){
                              /* Bidding in process */
                              /* Find whether the state was already changed */
                              struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZBLOB_STATUS_CHANGE_DONE};
                              buzzchunk_reloc_elem_t newelem = &cmpelem;
                              uint16_t cmonindex = buzzdarray_find(vm->cmonitor->blobrequest,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                              /* Check for an exsisting request */
                              if(cmonindex == buzzdarray_size(vm->cmonitor->blobrequest)){
                                 /* Stop the bidding*/
                                 buzzdarray_remove(vm->cmonitor->bidder,newbidindex);                              
                                 /* Start broadcasting */
                                 /* Look for blob holder and create a new if none exsists */
                                 const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
                                 const buzzblob_elem_t* v_blob = NULL;
                                 /*Nothing to do, if bs id doesnot exsist*/
                                 if(s){
                                    /* Look for blob key in blob bstig slot*/
                                    v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
                                    if(v_blob){
                                       /* Create an element for key */
                                       buzzobj_t k = buzzobj_new(BUZZTYPE_INT);  // TODO : try to unify args and hence avoid creation of buzzobj
                                       k->i.value = key;
                                       /* Look for virtual stigmergy */
                                       const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
                                       /* Fetch the element */
                                       const buzzbstig_elem_t* l = buzzbstig_fetch(*vs, &k);
                                       // printf("allocating chunk : id : %u , key : %u \n",id,k->i.value );
                                       uint16_t blob_size = buzzdarray_size((*v_blob)->available_list);
                                       for(int i=0;i < blob_size;i++){

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
                                                                        BROADCAST_MESSAGE_CONSTANT);
                                          /* Delete the existing element */
                                          buzzbstig_remove((*v_blob), &cid); 
                                          /* Remove the last element from the avilable list */
                                          buzzdarray_pop((*v_blob)->available_list);
                                          /* Decrease the size in cmon */
                                          (vm->cmonitor->chunknum)--;
                                       }
                                    }
                                 }
                                 /* Add an element in the request monitor */
                                 buzzchunk_reloc_elem_t newelem =(buzzchunk_reloc_elem_t)malloc(sizeof(struct buzzchunk_reloc_elem_s));
                                 newelem->id = id;
                                 newelem->key = key;
                                 newelem->cid = BUZZBLOB_STATUS_CHANGE_DONE;
                                 newelem->bidsize = requester; 
                                 newelem->time_to_wait = TIME_TO_FORGET_BLOB_REQUEST;
                                 newelem->time_to_destroy = 0;
                                 newelem->checkednids = buzzdarray_new(1,sizeof(buzzblob_bidder_t),buzzbstig_blob_bidderelem_destroy);
                                 buzzdarray_push(vm->cmonitor->blobrequest,&newelem);
                              }
                           }
                           else{
                              printf("[BUG ROBOT %u] blob status messaage received could't determine bidder state \n",vm->robot);
                           }
                        }
                     }
                  }
               }
            }
            /* No bs or no key found nothing to do */
            break;
         }
         case BUZZMSG_BSTIG_CHUNK_STATUS_QUERY:{
            uint16_t receiver;
            int64_t pos = buzzmsg_deserialize_u16(&receiver, msg, 1);
            
               uint16_t id,key,cid,message;
               uint8_t subtype;
               pos = buzzmsg_deserialize_u16(&id, msg, pos);
               pos = buzzmsg_deserialize_u16(&key, msg, pos);
               pos = buzzmsg_deserialize_u16(&cid, msg, pos);
               pos = buzzmsg_deserialize_u8(&subtype, msg, pos);
               pos = buzzmsg_deserialize_u16(&message, msg, pos);
               if(pos < 0) {
                  fprintf(stderr,
                   "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_CHUNK_STATUS message received from %u",
                    vm->robot, rid);
                  break;
               }
            if(vm->robot == receiver || receiver == BROADCAST_MESSAGE_CONSTANT){
               // printf("RID: %u from %u chunk status msg received : id: %u key %u chunk %u, msg %u \n",vm->robot, rid, id, key, cid, message );
               // if(subtype==BUZZRELOCATION_REQUEST){
               //    /* look for bs */
               //    const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
               //    if(s){
               //       /* Look for blob key in blob bstig slot*/
               //       const buzzblob_elem_t* v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
               //       if(v_blob){
               //          const buzzblob_chunk_t* chunk = buzzdict_get( (*v_blob)->data, &(cid), buzzblob_chunk_t); 
               //          if(chunk){
               //             /* Accept only if the chunk is avialable and if you are not 
               //                trying to relocate it */
               //             struct buzzchunk_reloc_elem_s ecmp = {.id = id, .key=key, .cid=cid };
               //             buzzchunk_reloc_elem_t newelem = &ecmp;
               //             /* find the element from chunk monitor */
               //              uint16_t cmonindex = buzzdarray_find(vm->cmonitor->chunks,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                           
               //             if((*chunk)->status != BUZZCHUNK_LOST &&
               //                cmonindex == buzzdarray_size(vm->cmonitor->chunks)){
               //                /* Chunk can be reserved */
               //                /* Was it previously marked */
               //                if((*chunk)->status == BUZZCHUNK_MARKED){
               //                   /* Yes remove it because it will be reserved for RID */
               //                   uint16_t index = buzzdarray_find((*v_blob)->marked_list, buzzdict_uint16keycmp,&cid);
               //                   if(index != buzzdict_size((*v_blob)->marked_list))
               //                   buzzdarray_remove((*v_blob)->marked_list,index);
               //                }
               //                if((*chunk)->status != BUZZCHUNK_RESERVED && (*chunk)->status != BUZZCHUNK_RELOCATED){
               //                   (*chunk)->status = BUZZCHUNK_RESERVED;
               //                   buzzdarray_push((*v_blob)->reserved_list,&cid);
               //                   // printf("[RID : %u]Chunk state was not reserved, Now it is reserved  ",vm->robot);
               //                }
               //                // printf("BUZZRELOCATION_REQUEST Request accepted (Id: %u Key : %u cid :%u )\n", id,key,cid);
               //                buzzoutmsg_queue_append_chunk_reloc(vm, 
               //                                                    BUZZMSG_BSTIG_CHUNK_STATUS_QUERY,
               //                                                    id,
               //                                                    key,
               //                                                    cid,
               //                                                    rid,
               //                                                    BUZZRELOCATION_RESPONCE,
               //                                                    1);
               //                break;
               //             }
               //             else{
               //                /* I am trying to relocate it or I lost it */
               //             }
               //          }
               //       }
               //    }
                  
               //    // printf("BUZZRELOCATION_REQUEST Request declined\n");
               //    buzzoutmsg_queue_append_chunk_reloc(vm, 
               //                                        BUZZMSG_BSTIG_CHUNK_STATUS_QUERY,
               //                                        id,
               //                                        key,
               //                                        cid,
               //                                        rid,
               //                                        BUZZRELOCATION_RESPONCE,
               //                                        0);
               //    break;

               // }
               // else if(subtype==BUZZRELOCATION_RESPONCE){
               //    /* look for bs */
               //    const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
               //    if(s){
               //       /* Look for blob key in blob bstig slot*/
               //       const buzzblob_elem_t* v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
               //       if(v_blob){
               //          const buzzblob_chunk_t* chunk = buzzdict_get( (*v_blob)->data, &(cid), buzzblob_chunk_t);
               //          struct buzzchunk_reloc_elem_s ecmp = {.id = id, .key=key, .cid=cid };
               //          buzzchunk_reloc_elem_t newelem = &ecmp;
               //          if(chunk){
               //             if(message){
               //                // printf("BUZZRELOCATION_RESPONCE Request accepted\n");
               //                /* Add to relocated list if the entry not found */
               //                uint16_t index = buzzdarray_find((*v_blob)->relocated_list,buzzdict_uint16keycmp,&cid);
               //                if(index == buzzdarray_size((*v_blob)->relocated_list)){
               //                   buzzdarray_push((*v_blob)->relocated_list,&cid);
               //                }
                              
               //                /* Delete the existing element */
               //                buzzbstig_remove(*v_blob, &cid); 
               //                /* add a dummy blob to the place */
               //                char chunk[1];
               //                *chunk=0;
               //                buzzblob_chunk_t dum = buzzbstig_chunk_new(0,chunk);
               //                dum->status = BUZZCHUNK_RELOCATED;
               //                /* Add the dummy into the bstig slot */
               //                buzzdict_set((*v_blob)->data, &cid, &dum);
               //                /* Got rid of this chunk */
               //                (vm->cmonitor->chunknum)--;
               //                /* Remove the element from chunk monitor */
               //                index = buzzdarray_find(vm->cmonitor->chunks,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
               //                if(index !=buzzdarray_size(vm->cmonitor->chunks)){
               //                   buzzdarray_remove(vm->cmonitor->chunks,index);
               //                } 
               //             }
               //             else{
               //                // printf("BUZZRELOCATION_RESPONCE Request declined\n");
               //                /* Request declined */
               //                /* add to checked list in relocation monitor */
               //                uint16_t index;
               //                uint16_t cmonindex = buzzdarray_find(vm->cmonitor->chunks,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
               //                if(cmonindex !=buzzdarray_size(vm->cmonitor->chunks)){
               //                   const buzzchunk_reloc_elem_t celem = 
               //                                     buzzdarray_get(vm->cmonitor->chunks,cmonindex,buzzchunk_reloc_elem_t);
               //                   index = buzzdarray_find(celem->checkednids,buzzdict_uint16keycmp,&rid);
               //                   if(index == buzzdarray_size(celem->checkednids)){
               //                      buzzdarray_push(celem->checkednids,&rid);
               //                   }
               //                   /* This robot (rid) declined ask the next robot */
               //                   /* Ask new neighbour, if all done put to marked list*/
               //                   int next_nid = -1;
               //                   if(buzzdict_size(vm->active_neighbors) != 0){
               //                      buzzdarray_t nid = buzzdarray_new(10,
               //                                                       sizeof(uint16_t),
               //                                                       NULL);
               //                      buzzdict_foreach(vm->active_neighbors, buzzbstig_neigh_loop, &nid);
               //                      buzzdarray_sort(nid,buzzdict_uint16keycmp);
               //                      // printf("[VM RID: %u] Size of active neigh %u Size of checkednids %u \n",vm->robot, buzzdarray_size(nid), buzzdarray_size(celem->checkednids) );
               //                      for(uint16_t p = buzzdarray_size(nid)-1; p >= 0 && p < buzzdarray_size(nid) ;p--){
               //                         uint16_t nlist = buzzdarray_get(nid,p,uint16_t);
               //                         // printf("VM RID: %u at finding next highest neigh Current index %u nid %u \n",vm->robot, p,nlist );
               //                         index = buzzdarray_find(celem->checkednids,buzzdict_uint16keycmp,&nlist);
               //                         if(index == buzzdarray_size(celem->checkednids)){
               //                            /* found the next canditate */
               //                            next_nid = nlist;
               //                            celem->time_to_wait = MAX_TIME_TO_WAIT_FOR_NEIGHBOUR;
               //                            buzzdarray_push(celem->checkednids,&next_nid);
               //                            /* Send a message to this nid */
               //                            // printf("Asking next highest neighbour id to relocate: %u \n",next_nid );
               //                            buzzoutmsg_queue_append_chunk_reloc(vm,
               //                                                               BUZZMSG_BSTIG_CHUNK_STATUS_QUERY,
               //                                                               celem->id,
               //                                                               celem->key,
               //                                                               celem->cid,
               //                                                               next_nid,
               //                                                               BUZZRELOCATION_REQUEST,
               //                                                               0);
               //                            break;
               //                         }
               //                      }
               //                      buzzdarray_destroy(&nid);
               //                      if(next_nid == -1){
               //                         /* No new neighbour to ask so remove and add to marked list */
               //                         /* Add to marked list and remove from cmon */
               //                         const buzzdict_t* s = buzzdict_get(vm->blobs, &(celem->id), buzzdict_t);
               //                         if(s){
               //                            /* Look for blob key in blob bstig slot*/
               //                            const buzzblob_elem_t* v_blob = buzzdict_get(*s, &(celem->key), buzzblob_elem_t);
               //                            if(v_blob){
               //                               index = buzzdarray_find((*v_blob)->marked_list,buzzdict_uint16keycmp,&(celem->cid));
               //                               if(index == buzzdarray_size((*v_blob)->marked_list)){
               //                                  buzzdarray_push((*v_blob)->marked_list, &(celem->cid) );
               //                                  /* Change the state to marked */
               //                                  const buzzblob_chunk_t* chunk = buzzdict_get( (*v_blob)->data, &(celem->cid), buzzblob_chunk_t);
               //                                  (*chunk)->status = BUZZCHUNK_MARKED;
               //                               }
               //                            }
               //                         }
               //                         /* Destroy the entry and add to marked list */
               //                         buzzdarray_remove(vm->cmonitor->chunks,cmonindex);
               //                      }
               //                   }
               //                   //newelem->time_to_wait ;
               //                   /* else nothing to do rid already exsist */
               //                } /* No comon chunk slot exsists nothing to do */
               //             }
               //          }
               //       }
               //    }
               // }
               if(subtype == BUZZ_ADD_BLOB_LOCATION){
                  const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
                  if(s){
                     /* Look for blob key in blob bstig slot*/
                     const buzzblob_elem_t* v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
                     if(v_blob){
                        struct buzzblob_bidder_s locationcmp = {.rid = cid, .availablespace = message};
                        buzzblob_location_t ploccmp = &locationcmp;  
                        /* Does the location already exsists */
                        uint16_t index = buzzdarray_find((*v_blob)->locations,buzzbstig_blob_bidderelem_key_cmp,&(ploccmp));
                        if(index == buzzdarray_size((*v_blob)->locations)){
                           buzzblob_location_t locationelem = (buzzblob_location_t)malloc(sizeof(struct buzzblob_bidder_s));
                           locationelem->rid = cid;
                           locationelem->availablespace = message;
                           buzzdarray_push((*v_blob)->locations, &(locationelem) );
                           if((*v_blob)->relocstate == BUZZBLOB_SINK){
                              /* New location added change the state, so, the robot can request the locations */
                              (*v_blob)->relocstate = BUZZBLOB_OPEN;
                           }
                           buzzoutmsg_queue_append_chunk_reloc(vm,
                                                              BUZZMSG_BSTIG_CHUNK_STATUS_QUERY,
                                                              id,
                                                              key,
                                                              cid,
                                                              BROADCAST_MESSAGE_CONSTANT,
                                                              BUZZ_ADD_BLOB_LOCATION,
                                                              message);
                        }                          
                     }
                  }

               }
            }
            /* No bs or no key found nothing to do */
            break;
         }
         case BUZZMSG_BSTIG_CHUNK_REMOVED:{
      
            int64_t pos = 1;
            
            uint16_t id,key,cid;
            uint8_t subtype;
            pos = buzzmsg_deserialize_u16(&id, msg, pos);
            pos = buzzmsg_deserialize_u16(&key, msg, pos);
            pos = buzzmsg_deserialize_u16(&cid, msg, pos);
            pos = buzzmsg_deserialize_u8(&subtype, msg, pos);
            if(pos < 0) {
               fprintf(stderr,
                "[WARNING] [ROBOT %u] Malformed BUZZMSG_BSTIG_CHUNK_STATUS message received from %u",
                 vm->robot, rid);
               break;
            }
            // if(subtype == BUZZCHUNK_REMOVED_PERMANETLY){
            //    /* Look for the chunk into you storage, if you have it reply else forward msg*/
            //    const buzzdict_t* s = buzzdict_get(vm->blobs, &(id), buzzdict_t);
            //    if(s){
            //       /* Look for blob key in blob bstig slot*/
            //       const buzzblob_elem_t* v_blob = buzzdict_get(*s, &(key), buzzblob_elem_t);
            //       if(v_blob){
            //          const buzzblob_chunk_t* chunk = buzzdict_get( (*v_blob)->data, &(cid), buzzblob_chunk_t);
            //          if(chunk){
            //             if((*chunk)->status != BUZZCHUNK_RELOCATED &&
            //                (*chunk)->status != BUZZCHUNK_LOST){
            //                /* I have the chunk, broadcast the avialability */
            //                buzzoutmsg_queue_append_chunk_removal(vm,
            //                                                      BUZZMSG_BSTIG_CHUNK_REMOVED,
            //                                                      id,
            //                                                      key,
            //                                                      cid,
            //                                                      BUZZCHUNK_AVAILABLE);
            //                /* Remove it from the cmon, if the robot is trying to relocate it*/
            //                struct buzzchunk_reloc_elem_s ecmp = {.id = id, .key=key, .cid=cid };
            //                buzzchunk_reloc_elem_t newelem = &ecmp;
            //                uint16_t cmonindex = buzzdarray_find(vm->cmonitor->chunks,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
            //                if(cmonindex !=buzzdarray_size(vm->cmonitor->chunks)){
            //                   buzzdarray_remove(vm->cmonitor->chunks,cmonindex);
            //                }
            //                /* Add to reserved list */
            //                (*chunk)->status = BUZZCHUNK_RESERVED;
            //                uint16_t index = buzzdarray_find((*v_blob)->reserved_list, buzzdict_uint16keycmp, &cid);
            //                if(index == buzzdarray_size((*v_blob)->reserved_list))
            //                   buzzdarray_push((*v_blob)->reserved_list, &(cid) );
            //                break;
            //             }
            //          }
            //          /* Propagete the message */
            //          /* Forward the message */
            //          buzzoutmsg_queue_append_chunk_removal(vm,
            //                                                BUZZMSG_BSTIG_CHUNK_REMOVED,
            //                                                id,
            //                                                key,
            //                                                cid,
            //                                                subtype);
            //          break;
            //       }
            //    }
            //    break;
            // }
            // else if(subtype == BUZZCHUNK_AVAILABLE){
            //    /* Find whether you lost this chunk */
            //    const buzzdict_t* s = buzzdict_get(vm->blobs, &(id), buzzdict_t);
            //    if(s){
            //        Look for blob key in blob bstig slot
            //       const buzzblob_elem_t* v_blob = buzzdict_get(*s, &(key), buzzblob_elem_t);
            //       if(v_blob){
            //          const buzzblob_chunk_t* chunk = buzzdict_get( (*v_blob)->data, &(cid), buzzblob_chunk_t);
            //          if(chunk){
            //             if((*chunk)->status == BUZZCHUNK_LOST){
            //                /*Yes I lost the chunk, Remove from lost list */
            //                uint16_t index = buzzdarray_find((*v_blob)->lost_list, buzzdict_uint16keycmp, &cid);
            //                if(index != buzzdarray_size((*v_blob)->lost_list)){
            //                   buzzdarray_remove((*v_blob)->lost_list,index);
            //                }
            //                /* Change the state and add to relocate list */
            //                (*chunk)->status = BUZZCHUNK_RELOCATED;
            //                index = buzzdarray_find((*v_blob)->relocated_list, buzzdict_uint16keycmp, &cid);
            //                if(index == buzzdarray_size((*v_blob)->relocated_list))
            //                   buzzdarray_push((*v_blob)->relocated_list, &(cid) );
            //                /* Remove from lost chunk traker in cmon */
            //                struct buzzchunk_reloc_elem_s ecmp = {.id = id, .key=key, .cid=cid };
            //                buzzchunk_reloc_elem_t newelem = &ecmp;
            //                uint16_t cmonindex = buzzdarray_find(vm->cmonitor->lostchunks,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
            //                if(cmonindex !=buzzdarray_size(vm->cmonitor->lostchunks)){
            //                   buzzdarray_remove(vm->cmonitor->lostchunks,cmonindex);
            //                   /* All good*/
            //                }
            //                else{
            //                   printf("[BUG RID : %u] Robot lost a chunk and the lost chunk tracker doesnot exist \n",vm->robot);

            //                }
            //                /* Forward the message */
            //                buzzoutmsg_queue_append_chunk_removal(vm,
            //                                                      BUZZMSG_BSTIG_CHUNK_REMOVED,
            //                                                      id,
            //                                                      key,
            //                                                      cid,
            //                                                      subtype);
            //                break;
            //             }
            //          }
            //          /* If you reach here you have the blob, so propagate the message */
            //          /* Forward the message */
            //          buzzoutmsg_queue_append_chunk_removal(vm,
            //                                                BUZZMSG_BSTIG_CHUNK_REMOVED,
            //                                                id,
            //                                                key,
            //                                                cid,
            //                                                subtype);
            //          break;
            //       }
            //    }
            //    break;
            // }
            if(subtype == BUZZCHUNK_BLOB_REMOVED){
               /* This blob was lost because of an unresolved chunk */
               /* VM->blobs */
               const buzzdict_t* s = buzzdict_get(vm->blobs, &(id), buzzdict_t);
               if(s){
                  /* Look for blob key in blob bstig slot*/
                  const buzzblob_elem_t* v_blob = buzzdict_get(*s, &(key), buzzblob_elem_t);
                  if(v_blob){
                     /* Yes the blob exsist */
                     /* Clear all data related to this blob */
                     /* Remove all slot holders */
                     /* Cmon chunks */
                     // for(uint16_t ch = 0; ch< buzzdarray_size(vm->cmonitor->chunks);){
                     //    const buzzchunk_reloc_elem_t chelem = buzzdarray_get(vm->cmonitor->chunks,ch,buzzchunk_reloc_elem_t);
                     //    if(chelem->id == id && chelem->key == key){
                     //       buzzdarray_remove(vm->cmonitor->chunks,ch);
                           
                     //    }
                     //    else ch++;
                     // }
                     //  Cmon lost chunks 
                     // for(uint16_t ch = 0; ch< buzzdarray_size(vm->cmonitor->lostchunks);){
                     //    const buzzchunk_reloc_elem_t chelem = buzzdarray_get(vm->cmonitor->lostchunks,ch,buzzchunk_reloc_elem_t);
                     //    if(chelem->id == id && chelem->key == key){
                     //       buzzdarray_remove(vm->cmonitor->lostchunks,ch);
                          
                     //    }
                     //    else ch++;
                     // }
                     /* Change the size of total chunks inside cmon */
                     uint32_t available_chunk = buzzdict_size((*v_blob)->available_list);
                     (vm->cmonitor->chunknum) = (vm->cmonitor->chunknum)-available_chunk;
                     /* remove the blob holder */
                     buzzdict_remove(*s,&(key));
                  
               
                     /* Clear out messages related to this blob*/
                     buzzoutmsg_remove_chunk_put_msg(vm,
                                                     id,
                                                     key);
                     /* VM->bstigs can stay for now TODO: THINK about what to do with it ?, to indicate lost blobs */
                  }
               }
               /* Propagate the message */
               buzzoutmsg_queue_append_chunk_removal(vm,
                                               BUZZMSG_BSTIG_CHUNK_REMOVED,
                                               id,
                                               key,
                                               cid,
                                               subtype);
               break;
            }
            // else if(subtype == BUZZCHUNK_NEIGHBOUR_QUERY){
            //    /* Look for the chunk into you storage, if you have it reply else forward msg*/
            //    const buzzdict_t* s = buzzdict_get(vm->blobs, &(id), buzzdict_t);
            //    if(s){
            //       /* Look for blob key in blob bstig slot*/
            //       const buzzblob_elem_t* v_blob = buzzdict_get(*s, &(key), buzzblob_elem_t);
            //       if(v_blob){
            //          const buzzblob_chunk_t* chunk = buzzdict_get( (*v_blob)->data, &(cid), buzzblob_chunk_t);
            //          if(chunk){
            //             if((*chunk)->status != BUZZCHUNK_RELOCATED &&
            //                (*chunk)->status != BUZZCHUNK_LOST){
            //                /* I have the chunk, broadcast the avialability */
            //                buzzoutmsg_queue_append_chunk_removal(vm,
            //                                                      BUZZMSG_BSTIG_CHUNK_REMOVED,
            //                                                      id,
            //                                                      key,
            //                                                      cid,
            //                                                      BUZZCHUNK_NEIGHBOUR_AVIALABLE);
            //                /* Remove it from the cmon, if the robot is trying to relocate it*/
            //                struct buzzchunk_reloc_elem_s ecmp = {.id = id, .key=key, .cid=cid };
            //                buzzchunk_reloc_elem_t newelem = &ecmp;
            //                uint16_t cmonindex = buzzdarray_find(vm->cmonitor->chunks,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
            //                if(cmonindex !=buzzdarray_size(vm->cmonitor->chunks)){
            //                   buzzdarray_remove(vm->cmonitor->chunks,cmonindex);
            //                }
            //                /* Remove it from the marked list if it exsists */
            //                uint16_t index = buzzdarray_find((*v_blob)->marked_list, buzzdict_uint16keycmp, &cid);
            //                if(index != buzzdarray_size((*v_blob)->marked_list))
            //                   buzzdarray_remove((*v_blob)->marked_list, index );
            //                /* Add to reserved list */
            //                (*chunk)->status = BUZZCHUNK_RESERVED;
            //                index = buzzdarray_find((*v_blob)->reserved_list, buzzdict_uint16keycmp, &cid);
            //                if(index == buzzdarray_size((*v_blob)->reserved_list))
            //                   buzzdarray_push((*v_blob)->reserved_list, &(cid) );
            //                break;
            //             }
            //          }
            //       }
            //    }
            //    break;
            // }
            // else if(subtype == BUZZCHUNK_NEIGHBOUR_AVIALABLE){
            //    /* Find whether you lost this chunk */
            //    const buzzdict_t* s = buzzdict_get(vm->blobs, &(id), buzzdict_t);
            //    if(s){
            //       /* Look for blob key in blob bstig slot*/
            //       const buzzblob_elem_t* v_blob = buzzdict_get(*s, &(key), buzzblob_elem_t);
            //       if(v_blob){
            //          const buzzblob_chunk_t* chunk = buzzdict_get( (*v_blob)->data, &(cid), buzzblob_chunk_t);
            //          if(chunk){
            //             if((*chunk)->status == BUZZCHUNK_LOST){
            //                /*Yes I lost the chunk, Remove from lost list */
            //                uint16_t index = buzzdarray_find((*v_blob)->lost_list, buzzdict_uint16keycmp, &cid);
            //                if(index != buzzdarray_size((*v_blob)->lost_list)){
            //                   buzzdarray_remove((*v_blob)->lost_list,index);
            //                }
            //                /* Change the state and add to relocate list */
            //                (*chunk)->status = BUZZCHUNK_RELOCATED;
            //                index = buzzdarray_find((*v_blob)->relocated_list, buzzdict_uint16keycmp, &cid);
            //                if(index == buzzdarray_size((*v_blob)->relocated_list))
            //                   buzzdarray_push((*v_blob)->relocated_list, &(cid) );
            //                /* Remove from lost chunk traker in cmon */
            //                struct buzzchunk_reloc_elem_s ecmp = {.id = id, .key=key, .cid=cid };
            //                buzzchunk_reloc_elem_t newelem = &ecmp;
            //                uint16_t cmonindex = buzzdarray_find(vm->cmonitor->lostchunks,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
            //                if(cmonindex !=buzzdarray_size(vm->cmonitor->lostchunks)){
            //                   buzzdarray_remove(vm->cmonitor->lostchunks,cmonindex);
            //                   /* All good*/
            //                }
            //                else{
            //                   printf("[BUG RID : %u] Robot lost a chunk and the lost chunk tracker doesnot exist \n",vm->robot);

            //                }
                           
            //                break;
            //             }
            //          }
            //       }
            //    }
            //    break;
            // }
            /* Unknown subtype messsage */
            break;
         }
         case BUZZMSG_BSTIG_BLOB_BID:{
            int64_t pos = 1;
            uint16_t id,key,bidderid;
            uint8_t subtype;
            uint32_t hash, blob_size;
            pos = buzzmsg_deserialize_u16(&id, msg, pos);
            pos = buzzmsg_deserialize_u16(&key, msg, pos);
            pos = buzzmsg_deserialize_u16(&bidderid, msg, pos);
            pos = buzzmsg_deserialize_u8(&subtype, msg, pos);
            // printf("[RID : %u] Bid msg receiver from %u, id: %u, k %u, bidderid: %u, subtype : %u\n",vm->robot, rid, id, key, bidderid,subtype);
            if(subtype == BUZZBSTIG_BID_NEW){
               pos = buzzmsg_deserialize_u32(&blob_size, msg, pos);
               pos = buzzmsg_deserialize_u32(&hash, msg, pos);
               // printf("blob_size : %u, hash: %u \n",blob_size,hash );
               /* Retrive the local data about the blob */
               /* Look for blob holder and create a new if none exsists */
               const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
               const buzzblob_elem_t* v_blob = NULL;
               /*Nothing to do, if bs id doesnot exsist*/
               if(s){
                  /* Look for blob key in blob bstig slot*/
                  v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
                  if(!v_blob){
                  /* Create data holders to host the blob */
                  buzz_blob_slot_holders_new(vm, id, key, blob_size,hash);
                   
                  }
               }
               /* Did I create the bid ? */
               struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZBSTIG_BID_NEW};
               buzzchunk_reloc_elem_t newelem = &cmpelem;
               uint16_t cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
               if(cmonindex == buzzdarray_size(vm->cmonitor->bidder)){
                  if(s){
                     int myavailsize = MAX_BLOB_CHUNKS - vm->cmonitor->chunknum;
                     uint16_t chunkpieces = ceil(((float)blob_size/(float)BLOB_CHUNK_SIZE));
                     /* Check wheter you already bid */
                     cmpelem.cid = BUZZBSITG_BID_REPLY;
                     cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                     if(myavailsize > 0 &&
                        cmonindex == buzzdarray_size(vm->cmonitor->bidder)){
                        /* I have some space to store chunks, bid for the chunk */
                        uint16_t bidsize = (myavailsize > chunkpieces) ? chunkpieces+1 : myavailsize;
                        /* Append to bidder inside the VM reloc mon and append a message */
                        buzzbstig_relocation_bider_add(vm,
                                                      id,
                                                      key,
                                                      bidsize,
                                                      blob_size,
                                                      hash,
                                                      BUZZBSITG_BID_REPLY,
                                                      bidderid);
                        // printf("[rid %u]Sent a bid reply \n",vm->robot);
                        /* Hold the space for this bid */
                        //vm->cmonitor->chunknum = vm->cmonitor->chunknum + bidsize;
                        /* Forward the received message to neighbours */
                        buzzoutmsg_queue_append_bid(vm,
                                                  BUZZMSG_BSTIG_BLOB_BID,
                                                  id,
                                                  key,
                                                  bidderid,
                                                  0,
                                                  blob_size,
                                                  hash,
                                                  0,
                                                  (uint16_t)subtype,
                                                  bidderid);
                     }
                     else{
                        /* I can't bid, Simply forward the message */
                        buzzoutmsg_queue_append_bid(vm,
                                                  BUZZMSG_BSTIG_BLOB_BID,
                                                  id,
                                                  key,
                                                  bidderid,
                                                  0,
                                                  blob_size,
                                                  hash,
                                                  0,
                                                  (uint16_t)subtype,
                                                  bidderid);
                     }
                  }
               }
               break;
            }
            else if(subtype == BUZZBSITG_BID_REPLY){

               const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
               const buzzblob_elem_t* v_blob = NULL;
               /*Nothing to do, if bs id & key doesnot exsist*/
               if(s){
                  /* Look for blob key in blob bstig slot*/
                  v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
                  if(v_blob){
                     uint8_t getter;
                     pos = buzzmsg_deserialize_u8(&getter, msg, pos);
                     uint16_t recvavilsize;
                     pos = buzzmsg_deserialize_u16(&recvavilsize, msg, pos);
                     // printf(" robot %u BID Reply size:%u getter: %u from %u\n",vm->robot, recvavilsize,getter,rid);
                     /* Are you the one who created the bid ?*/
                     struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZBSTIG_BID_NEW};
                     buzzchunk_reloc_elem_t newelem = &cmpelem;
                     uint16_t cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                     if(cmonindex !=buzzdarray_size(vm->cmonitor->bidder)){
                        const buzzchunk_reloc_elem_t celem = 
                                                         buzzdarray_get(vm->cmonitor->bidder,cmonindex,buzzchunk_reloc_elem_t);
                        /* Yes, I created the bid */
                        /* Check weather the bid of this robot already exsists */
                        struct buzzblob_bidder_s biddercmp = {.rid = bidderid, .availablespace = recvavilsize};
                        buzzblob_bidder_t belem = &biddercmp; 
                        uint16_t index = buzzdarray_find(celem->checkednids, buzzbstig_blob_bidderelem_key_cmp, &belem);
                        if(index == buzzdarray_size(celem->checkednids)){
                           buzzblob_bidder_t addbider = (buzzblob_bidder_t)malloc(sizeof(struct buzzblob_bidder_s));
                           addbider->rid = bidderid;
                           addbider->role = getter;
                           // printf("GOT GETTER STATE : %u \n",getter );
                           addbider->availablespace = recvavilsize;
                           buzzdarray_push(celem->checkednids, &(addbider) );
                           /* If the alloction is done it is a late reply sort the list again */
                           cmpelem.cid = BUZZCHUNK_BID_ALLOCATION;
                           buzzchunk_reloc_elem_t newelem = &cmpelem;
                           cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                           if(cmonindex !=buzzdarray_size(vm->cmonitor->bidder)){
                              /* Sort the allocation array in the accending order */
                              buzzdarray_sort(celem->checkednids,buzzbstig_blob_bidderpriority_key_cmp);
                           }
                           break;
                        }
                        else{
                           /* I already have your bid */
                        }
                     }
                     else{
                        /* No, i did not create the bid, forward the msg */
                        // buzzoutmsg_queue_append_bid(vm,
                        //                           BUZZMSG_BSTIG_BLOB_BID,
                        //                           id,
                        //                           key,
                        //                           bidderid,
                        //                           recvavilsize,
                        //                           blob_size,
                        //                           hash,
                        //                           getter,
                        //                           (uint16_t)subtype);
                     }
                  }
               }
               break;
            }
            else if(subtype == BUZZCHUNK_BID_ALLOCATION){
               /* Decode bid size */
               uint16_t recvavilsize;
               pos = buzzmsg_deserialize_u16(&recvavilsize, msg, pos);
               if(bidderid == vm->robot){
                  /* Am I the receipient of this message */
                  const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
                  /* Nothing to do, if bs id and key doesnot exsist */
                  if(s){
                     /* Look for blob key in blob bstig slot*/
                     const buzzblob_elem_t* v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
                     if(v_blob){
                        /* Did I already accept the bid */
                        struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZCHUNK_BID_ALLOC_ACCEPT};
                        buzzchunk_reloc_elem_t newelem = &cmpelem;
                        uint16_t cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                        if(cmonindex == buzzdarray_size(vm->cmonitor->bidder)){
                           /* Did I get the bid allocated, I will have to accept it or decline */
                           cmpelem.cid=BUZZBSITG_BID_REPLY;
                           cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                           if(cmonindex != buzzdarray_size(vm->cmonitor->bidder)){
                              /* Remove the element from cmon */
                              buzzdarray_remove(vm->cmonitor->bidder,cmonindex);
                           }
                          
                           /* Yes I bid for it */
                           int myavailsize = MAX_BLOB_CHUNKS - vm->cmonitor->chunknum;
                           if (myavailsize >= recvavilsize){
                              /* Accept bid */
                              buzzbstig_relocation_bider_add(vm,
                                                            id,
                                                            key,
                                                            recvavilsize,
                                                            blob_size,
                                                            hash,
                                                            BUZZCHUNK_BID_ALLOC_ACCEPT,
                                                            rid);
                              if((*v_blob)->relocstate != BUZZBLOB_SOURCE && (*v_blob)->relocstate != BUZZBLOB_SINK)
                              /* Indicate this is a host of this blob */
                              (*v_blob)->relocstate = BUZZBLOB_HOST;

                              // printf("ROBOT %u accepted blob state reloc %u from %u\n",vm->robot, (*v_blob)->relocstate,rid);
                              /* reserve the size once you accept */
                              (vm->cmonitor->chunknum)+=recvavilsize;
                              
                           }
                           else{
                              /* Decline bid */
                              buzzoutmsg_queue_append_bid(vm,
                                                           BUZZMSG_BSTIG_BLOB_BID,
                                                           id,
                                                           key,
                                                           vm->robot,
                                                           recvavilsize,
                                                           blob_size,
                                                           hash,
                                                           0,
                                                           (uint16_t)BUZZCHUNK_BID_ALLOC_REJECT,
                                                           rid);
                              /* Remove the element from cmon and reject any further request on this same element */
                              buzzdarray_remove(vm->cmonitor->bidder,cmonindex);
                           }
                        }
                     }
                  }
                  break;
               }
               else{
                  // buzzoutmsg_queue_append_bid(vm,
                  //                              BUZZMSG_BSTIG_BLOB_BID,
                  //                              id,
                  //                              key,
                  //                              bidderid,
                  //                              recvavilsize,
                  //                              blob_size,
                  //                              hash,
                  //                              0,
                  //                              (uint16_t)subtype);
                  break;
               }
              
            }
            else if(subtype == BUZZCHUNK_BID_ALLOC_ACCEPT){
               /* Decode bid size */
               uint16_t recvavilsize;
               pos = buzzmsg_deserialize_u16(&recvavilsize, msg, pos);

               const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
               /* Nothing to do, if bs id doesnot exsist */
               if(s){
                  /* Look for blob key in blob bstig slot*/
                  const buzzblob_elem_t* v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
                  if(v_blob){
                     /* Did I allocate it */
                     struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZCHUNK_BID_ALLOCATION};
                     buzzchunk_reloc_elem_t newelem = &cmpelem;
                     uint16_t cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                     if(cmonindex !=buzzdarray_size(vm->cmonitor->bidder)){
                        const buzzchunk_reloc_elem_t celem = 
                           buzzdarray_get(vm->cmonitor->bidder,cmonindex,buzzchunk_reloc_elem_t);
                        /* Find the allocation inside the monitor */
                        struct buzzblob_bidder_s biddercmp = {.rid = bidderid, .availablespace = recvavilsize};
                        buzzblob_bidder_t belem = &biddercmp;   
                        uint16_t index = buzzdarray_find(celem->checkednids, buzzbstig_blob_bidderelem_key_cmp, &belem);
                        if(index != buzzdarray_size(celem->checkednids)){
                           buzzblob_bidder_t lbid = buzzdarray_get(celem->checkednids,index,buzzblob_bidder_t);
                           // printf("[RID: %u ] Allocating blob id: %u key: %u to : rceivd: %u / %u, size of aloc : %u\n",vm->robot,id,key,lbid->rid, bidderid,lbid->availablespace);
                           /* Allocate the chunks of respective size to this robot in P2P Queue */
                           buzzbstig_allocate_chunks(vm,
                                                     id,
                                                     key,
                                                     bidderid,
                                                     lbid->availablespace);
                           /* Remove this allocation from monitor */
                           buzzdarray_remove(celem->checkednids,index);
                        }
                     }
                     else{
                        // buzzoutmsg_queue_append_bid(vm,
                        //                              BUZZMSG_BSTIG_BLOB_BID,
                        //                              id,
                        //                              key,
                        //                              bidderid,
                        //                              recvavilsize,
                        //                              blob_size,
                        //                              hash,
                        //                              0,
                        //                              (uint16_t)subtype);
                        
                     }
                  }
               }
               break;
            }
            else if(subtype == BUZZCHUNK_BID_ALLOC_REJECT){
               /* Decode bid size */
               uint16_t recvavilsize;
               pos = buzzmsg_deserialize_u16(&recvavilsize, msg, pos);
               // printf("bid size : %u\n",recvavilsize );
               const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
               /* Nothing to do, if bs id doesnot exsist */
               if(s){
                  /* Look for blob key in blob bstig slot*/
                  const buzzblob_elem_t* v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
                  if(v_blob){
                     /* Did I allocate it */
                     struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZCHUNK_BID_ALLOCATION};
                     buzzchunk_reloc_elem_t newelem = &cmpelem;
                     uint16_t cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                     if(cmonindex !=buzzdarray_size(vm->cmonitor->bidder)){
                        const buzzchunk_reloc_elem_t allocationelem = 
                           buzzdarray_get(vm->cmonitor->bidder,cmonindex,buzzchunk_reloc_elem_t);
                           allocationelem->time_to_wait = MAX_TIME_TO_HOLD_SPACE_FOR_A_BID;
                        /* Find the allocation inside the monitor */
                        struct buzzblob_bidder_s biddercmp = {.rid = bidderid, .availablespace = recvavilsize};
                        buzzblob_bidder_t belem = &biddercmp;   
                        uint16_t index = buzzdarray_find(allocationelem->checkednids, buzzbstig_blob_bidderelem_key_cmp, &belem);
                        if(index != buzzdarray_size(allocationelem->checkednids)){
                           /* Find the bid element */
                           cmpelem.cid=BUZZBSTIG_BID_NEW;
                           buzzchunk_reloc_elem_t bidelem = &cmpelem;
                           cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&bidelem);
                           if(cmonindex !=buzzdarray_size(vm->cmonitor->bidder)){
                              /* Bid element found */
                              const buzzchunk_reloc_elem_t bidderselem = 
                                       buzzdarray_get(vm->cmonitor->bidder,cmonindex,buzzchunk_reloc_elem_t);

                              /* Find the last element in the allocation list */
                              buzzblob_bidder_t lastalocrobot = 
                                       buzzdarray_last(allocationelem->checkednids,buzzblob_bidder_t);
                              uint16_t lastlocationrobotindex = MAX_UINT16;
                              /* Is there an element in location list */
                              if(buzzdarray_size((*v_blob)->locations)>0){
                                 /* Find the last element in locaiton list */
                                 buzzblob_bidder_t lastlocationrobot = 
                                          buzzdarray_last((*v_blob)->locations,buzzblob_bidder_t);
                                 lastlocationrobotindex = buzzdarray_find(bidderselem->checkednids, buzzbstig_blob_bidderelem_key_cmp, &lastlocationrobot);
                              }
                              /* Find the next robots id in the bidder list */
                              uint16_t bidindex = buzzdarray_find(bidderselem->checkednids, buzzbstig_blob_bidderelem_key_cmp, &lastalocrobot);
                              uint16_t indextouse = (bidindex < lastlocationrobotindex) ? bidindex : lastlocationrobotindex;
                              // printf("Index of last aloc robot %u  and loction robot %u used %u \n",bidindex,lastlocationrobotindex,indextouse );
                              /* Allocate to other robots, again assuming the bid is ordered */
                              uint16_t allocsize=0;
                              /* Remove the allocation of the rejected robot */
                              buzzdarray_remove(allocationelem->checkednids,index); 
                              /* Transfer the allocated size to other robots */
                              for(int y=indextouse-1; y >= 0 && allocsize < recvavilsize; y--){
                                 const buzzblob_bidder_t bidelem = 
                                          buzzdarray_get(bidderselem->checkednids,y,buzzblob_bidder_t);
                                 // printf(" RID: %u Alloc rejected by %u , k %u id %u alloc to %u index of checked nids %i  \n",vm->robot,
                                 //          bidderid,key,id,bidelem->rid,y );
                                 uint16_t allocated_size = (allocsize + bidelem->availablespace < recvavilsize) ? bidelem->availablespace : recvavilsize - allocsize;  
                                 /* Highest rid gets bid size, send an allocation message */
                                 buzzblob_bidder_t addbider = (buzzblob_bidder_t)malloc(sizeof(struct buzzblob_bidder_s));
                                 addbider->rid = bidelem->rid;
                                 addbider->role = bidelem->role;
                                 addbider->availablespace = allocated_size;
                                 /* Add the size to allocated size */
                                 allocsize+=allocated_size;
                                 buzzdarray_push(allocationelem->checkednids, &(addbider) );
                                 /* Send an allocation messsage */
                                 buzzoutmsg_queue_append_bid(vm,
                                                              BUZZMSG_BSTIG_BLOB_BID,
                                                              bidderselem->id,
                                                              bidderselem->key,
                                                              bidelem->rid,
                                                              allocated_size,
                                                              (*v_blob)->size,
                                                              (*v_blob)->hash,
                                                              bidelem->role,
                                                              (uint16_t) BUZZCHUNK_BID_ALLOCATION,
                                                              bidelem->rid);
                              }
                              if(allocsize < recvavilsize){
                                 /* Could not allocate all chunks, intiate the priority policy and remove the oldest blob */
                                 /* create a darray for storing the id and key in accending order of priority */
                                 buzzdarray_t priority = buzzbstig_get_prioritylist(vm);
                                 /* Get the first blob and try force allocation */
                                 if(!buzzdarray_isempty(priority) ){
                                    for(uint16_t pi=0; pi < buzzdarray_size(priority) && 
                                        allocsize < recvavilsize; pi++){
                                       const struct buzzbstig_blobpriority_list_param* pe = buzzdarray_get(priority, pi, struct buzzbstig_blobpriority_list_param*);
                                       /* Fetch the entries in order form priority list */
                                       const buzzdict_t* s = buzzdict_get(vm->blobs, &(pe->id), buzzdict_t);
                                       if(s){
                                          /* Look for blob key in blob bstig slot*/
                                          const buzzblob_elem_t* v_blob = buzzdict_get(*s, &(pe->key), buzzblob_elem_t);
                                          if(v_blob){
                                             /* Create a chunk montior for force allocation */
                                             buzzchunk_reloc_elem_t newelem =(buzzchunk_reloc_elem_t)malloc(sizeof(struct buzzchunk_reloc_elem_s));
                                             newelem->id = id;
                                             newelem->key = key;
                                             newelem->cid = BUZZCHUNK_BID_FORCE_ALLOCATION;
                                             newelem->bidsize = 0;  // temp hold the last id from priority list
                                             newelem->time_to_wait = MAX_TIME_TO_HOLD_SPACE_FOR_A_BID;
                                             newelem->time_to_destroy = 0; // temp hold the last key from priority list
                                             newelem->checkednids = buzzdarray_new(10,sizeof(buzzblob_bidder_t),buzzbstig_blob_bidderelem_destroy);
                                       
                                             /* Add the allocaiton element into the bidder */
                                             buzzdarray_push(vm->cmonitor->bidder,&newelem);
                                             for(int l=0;l<buzzdarray_size((*v_blob)->locations) && 
                                                   allocsize < recvavilsize; l++){
                                                /* Get the first location element of lowest priority blob */
                                                buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,l, buzzblob_location_t);
                                                /* Allocate if I am not allocating to myself */
                                                if(lowloc->rid != vm->robot){
                                                   /* Force allocate the chunk */
                                                   uint16_t allocated_size = (allocsize + lowloc->availablespace < recvavilsize) ? lowloc->availablespace : recvavilsize - allocsize;  
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
                                                                                id,
                                                                                key,
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
                                    printf("[RID: %u] ERROR, BUZZCHUNK_BID_ALLOC_REJECT [ priority list empty ] Trying to force allocate but no blob exsist to remove, try increasing available size\n", vm->robot );   
                                 }
                                 if(allocsize < recvavilsize)
                                    printf("[RID: %u] ERROR, BUZZCHUNK_BID_ALLOC_REJECT [ size ] Trying to force allocate but no blob exsist to remove, try increasing available size\n", vm->robot );
                                 buzzdarray_destroy(&priority);
                              }  
                              
                           }
                           else{
                              printf("[BUG] RID : %d allocated the chunks with missing bid element \n",vm->robot);
                           }

                        }
                        else{
                        // buzzoutmsg_queue_append_bid(vm,
                        //                              BUZZMSG_BSTIG_BLOB_BID,
                        //                              id,
                        //                              key,
                        //                              bidderid,
                        //                              recvavilsize,
                        //                              blob_size,
                        //                              hash,
                        //                              0,
                        //                              (uint16_t)subtype);
                        
                        }
                     }
                     else{
                        // buzzoutmsg_queue_append_bid(vm,
                        //                              BUZZMSG_BSTIG_BLOB_BID,
                        //                              id,
                        //                              key,
                        //                              bidderid,
                        //                              recvavilsize,
                        //                              blob_size,
                        //                              hash,
                        //                              0,
                        //                              (uint16_t)subtype);
                     }
                  }
               }
               break;
            }
            else if(subtype == BUZZCHUNK_BID_FORCE_ALLOCATION){
               /* Decode the key and id to remove */
               uint16_t keytoremove, idtoremove;
               pos = buzzmsg_deserialize_u16(&idtoremove, msg, pos);
               pos = buzzmsg_deserialize_u16(&keytoremove, msg, pos);
               /* Decode bid size */
               uint16_t recvavilsize;
               pos = buzzmsg_deserialize_u16(&recvavilsize, msg, pos);
                // printf("BUZZCHUNK_BID_FORCE_ALLOCATION bidderid: %u for id: %u key: %u idto remove: %u key toremove: %u bid size : %u\n", 
                //      bidderid,id,key, idtoremove, keytoremove, recvavilsize );
              
               if(bidderid == vm->robot){
                  /* Did you already accept ?*/
                  struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZCHUNK_BID_FORCE_ALLOCATION_ACCEPT};
                  buzzchunk_reloc_elem_t newelem = &cmpelem;
                  uint16_t cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                  if(cmonindex == buzzdarray_size(vm->cmonitor->bidder)){
                     /* Retrive the blob to remove */
                     /* Look for blob holder */
                     const buzzdict_t* s = buzzdict_get(vm->blobs, &(idtoremove), buzzdict_t);
                     const buzzblob_elem_t* v_blob = NULL;
                     /*Nothing to do, if bs id doesnot exsist*/
                     if(s){
                        /* Look for blob key in blob bstig slot*/
                        v_blob = buzzdict_get(*s, &(keytoremove), buzzblob_elem_t);
                     }
                     if (MAX_BLOB_CHUNKS - (vm->cmonitor->chunknum) >= recvavilsize){
                        // printf("There is space some how without removing blobs accept \n");
                        const buzzdict_t* news = buzzdict_get(vm->blobs, &(id), buzzdict_t);
                        const buzzblob_elem_t* newv_blob = NULL;
                         if(s){
                           /* Look for blob key in blob bstig slot*/
                           newv_blob = buzzdict_get(*news, &(key), buzzblob_elem_t);
                        }
                        if(newv_blob){
                           if((*newv_blob)->relocstate != BUZZBLOB_SOURCE && (*newv_blob)->relocstate != BUZZBLOB_SINK)
                              /* Indicate this is a host of this blob */
                              (*newv_blob)->relocstate = BUZZBLOB_HOST;
                        }
                        // printf("ROBOT %u accepted blob state reloc %u\n",vm->robot, (*v_blob)->relocstate);
                        /* Reserve size for incoming blob */
                        (vm->cmonitor->chunknum)+=recvavilsize;
                        /* Accept */
                        buzzbstig_relocation_bider_add(vm,
                                                         id,
                                                         key,
                                                         recvavilsize,
                                                         (uint32_t)idtoremove,
                                                         (uint32_t)keytoremove,
                                                         BUZZCHUNK_BID_FORCE_ALLOCATION_ACCEPT,
                                                         rid);
                     }
                     else if(v_blob){
                        // printf("Accepting by removing the blob\n");
                        /* Ask everybody to remove the blob */
                        buzzoutmsg_queue_append_chunk_removal(vm,
                                                  BUZZMSG_BSTIG_CHUNK_REMOVED,
                                                  idtoremove,
                                                  keytoremove,
                                                  0,
                                                  BUZZCHUNK_BLOB_REMOVED);
                        struct buzzblob_bidder_s loccmp = {.rid=vm->robot,.availablespace=0};
                        buzzblob_location_t ploccmp = &loccmp;
                        uint16_t locindex = buzzdarray_find((*v_blob)->locations, buzzbstig_blob_bidderelem_key_cmp, &ploccmp);
                        buzzblob_bidder_t locelem = buzzdarray_get((*v_blob)->locations,locindex, buzzblob_location_t);
                        /* Change the size to reflect the new size */
                        // printf("cmon size : %u location element size : %u final size: %u\n", (vm->cmonitor->chunknum),locelem->availablespace,(vm->cmonitor->chunknum)-locelem->availablespace);
                        (vm->cmonitor->chunknum)= (vm->cmonitor->chunknum) - locelem->availablespace;
                        /* This clears all data and ejects the total blob */
                        buzzdict_remove(*s,&(keytoremove));
                        if(MAX_BLOB_CHUNKS - (vm->cmonitor->chunknum) >= recvavilsize){
                           // printf("Accepting because there is space after blob removal\n");
                           const buzzdict_t* news = buzzdict_get(vm->blobs, &(id), buzzdict_t);
                           const buzzblob_elem_t* newv_blob = NULL;
                            if(s){
                              /* Look for blob key in blob bstig slot*/
                              newv_blob = buzzdict_get(*news, &(key), buzzblob_elem_t);
                           }
                           if(newv_blob){
                              if((*newv_blob)->relocstate != BUZZBLOB_SOURCE && (*newv_blob)->relocstate != BUZZBLOB_SINK)
                                 /* Indicate this is a host of this blob */
                                 (*newv_blob)->relocstate = BUZZBLOB_HOST;
                           }
                           /* Reserve size for incoming blob */
                           (vm->cmonitor->chunknum)+=recvavilsize;
                           /* Accept */
                           buzzbstig_relocation_bider_add(vm,
                                                            id,
                                                            key,
                                                            recvavilsize,
                                                            (uint32_t)idtoremove,
                                                            (uint32_t)keytoremove,
                                                            BUZZCHUNK_BID_FORCE_ALLOCATION_ACCEPT,
                                                            rid);
                        }
                        else{
                           // printf("Rejecting because no space exsists after blob removal \n");
                           /* Reject */
                           buzzoutmsg_queue_append_bid(vm,
                                                     BUZZMSG_BSTIG_BLOB_BID,
                                                     id,
                                                     key,
                                                     bidderid,
                                                     recvavilsize,
                                                     (uint32_t)idtoremove,
                                                     (uint32_t)keytoremove,
                                                     0,
                                                     (uint16_t)BUZZCHUNK_BID_FORCE_ALLOCATION_REJECT,
                                                     rid);
                        }
                     }
                     else{
                        // printf("Rejecting because no blob exsists\n");
                        /* Reject */
                        buzzoutmsg_queue_append_bid(vm,
                                                  BUZZMSG_BSTIG_BLOB_BID,
                                                  id,
                                                  key,
                                                  bidderid,
                                                  recvavilsize,
                                                  (uint32_t)idtoremove,
                                                  (uint32_t)keytoremove,
                                                  0,
                                                  (uint16_t)BUZZCHUNK_BID_FORCE_ALLOCATION_REJECT,
                                                  rid);
                     }
                  }
               }
               else{
                  // buzzoutmsg_queue_append_bid(vm,
                  //                              BUZZMSG_BSTIG_BLOB_BID,
                  //                              id,
                  //                              key,
                  //                              bidderid,
                  //                              recvavilsize,
                  //                              (uint32_t)idtoremove,
                  //                              (uint32_t)keytoremove,
                  //                              0,
                  //                              (uint16_t)BUZZCHUNK_BID_FORCE_ALLOCATION);
               }
               break;
            }
            else if(subtype == BUZZCHUNK_BID_FORCE_ALLOCATION_ACCEPT){
               /* Decode bid size */
               uint16_t recvavilsize;
               pos = buzzmsg_deserialize_u16(&recvavilsize, msg, pos);
                // printf("BUZZCHUNK_BID_FORCE_ALLOCATION_ACCEPT bidderid: %u for id: %u key: %u bid size : %u\n", 
                //      bidderid,id,key, recvavilsize );
              
               const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
               /* Nothing to do, if bs id doesnot exsist */
               if(s){
                  /* Look for blob key in blob bstig slot*/
                  const buzzblob_elem_t* v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
                  if(v_blob){
                     /* Did I allocate it */
                     struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZCHUNK_BID_FORCE_ALLOCATION};
                     buzzchunk_reloc_elem_t newelem = &cmpelem;
                     uint16_t cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                     if(cmonindex !=buzzdarray_size(vm->cmonitor->bidder)){
                        const buzzchunk_reloc_elem_t celem = 
                           buzzdarray_get(vm->cmonitor->bidder,cmonindex,buzzchunk_reloc_elem_t);
                        /* Find the allocation inside the monitor */
                        struct buzzblob_bidder_s biddercmp = {.rid = bidderid, .availablespace = recvavilsize};
                        buzzblob_bidder_t belem = &biddercmp;   
                        uint16_t index = buzzdarray_find(celem->checkednids, buzzbstig_blob_bidderelem_key_cmp, &belem);
                        if(index != buzzdarray_size(celem->checkednids)){
                           buzzblob_bidder_t lbid = buzzdarray_get(celem->checkednids,index,buzzblob_bidder_t);
                           // printf("[RID: %u ] Allocating blob id: %u key: %u to : %u, size of aloc : %u\n",vm->robot,id,key,lbid->rid,lbid->availablespace);
                           /* Allocate the chunks of respective size to this robot in P2P Queue */
                           buzzbstig_allocate_chunks(vm,
                                                     id,
                                                     key,
                                                     bidderid,
                                                     lbid->availablespace);
                           /* Remove this allocation from monitor */
                           buzzdarray_remove(celem->checkednids,index);
                        }
                     }
                     else{
                        // buzzoutmsg_queue_append_bid(vm,
                        //                              BUZZMSG_BSTIG_BLOB_BID,
                        //                              id,
                        //                              key,
                        //                              bidderid,
                        //                              recvavilsize,
                        //                              blob_size,
                        //                              hash,
                        //                              0,
                        //                              (uint16_t)subtype);
                        
                     }
                  }
               }
               break;

            }
            else if(subtype == BUZZCHUNK_BID_FORCE_ALLOCATION_REJECT){
               /* Decode the key and id to remove */
               uint16_t keytoremove, idtoremove;
               pos = buzzmsg_deserialize_u16(&idtoremove, msg, pos);
               pos = buzzmsg_deserialize_u16(&keytoremove, msg, pos);
               /* Decode bid size */
               uint16_t recvavilsize;
               pos = buzzmsg_deserialize_u16(&recvavilsize, msg, pos);
               // printf("BUZZCHUNK_BID_FORCE_ALLOCATION_REJECT bidderid: %u for id: %u key: %u idto remove: %u key toremove: %u bid size : %u\n", 
               //       bidderid,id,key, idtoremove, keytoremove, recvavilsize );
               const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
               /* Nothing to do, if bs id doesnot exsist */
               if(s){
                  /* Look for blob key in blob bstig slot*/
                  const buzzblob_elem_t* v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
                  if(v_blob){
                     /* Did I allocate it */
                     struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZCHUNK_BID_FORCE_ALLOCATION};
                     buzzchunk_reloc_elem_t newelem = &cmpelem;
                     uint16_t cmonindex = buzzdarray_find(vm->cmonitor->bidder,buzzvm_cmonitor_reloc_elem_key_cmp,&newelem);
                     if(cmonindex !=buzzdarray_size(vm->cmonitor->bidder)){
                        const buzzchunk_reloc_elem_t allocationelem = 
                           buzzdarray_get(vm->cmonitor->bidder,cmonindex,buzzchunk_reloc_elem_t);
                           /* Give it more time */
                           allocationelem->time_to_wait = MAX_TIME_TO_HOLD_SPACE_FOR_A_BID;
                        /* Create the priority list */
                        buzzdarray_t priority = buzzbstig_get_prioritylist(vm);
                        /* Find the allocation inside the monitor */
                        struct buzzblob_bidder_s biddercmp = {.rid = bidderid, .availablespace = recvavilsize};
                        buzzblob_bidder_t belem = &biddercmp;   
                        uint16_t index = buzzdarray_find(allocationelem->checkednids, buzzbstig_blob_bidderelem_key_cmp, &belem);
                        if(index != buzzdarray_size(allocationelem->checkednids)){
                           /* Find the requested remove element from priority list */
                           struct buzzbstig_blobpriority_list_param prioritycmp = {.id=allocationelem->bidsize,.key=allocationelem->time_to_destroy};

                           struct buzzbstig_blobpriority_list_param* pprioritycmp = &prioritycmp;
                           cmonindex = buzzdarray_find(priority,buzzbstig_priority_cmp,&pprioritycmp);
                           uint16_t priorityresume=0;
                           if(cmonindex ==buzzdarray_size(priority)){
                              /* Element not found in the priority list */
                              cmonindex=0;
                           }   
                           else{
                              /* Element found in prioty resume */
                              priorityresume =1;
                           }
                           uint16_t allocsize=0;
                           for(uint16_t pi=cmonindex; pi < buzzdarray_size(priority) && 
                              allocsize < recvavilsize; pi++){
                              const struct buzzbstig_blobpriority_list_param* pe = buzzdarray_get(priority, pi, struct buzzbstig_blobpriority_list_param*);
                              /* Fetch the entries in order form priority list */
                              const buzzdict_t* prs = buzzdict_get(vm->blobs, &(pe->id), buzzdict_t);
                              if(prs){
                                 /* Look for blob key in blob bstig slot*/
                                 const buzzblob_elem_t* pv_blob = buzzdict_get(*prs, &(pe->key), buzzblob_elem_t);
                                 if(pv_blob){
                                    if(pi==cmonindex && priorityresume ){
                                       /* Find the last element in the allocation list */
                                       buzzblob_bidder_t lastalocrobot = 
                                                buzzdarray_last(allocationelem->checkednids,buzzblob_bidder_t);
                                       uint16_t tmpindex = buzzdarray_find((*pv_blob)->locations,buzzdict_uint16keycmp,&(lastalocrobot->rid));
                                       for(int l=tmpindex+1;l<buzzdarray_size((*pv_blob)->locations) && 
                                             allocsize < recvavilsize; l++){
                                          /* Get the first location element of lowest priority blob */
                                          buzzblob_location_t lowloc = buzzdarray_get((*pv_blob)->locations,l, buzzblob_location_t);
                                          /* Allocate if I am not allocating to myself */
                                          if(lowloc->rid != vm->robot){ 
                                             /* Force allocate the chunk */
                                             uint16_t allocated_size = (allocsize + lowloc->availablespace < recvavilsize) ? lowloc->availablespace : recvavilsize - allocsize;  
                                             buzzblob_bidder_t addbider = (buzzblob_bidder_t)malloc(sizeof(struct buzzblob_bidder_s));
                                             addbider->rid = lowloc->rid;
                                             addbider->role = 0;
                                             addbider->availablespace = allocated_size;
                                             /* remove the rejected robots entry */
                                             buzzdarray_remove(allocationelem->checkednids,index);
                                             /* Add the size to allocated size */
                                             allocsize+=allocated_size;
                                             buzzdarray_push(allocationelem->checkednids, &(addbider) );
                                             /* Send a force allocation messsage */
                                             buzzoutmsg_queue_append_bid(vm,
                                                                          BUZZMSG_BSTIG_BLOB_BID,
                                                                          allocationelem->id,
                                                                          allocationelem->key,
                                                                          lowloc->rid,
                                                                          allocated_size,
                                                                          pe->id,
                                                                          pe->key,
                                                                          0,
                                                                          (uint16_t) BUZZCHUNK_BID_FORCE_ALLOCATION,
                                                                          lowloc->rid);
                                          }                                       
                                       }
                                    }
                                    else{
                                       for(int l=0;l<buzzdarray_size((*pv_blob)->locations) && 
                                             allocsize < recvavilsize; l++){
                                          /* Get the first location element of lowest priority blob */
                                          buzzblob_location_t lowloc = buzzdarray_get((*pv_blob)->locations,l, buzzblob_location_t);
                                          /* Allocate if I am not allocating to myself */
                                          if(lowloc->rid != vm->robot){
                                             /* Force allocate the chunk */
                                             uint16_t allocated_size = (allocsize + lowloc->availablespace < recvavilsize) ? lowloc->availablespace : recvavilsize - allocsize;  
                                             buzzblob_bidder_t addbider = (buzzblob_bidder_t)malloc(sizeof(struct buzzblob_bidder_s));
                                             addbider->rid = lowloc->rid;
                                             addbider->role = 0;
                                             addbider->availablespace = allocated_size;
                                             /* remove the rejected robots entry */
                                             buzzdarray_remove(allocationelem->checkednids,index);
                                             /* Add the size to allocated size */
                                             allocsize+=allocated_size;
                                             buzzdarray_push(allocationelem->checkednids, &(addbider) );
                                             /* Send a force allocation messsage */
                                             buzzoutmsg_queue_append_bid(vm,
                                                                          BUZZMSG_BSTIG_BLOB_BID,
                                                                          allocationelem->id,
                                                                          allocationelem->key,
                                                                          lowloc->rid,
                                                                          allocated_size,
                                                                          pe->id,
                                                                          pe->key,
                                                                          0,
                                                                          (uint16_t) BUZZCHUNK_BID_FORCE_ALLOCATION,
                                                                          lowloc->rid);
                                          }
                                       }
                                    }
                                 }
                              }
                              allocationelem->bidsize = pe->id;  // temp hold the last id 
                              allocationelem->time_to_destroy = pe->key; 
                           }
                           if(allocsize < recvavilsize){
                              /* Could not allocate all chunks, intiate the priority policy and remove the oldest blob */
                              printf("[RID: %u] ERROR, BUZZCHUNK_BID_FORCE_ALLOCATION_REJECT Trying to force allocate but no blob exsist to remove, try increasing available size\n",vm->robot );
                           }
                           
                        }
                        buzzdarray_destroy(&priority); 
                     }
                     else{ // This robot did not allocate 
                        // buzzoutmsg_queue_append_bid(vm,
                        //                              BUZZMSG_BSTIG_BLOB_BID,
                        //                              id,
                        //                              key,
                        //                              bidderid,
                        //                              recvavilsize,
                        //                              (uint32_t)idtoremove,
                        //                              (uint32_t)keytoremove,
                        //                              0,
                        //                              (uint16_t)subtype);
                     }
                  }
               }
               break;
            }
            case BUZZMSG_BSTIG_BLOB_REQUEST:{
               uint16_t id,key,sender;
               int64_t pos = buzzmsg_deserialize_u16(&id, msg, 1);
               pos = buzzmsg_deserialize_u16(&key, msg, pos);
               pos = buzzmsg_deserialize_u16(&sender, msg, pos);
               // printf("RID: %u  rid sender %u received BUZZMSG_BSTIG_BLOB_REQUEST id %u k %u sender %u \n",vm->robot,
               // rid, id, key, sender );
               /* Find whether you have all the chunks */
               /* Look for blob holder and create a new if none exsists */
               const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
               const buzzblob_elem_t* v_blob = NULL;
               /*Nothing to do, if bs id doesnot exsist*/
               if(s){
                  /* Look for blob key in blob bstig slot*/
                  v_blob = buzzdict_get(*s, &key, buzzblob_elem_t);
                  if(v_blob){   
                     uint16_t available_size=0;
                     for(int i=0;i<buzzdarray_size((*v_blob)->locations);i++){
                        /* Get thelocation elements and calculate the space */
                        buzzblob_location_t lowloc = buzzdarray_get((*v_blob)->locations,i, buzzblob_location_t);
                        if(lowloc->rid == vm->robot){
                           available_size = lowloc->availablespace;
                        }                  
                     }
                     // printf("avilable list size : %i avilable size %u \n", buzzdarray_size((*v_blob)->available_list), available_size);
                     /* All chunks are avilable unicast it */
                     if(buzzdarray_size((*v_blob)->available_list) >= available_size){
                         /* Create an element for key */
                        buzzobj_t k = buzzobj_new(BUZZTYPE_INT);  // TODO : try to unify args and hence avoid creation of buzzobj
                        k->i.value = key;
                        /* Look for virtual stigmergy */
                        const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
                        /* Fetch the element */
                        const buzzbstig_elem_t* l = buzzbstig_fetch(*vs, &k);
                        for(int i=0;i < buzzdarray_size((*v_blob)->available_list);i++){
                           uint16_t cid = buzzdarray_get((*v_blob)->available_list,i,uint16_t);
                           const buzzblob_chunk_t* cdata = buzzdict_get((*v_blob)->data, &cid, buzzblob_chunk_t);

                           // printf("unicasting id: %u k: %u on request from %u chunk: %i , cid : %u\n",id, key, sender, i,cid  );
                           /* Add to P2P Queue */
                           buzzoutmsg_queue_append_chunk(vm,
                                                         BUZZMSG_BSTIG_CHUNK_PUT,
                                                         id,
                                                         k,
                                                         *l,
                                                         (*v_blob)->size,
                                                         cid,
                                                         *cdata,
                                                         sender);
                        }
                        buzzobj_destroy(&k);
                     }
                     else{
                        // printf("[ROBOT %u] id %u k %u requester %u not all chunks avilable add to blob request\n",vm->robot,id,key,sender);
                        /* Not all chunks are avilable add request and wait */
                        struct buzzchunk_reloc_elem_s cmpelem = {.id = id, .key = key, .cid=BUZZBLOB_WAITING_FOR_BLOB_AVILABILITY, .bidsize=sender};
                        buzzchunk_reloc_elem_t newelem = &cmpelem;
                        uint16_t cmonindex = buzzdarray_find(vm->cmonitor->blobrequest,buzzvm_cmonitor_blobrequest_requester_key_cmp,&newelem);
                        if(cmonindex == buzzdarray_size(vm->cmonitor->blobrequest)){
                           /* Add an elemnt in refersher to monitor and maintain when robots are moving */
                           buzzchunk_reloc_elem_t newelem =(buzzchunk_reloc_elem_t)malloc(sizeof(struct buzzchunk_reloc_elem_s));
                           newelem->id = id;
                           newelem->key = key;
                           newelem->cid = BUZZBLOB_WAITING_FOR_BLOB_AVILABILITY;
                           newelem->bidsize = sender; 
                           newelem->time_to_wait = TIME_TO_REFRESH_GETTER_STATE;
                           newelem->time_to_destroy = 0;
                           newelem->checkednids = buzzdarray_new(1,sizeof(buzzblob_bidder_t),buzzbstig_blob_bidderelem_destroy);
                           buzzdarray_push(vm->cmonitor->blobrequest,&newelem);
                        }

                     }
                  }
               }
               break;
            }   

         }

      }
      /* Get rid of the message */
      buzzmsg_payload_destroy(&msg);
   }
   /* Update swarm membership */
   buzzswarm_members_update(vm->swarmmembers);
   /* Update neighbor table */
   buzzvm_neighbors_update(vm);
   /* Update blob status */
   buzzbstig_blobstatus_update(vm);
   /* Update blob getters */
   buzzbstig_blobgetters_update(vm);
   /* Update blob requests */
   buzzbstig_blobrequest_update(vm);
   /* Update outmsg anti-flooding checkers */
   buzzoutmsg_update_antiflooding_entry(vm);
   /* Refresh chunk stigs */
   //buzzbstig_chunkstig_update(vm);
}

/****************************************/
/****************************************/
void buzzvm_darray_testprint(uint32_t pos, void* data, void* params){
   if(params){
      buzzblob_location_t e = *(buzzblob_location_t*) data;
      printf("%u->%u,",e->rid,e->availablespace);  
   }
   else
      printf("%u,",*(uint16_t*) data);
}
void buzzvm_neighbors_test_print_loop1(const void* key, void* data, void* params){
// printf(" Key : %u  Size : %i ",*(uint16_t*) ((uint16_t*)key), buzzdarray_size( *(buzzdarray_t*)((buzzdarray_t*)data)) );
}
void buzzvm_neighbors_test_print_loop(const void* key, void* data, void* params){
printf(" Bstig id: %u : ", *(uint16_t*) ((uint16_t*)key) );
buzzdict_foreach(*(buzzdict_t*)((buzzdict_t*)data), buzzvm_neighbors_test_print_loop1, NULL);

}
void buzzvm_neighbors_loop(const void* key, void* data, void* params){
   buzzneighbour_chunk_t n = *(buzzneighbour_chunk_t*) data;
   buzzdarray_t n_to_remove = *(buzzdarray_t*) params;
   uint16_t rid = *(uint16_t*) key;
   //buzzneighbour_chunk_t cc= *(buzzneighbour_chunk_t*) data;
   // printf(" NID: %u, [ ", rid);
   // buzzdict_foreach(cc->chunks_on, buzzvm_neighbors_test_print_loop, NULL);
   // printf(" ]" );
   (n->timetoforget)--;
   if(n->timetoforget<=0){
       // printf("(removed)");
     buzzdarray_push(n_to_remove,&rid); 
   } 
}

/****************************************/
/****************************************/

void buzzvm_neighbors_update(buzzvm_t vm){
   buzzdarray_t n_to_remove = buzzdarray_new(10,
                                             sizeof(uint16_t),
                                             NULL);
   //printf("[DEBUG] neighbor of rid : %u",vm->robot);
   buzzdict_foreach(vm->active_neighbors, buzzvm_neighbors_loop, &n_to_remove);
   //printf("\n");
   for(int i=0; i< buzzdarray_size(n_to_remove); i++){
      uint16_t rid = buzzdarray_get(n_to_remove,i,uint16_t);
      buzzdict_remove(vm->active_neighbors,&rid);
   }
   if(vm->nchange !=0) vm->nchange--;
   buzzdarray_destroy(&n_to_remove);
}

/****************************************/
/****************************************/

void buzzvm_gsyms_test_print(const void* key, void* data, void* params){
   buzzvm_t vm = *(buzzvm_t*)params;
   uint16_t sid = *(uint16_t*)key;
   const char* keystring = buzzstrman_get(vm->strings, sid);
   buzzobj_t* op  = (buzzobj_t*)data;
   buzzobj_t o = *(buzzobj_t*) op;
   const char* id_Str = "id";
   if(!strcmp(keystring,id_Str)) printf("\t ID string");
   if(o->o.type == BUZZTYPE_INT){
      printf("\t [%u] BUZZTYPE_INT : %s -> %u \n", sid,keystring,o->i.value);
   }
   else if(o->o.type == BUZZTYPE_FLOAT){
      printf("\t [%u] BUZZTYPE_FLOAT : %s -> %f \n", sid,keystring,o->f.value);
   }
   else if(o->o.type == BUZZTYPE_STRING){
      printf("\t [%u] BUZZTYPE_STRING : %s -> %s \n", sid,keystring,o->s.value.str);
   }
   else if(o->o.type == BUZZTYPE_TABLE){
      printf("\t [%u] BUZZTYPE_TABLE : %s -> [table] \n", sid,keystring);
   }
   else if(o->o.type == BUZZTYPE_CLOSURE){
      printf("\t [%u] BUZZTYPE_CLOSURE : %s -> [CLOSURE] \n", sid,keystring);
   }
   else if(o->o.type == BUZZTYPE_USERDATA){
      printf("\t [%u] BUZZTYPE_USERDATA : %s -> [USERDATA] \n", sid,keystring);
   }
   else if(o->o.type == BUZZTYPE_NIL){
      printf("\t [%u] BUZZTYPE_NIL : %s -> [NIL] \n", sid,keystring);
   }

}

/****************************************/
/****************************************/

void buzzvm_process_outmsgs(buzzvm_t vm) {
   // uint16_t id =1;
   // const buzzbstig_t* vs = buzzdict_get(vm->bstigs, &id, buzzbstig_t);
   // if(vs){
   // uint16_t vs_size = buzzdict_size((*vs)->data);
   // printf("[RID: %u]Size of chunk stig : %d \n", vm->robot,vs_size);
   // }
   // printf("[RID : %u] Size of outmsg : %i, bid: %i, cput: %i, cp2p queue: %i \n",vm->robot, (int)buzzoutmsg_queue_size(vm),(int)buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_BLOB_BID]),
   //    (int)buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT]),
   //    (int)buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P]));
   // uint16_t id = 100, k = 3;
   // const buzzdict_t* s = buzzdict_get(vm->blobs, &id, buzzdict_t);
   // if(s){
   //    /* Look for blob key in blob bstig slot*/
   //    const buzzblob_elem_t* v_blob = buzzdict_get(*s, &k, buzzblob_elem_t);
   //    if(v_blob){
   //       uint32_t available_chunk = buzzdict_size((*v_blob)->available_list);
   //       uint32_t locations = buzzdarray_size((*v_blob)->locations);
   //       // uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
   //       printf("[RID: %u] [ B 1]Size of avialable chunk :  %u / %u  , location list size :  %u  ", vm->robot, (unsigned int)available_chunk, (unsigned int)(vm->cmonitor->chunknum), (unsigned int)locations);
   //       if((*v_blob)->status == BUZZBLOB_READY) printf("BLOB STATE: BUZZBLOB_READY       ");
   //       if((*v_blob)->status == BUZZBLOB_BUFFERING) printf("BLOB STATE: BUZZBLOB_BUFFERING       ");
   //       if((*v_blob)->status == BUZZBLOB_BUFFERED) printf("BLOB STATE: BUZZBLOB_BUFFERED         ");

   //       if(vm->cmonitor->status == BUZZCMONITOR_FREE) printf("CMON STATE: BUZZCMONITOR_FREE       ");
   //       if(vm->cmonitor->status == BUZZCMONITOR_RELOCATING) printf("CMON STATE: BUZZCMONITOR_RELOCATING       ");
   //       if((*v_blob)->relocstate == BUZZBLOB_OPEN) printf("BLOB RELOC STATE: BUZZBLOB_OPEN\n");
   //       if((*v_blob)->relocstate == BUZZBLOB_SINK) printf("BLOB RELOC STATE: BUZZBLOB_SINK\n");
   //       if((*v_blob)->relocstate == BUZZBLOB_HOST) printf("BLOB RELOC STATE: BUZZBLOB_HOST\n");
   //       if((*v_blob)->relocstate == BUZZBLOB_HOST_FORWARDING) printf("BLOB RELOC STATE: BUZZBLOB_HOST_FORWARDING\n");
         
   //       if((*v_blob)->relocstate == BUZZBLOB_SOURCE) printf("BLOB RELOC STATE: BUZZBLOB_SOURCE\n");
   //       if((*v_blob)->relocstate == BUZZBLOB_FORWARDING) printf("BLOB RELOC STATE: BUZZBLOB_FORWARDING\n");
   //       printf("[RID: %u ][ B 1] available_list [ ",vm->robot );
   //       buzzdarray_foreach((*v_blob)->available_list,buzzvm_darray_testprint,NULL);
   //       printf(" ]\n");
   //       printf("[RID: %u ][ B 1] locations [ ", vm->robot);
   //       uint16_t loc = 1;
   //       buzzdarray_foreach((*v_blob)->locations,buzzvm_darray_testprint,&loc);
   //       printf(" ]\n");
   //    }
   // }
   //    if(1){
   //       k=9;
   //       /* Look for blob key in blob bstig slot*/
   //       const buzzblob_elem_t* v_blob = buzzdict_get(*s, &k, buzzblob_elem_t);
   //       if(v_blob){
   //          uint32_t available_chunk = buzzdict_size((*v_blob)->available_list);
   //          uint32_t locations = buzzdarray_size((*v_blob)->locations);
   //          // uint16_t chunk_num = ceil( (float)(*v_blob)->size/(float)BLOB_CHUNK_SIZE);
   //          printf("[RID: %u] [ B 10]Size of avialable chunk :  %d / %u  , location list size :  %u  ", vm->robot,
   //             available_chunk, (vm->cmonitor->chunknum), locations);
   //          if((*v_blob)->status == BUZZBLOB_READY) printf("BLOB STATE: BUZZBLOB_READY       ");
   //          if((*v_blob)->status == BUZZBLOB_BUFFERING) printf("BLOB STATE: BUZZBLOB_BUFFERING       ");
   //          if((*v_blob)->status == BUZZBLOB_BUFFERED) printf("BLOB STATE: BUZZBLOB_BUFFERED         ");

   //          if(vm->cmonitor->status == BUZZCMONITOR_FREE) printf("CMON STATE: BUZZCMONITOR_FREE       ");
   //          if(vm->cmonitor->status == BUZZCMONITOR_RELOCATING) printf("CMON STATE: BUZZCMONITOR_RELOCATING       ");
   //          if((*v_blob)->relocstate == BUZZBLOB_OPEN) printf("BLOB RELOC STATE: BUZZBLOB_OPEN\n");
   //          if((*v_blob)->relocstate == BUZZBLOB_SINK) printf("BLOB RELOC STATE: BUZZBLOB_SINK\n");
   //          if((*v_blob)->relocstate == BUZZBLOB_HOST) printf("BLOB RELOC STATE: BUZZBLOB_HOST\n");
   //          if((*v_blob)->relocstate == BUZZBLOB_HOST_FORWARDING) printf("BLOB RELOC STATE: BUZZBLOB_HOST_FORWARDING\n");
            
   //          if((*v_blob)->relocstate == BUZZBLOB_SOURCE) printf("BLOB RELOC STATE: BUZZBLOB_SOURCE\n");
   //          if((*v_blob)->relocstate == BUZZBLOB_FORWARDING) printf("BLOB RELOC STATE: BUZZBLOB_FORWARDING\n");
   //          printf("[RID: %u ][ B 10] available_list [ ",vm->robot );
   //          buzzdarray_foreach((*v_blob)->available_list,buzzvm_darray_testprint,NULL);
   //          printf(" ]\n");
   //          printf("[RID: %u ][ B 10] locations [ ", vm->robot);
   //          uint16_t loc = 1;
   //          buzzdarray_foreach((*v_blob)->locations,buzzvm_darray_testprint,&loc);
   //          printf(" ]\n");
   //       }
   //    }
   // }
   /* Must broadcast swarm list message? */
   if(vm->swarmbroadcast > 0)
      --vm->swarmbroadcast;
   if(vm->swarmbroadcast == 0 &&
      !buzzdict_isempty(vm->swarms)) {
      vm->swarmbroadcast = SWARM_BROADCAST_PERIOD;
      buzzoutmsg_queue_append_swarm_list(vm,
                                         vm->swarms);
   }
}

/****************************************/
/****************************************/

void buzzvm_darray_destroy(uint32_t pos,
                           void* data,
                           void* params) {
   buzzdarray_t* s = (buzzdarray_t*)data;
   buzzdarray_destroy(s);
}

buzzvm_t buzzvm_new(uint16_t robot) {
   /* Create VM state. calloc() takes care of zeroing everything */
   buzzvm_t vm = (buzzvm_t)calloc(1, sizeof(struct buzzvm_s));
   /* Create stacks */
   vm->stacks = buzzdarray_new(BUZZVM_STACKS_INIT_CAPACITY,
                               sizeof(buzzdarray_t),
                               buzzvm_darray_destroy);
   vm->stack = buzzdarray_new(BUZZVM_STACK_INIT_CAPACITY,
                              sizeof(buzzobj_t),
                              NULL);
   buzzdarray_push(vm->stacks, &(vm->stack));
   /* Create local variable tables */
   vm->lsymts = buzzdarray_new(BUZZVM_LSYMTS_INIT_CAPACITY,
                               sizeof(buzzvm_lsyms_t),
                               buzzvm_lsyms_destroy);
   vm->lsyms = NULL;
   /* Create global variable tables */
   vm->gsyms = buzzdict_new(BUZZVM_SYMS_INIT_CAPACITY,
                            sizeof(int32_t),
                            sizeof(buzzobj_t),
                            buzzdict_int32keyhash,
                            buzzdict_int32keycmp,
                            NULL);
   /* Create string list */
   vm->strings = buzzstrman_new();
   /* Create heap */
   vm->heap = buzzheap_new();
   /* Create function list */
   vm->flist = buzzdarray_new(20, sizeof(buzzvm_funp), NULL);
   /* Create swarm list */
   vm->swarms = buzzdict_new(10,
                             sizeof(uint16_t),
                             sizeof(uint8_t),
                             buzzdict_uint16keyhash,
                             buzzdict_uint16keycmp,
                             NULL);
   /* Create swarm stack */
   vm->swarmstack = buzzdarray_new(10,
                                   sizeof(uint16_t),
                                   NULL);
   /* Create swarm member structure */
   vm->swarmmembers = buzzswarm_members_new();
   vm->swarmbroadcast = SWARM_BROADCAST_PERIOD;
   /* Create message queues */
   vm->inmsgs = buzzinmsg_queue_new();
   vm->outmsgs = buzzoutmsg_queue_new(robot);
   /* Create virtual stigmergy */
   vm->vstigs = buzzdict_new(10,
                             sizeof(uint16_t),
                             sizeof(buzzvstig_t),
                             buzzdict_uint16keyhash,
                             buzzdict_uint16keycmp,
                             buzzvm_vstig_destroy);
   /* Create active neighbors dictionary */
   vm->active_neighbors = buzzdict_new(10,
                                sizeof(uint16_t),
                                sizeof(buzzneighbour_chunk_t),
                                buzzdict_uint16keyhash,
                                buzzdict_uint16keycmp,
                                buzzvm_neighbors_destroy);
   /* Create chunk monitor */
   vm->cmonitor = (buzzchunk_monitor_t)calloc(1,sizeof(struct buzzchunk_monitor_s));
   /* Create dictionay for relocation management */
   vm->cmonitor->blobrequest = buzzdarray_new(10, 
                                 sizeof(buzzchunk_reloc_elem_t),
                                 buzzvm_cmonitor_chunks_destroy);
   /* Create dictionay for lost chunk management */
   vm->cmonitor->getters = buzzdarray_new(10, 
                                 sizeof(buzzchunk_reloc_elem_t),
                                 buzzvm_cmonitor_chunks_destroy);
   /* Create dictionay for lost chunk management */
   vm->cmonitor->bidder = buzzdarray_new(10, 
                                 sizeof(buzzchunk_reloc_elem_t),
                                 buzzvm_cmonitor_chunks_destroy);
   /* Create virtual stigmergy */
   vm->listeners = buzzdict_new(10,
                                sizeof(uint16_t),
                                sizeof(buzzobj_t),
                                buzzdict_uint16keyhash,
                                buzzdict_uint16keycmp,
                                NULL);
   /* Create blob stigmergy */
   vm->bstigs = buzzdict_new(10,
                             sizeof(uint16_t),
                             sizeof(buzzbstig_t),
                             buzzdict_uint16keyhash,
                             buzzdict_uint16keycmp,
                             buzzvm_bstig_destroy);
   /* Create blob stigmergy */
   vm->blobs = buzzdict_new(10,
                             sizeof(uint16_t),
                             sizeof(buzzdict_t),
                             buzzdict_uint16keyhash,
                             buzzdict_uint16keycmp,
                             buzzvm_blobs_destroy);
   /* Create Chunk stigmergy list holder */
   vm->chunk_stig = buzzdarray_new(10, 
                                 sizeof(uint16_t),
                                 NULL);
   /* Take care of the robot id */
   vm->robot = robot;
   /* Initialize empty random number generator (buzzvm_math takes care of creating it) */
   vm->rngstate = NULL;
   vm->rngidx = 0;
   vm->nchange = 0;
   /* Return new vm */
   return vm;
}

/****************************************/
/****************************************/

void buzzvm_destroy(buzzvm_t* vm) {
   /* Get rid of the rng state */
   free((*vm)->rngstate);
   /* Get rid of the stack */
   buzzstrman_destroy(&(*vm)->strings);
   /* Get rid of the global variable table */
   buzzdict_destroy(&(*vm)->gsyms);
   /* Get rid of the local variable tables */
   buzzdarray_destroy(&(*vm)->lsymts);
   /* Get rid of the stack */
   buzzdarray_destroy(&(*vm)->stacks);
   /* Get rid of the heap */
   buzzheap_destroy(&(*vm)->heap);
   /* Get rid of the function list */
   buzzdarray_destroy(&(*vm)->flist);
   /* Get rid of the swarm list */
   buzzdict_destroy(&(*vm)->swarms);
   buzzdarray_destroy(&(*vm)->swarmstack);
   buzzswarm_members_destroy(&((*vm)->swarmmembers));
   /* Get rid of the message queues */
   buzzinmsg_queue_destroy(&(*vm)->inmsgs);
   buzzoutmsg_queue_destroy(&(*vm)->outmsgs);
   /* Get rid of the virtual stigmergy structures */
   buzzdict_destroy(&(*vm)->vstigs);
   /* Get rid of the blob stigmergy structures */
   buzzdict_destroy(&(*vm)->bstigs);
   /* Get rid of the blob  structures */
   buzzdict_destroy(&(*vm)->blobs);
   buzzdarray_destroy(&(*vm)->chunk_stig);
   /* Get rid of neighbor value listeners */
   buzzdict_destroy(&(*vm)->listeners);
   buzzdarray_destroy(&((*vm)->cmonitor->blobrequest));
   buzzdarray_destroy(&((*vm)->cmonitor->getters));
   buzzdarray_destroy(&((*vm)->cmonitor->bidder));
   free((*vm)->cmonitor);
   free(*vm);
   *vm = 0;
}

/****************************************/
/****************************************/

void buzzvm_seterror(buzzvm_t vm,
                     buzzvm_error errcode,
                     const char* errmsg,
                     ...) {
   /* Set error state */
   vm->state = BUZZVM_STATE_ERROR;
   vm->error = errcode;
   /* Get rid of old error message */
   if(vm->errormsg) free(vm->errormsg);
   /* Was a custom message passed? */
   if(errmsg) {
      /* Yes, use it */
      /* Compose user-defined error message */
      char* msg;
      va_list al;
      va_start(al, errmsg);
      vasprintf(&msg, errmsg, al);
      va_end(al);
      /* Concatenate error description and user defined message */
      asprintf(&vm->errormsg,
               "%s: %s",
               buzzvm_error_desc[vm->error],
               msg);
      /* Get rid of user-defined error message */
      free(msg);
   }
   else {
      /* No, use the default error description */
      vm->errormsg = strdup(buzzvm_error_desc[vm->error]);
   }
}

/****************************************/
/****************************************/

int buzzvm_set_bcode(buzzvm_t vm,
                     const uint8_t* bcode,
                     uint32_t bcode_size) {
   /* Fetch the string count */
   uint16_t count;
   memcpy(&count, bcode, sizeof(uint16_t));
   /* Go through the strings and store them */
   uint32_t i = sizeof(uint16_t);
   long int c = 0;
   for(; (c < count) && (i < bcode_size); ++c) {
      /* Store string */
      buzzvm_string_register(vm, (char*)(bcode + i), 1);
      /* Advance to first character of next string */
      while(*(bcode + i) != 0) ++i;
      ++i;
   }
   /* Initialize VM state */
   vm->state = BUZZVM_STATE_READY;
   vm->error = BUZZVM_ERROR_NONE;
   /* Initialize bytecode data */
   vm->bcode_size = bcode_size;
   vm->bcode = bcode;
   /* Set program counter */
   vm->pc = i;
   vm->oldpc = vm->pc;
   /*
    * Register function definitions
    * Stop when you find a 'nop'
    */
   while(vm->bcode[vm->pc] != BUZZVM_INSTR_NOP)
      if(buzzvm_step(vm) != BUZZVM_STATE_READY) return vm->state;
   buzzvm_step(vm);
   /* Initialize empty neighbors */
   buzzneighbors_reset(vm);
   /* Register robot id */
   buzzvm_pushs(vm, buzzvm_string_register(vm, "id", 1));
   buzzvm_pushi(vm, vm->robot);
   buzzvm_gstore(vm);
   /* Register basic functions */
   buzzobj_register(vm);
   /* Register stigmergy methods */
   buzzvstig_register(vm);
   /* Register stigmergy methods */
   buzzbstig_register(vm);
   /* Register swarm methods */
   buzzswarm_register(vm);
   /* Register math methods */
   buzzmath_register(vm);
   /* Register io methods */
   buzzio_register(vm);
   /* Register string methods */
   buzzstring_register(vm);
   /* All done */
   return BUZZVM_STATE_READY;
}

/****************************************/
/****************************************/

#define assert_pc(IDX) if((IDX) < 0 || (IDX) >= vm->bcode_size) { buzzvm_seterror(vm, BUZZVM_ERROR_PC, NULL); return vm->state; }

#define inc_pc() vm->oldpc = vm->pc; ++vm->pc; assert_pc(vm->pc);

#define get_arg(TYPE) assert_pc(vm->pc + sizeof(TYPE)); TYPE arg = *((TYPE*)(vm->bcode + vm->pc)); vm->pc += sizeof(TYPE);

buzzvm_state buzzvm_step(buzzvm_t vm) {
   /* buzzvm_dump(vm); */
   /* Can't execute if not ready */
   if(vm->state != BUZZVM_STATE_READY) return vm->state;
   /* Execute GC */
   buzzheap_gc(vm);
   /* Fetch instruction and (potential) argument */
   uint8_t instr = vm->bcode[vm->pc];
   /* Execute instruction */
   switch(instr) {
      case BUZZVM_INSTR_NOP: {
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_DONE: {
         buzzvm_done(vm);
         break;
      }
      case BUZZVM_INSTR_PUSHNIL: {
         inc_pc();
         buzzvm_pushnil(vm);
         break;
      }
      case BUZZVM_INSTR_DUP: {
         inc_pc();
         buzzvm_dup(vm);
         break;
      }
      case BUZZVM_INSTR_POP: {
         if(buzzvm_pop(vm) != BUZZVM_STATE_READY) return vm->state;
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_RET0: {
         if(buzzvm_ret0(vm) != BUZZVM_STATE_READY) return vm->state;
         assert_pc(vm->pc);
         break;
      }
      case BUZZVM_INSTR_RET1: {
         if(buzzvm_ret1(vm) != BUZZVM_STATE_READY) return vm->state;
         assert_pc(vm->pc);
         break;
      }
      case BUZZVM_INSTR_ADD: {
         buzzvm_add(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_SUB: {
         buzzvm_sub(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_MUL: {
         buzzvm_mul(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_DIV: {
         buzzvm_div(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_MOD: {
         buzzvm_mod(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_POW: {
         buzzvm_pow(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_UNM: {
         buzzvm_unm(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_AND: {
         buzzvm_and(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_OR: {
         buzzvm_or(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_NOT: {
         buzzvm_not(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_EQ: {
         buzzvm_eq(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_NEQ: {
         buzzvm_neq(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_GT: {
         buzzvm_gt(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_GTE: {
         buzzvm_gte(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_LT: {
         buzzvm_lt(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_LTE: {
         buzzvm_lte(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_GLOAD: {
         inc_pc();
         buzzvm_gload(vm);
         break;
      }
      case BUZZVM_INSTR_GSTORE: {
         inc_pc();
         if(buzzvm_gstore(vm) != BUZZVM_STATE_READY) return vm->state;
         break;
      }
      case BUZZVM_INSTR_PUSHT: {
         buzzvm_pusht(vm);
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_TPUT: {
         if(buzzvm_tput(vm) != BUZZVM_STATE_READY) return vm->state;
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_TGET: {
         if(buzzvm_tget(vm) != BUZZVM_STATE_READY) return vm->state;
         inc_pc();
         break;
      }
      case BUZZVM_INSTR_CALLC: {
         inc_pc();
         if(buzzvm_callc(vm) != BUZZVM_STATE_READY) return vm->state;
         assert_pc(vm->pc);
         break;
      }
      case BUZZVM_INSTR_CALLS: {
         inc_pc();
         if(buzzvm_calls(vm) != BUZZVM_STATE_READY) return vm->state;
         assert_pc(vm->pc);
         break;
      }
      case BUZZVM_INSTR_PUSHF: {
         inc_pc();
         get_arg(float);
         if(buzzvm_pushf(vm, arg) != BUZZVM_STATE_READY) return vm->state;
         break;
      }
      case BUZZVM_INSTR_PUSHI: {
         inc_pc();
         get_arg(int32_t);
         if(buzzvm_pushi(vm, arg) != BUZZVM_STATE_READY) return vm->state;
         break;
      }
      case BUZZVM_INSTR_PUSHS: {
         inc_pc();
         get_arg(int32_t);
         if(buzzvm_pushs(vm, arg) != BUZZVM_STATE_READY) return vm->state;
         break;
      }
      case BUZZVM_INSTR_PUSHCN: {
         inc_pc();
         get_arg(uint32_t);
         if(buzzvm_pushcn(vm, arg) != BUZZVM_STATE_READY) return vm->state;
         break;
      }
      case BUZZVM_INSTR_PUSHCC: {
         inc_pc();
         get_arg(uint32_t);
         if(buzzvm_pushcc(vm, arg) != BUZZVM_STATE_READY) return vm->state;
         break;
      }
      case BUZZVM_INSTR_PUSHL: {
         inc_pc();
         get_arg(uint32_t);
         if(buzzvm_pushl(vm, arg) != BUZZVM_STATE_READY) return vm->state;
         break;
      }
      case BUZZVM_INSTR_LLOAD: {
         inc_pc();
         get_arg(uint32_t);
         buzzvm_lload(vm, arg);
         break;
      }
      case BUZZVM_INSTR_LSTORE: {
         inc_pc();
         get_arg(uint32_t);
         buzzvm_lstore(vm, arg);
         break;
      }
      case BUZZVM_INSTR_JUMP: {
         inc_pc();
         get_arg(uint32_t);
         vm->pc = arg;
         assert_pc(vm->pc);
         break;
      }
      case BUZZVM_INSTR_JUMPZ: {
         inc_pc();
         get_arg(uint32_t);
         buzzvm_stack_assert(vm, 1);
         if(buzzvm_stack_at(vm, 1)->o.type == BUZZTYPE_NIL ||
            (buzzvm_stack_at(vm, 1)->o.type == BUZZTYPE_INT &&
             buzzvm_stack_at(vm, 1)->i.value == 0)) {
            vm->pc = arg;
            assert_pc(vm->pc);
         }
         buzzvm_pop(vm);
         break;
      }
      case BUZZVM_INSTR_JUMPNZ: {
         inc_pc();
         get_arg(uint32_t);
         buzzvm_stack_assert(vm, 1);
         if(buzzvm_stack_at(vm, 1)->o.type != BUZZTYPE_NIL &&
            (buzzvm_stack_at(vm, 1)->o.type != BUZZTYPE_INT ||
             buzzvm_stack_at(vm, 1)->i.value != 0)) {
            vm->pc = arg;
            assert_pc(vm->pc);
         }
         buzzvm_pop(vm);
         break;
      }
      default:
         buzzvm_seterror(vm, BUZZVM_ERROR_INSTR, NULL);
         break;
   }
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_execute_script(buzzvm_t vm) {
   while(buzzvm_step(vm) == BUZZVM_STATE_READY);
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_closure_call(buzzvm_t vm,
                                 uint32_t argc) {
   /* Insert the self table right before the closure */
   buzzdarray_insert(vm->stack,
                     buzzdarray_size(vm->stack) - argc - 1,
                     buzzheap_newobj(vm, BUZZTYPE_NIL));
   /* Push the argument count */
   buzzvm_pushi(vm, argc);
   /* Save the current stack depth */
   uint32_t stacks = buzzdarray_size(vm->stacks);
   /* Call the closure and keep stepping until
    * the stack count is back to the saved value */
   buzzvm_callc(vm);
   do if(buzzvm_step(vm) != BUZZVM_STATE_READY) return vm->state;
   while(stacks < buzzdarray_size(vm->stacks));
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_function_call(buzzvm_t vm,
                                  const char* fname,
                                  uint32_t argc) {
   /* Reset the VM state if it's DONE */
   if(vm->state == BUZZVM_STATE_DONE)
      vm->state = BUZZVM_STATE_READY;
   /* Don't continue if the VM has an error */
   if(vm->state != BUZZVM_STATE_READY)
      return vm->state;
   /* Push the function name (return with error if not found) */
   buzzvm_pushs(vm, buzzvm_string_register(vm, fname, 0));
   /* Get associated symbol */
   buzzvm_gload(vm);
   /* Make sure it's a closure */
   buzzvm_type_assert(vm, 1, BUZZTYPE_CLOSURE);
   /* Move closure before arguments */
   if(argc > 0) {
      buzzdarray_insert(vm->stack,
                        buzzdarray_size(vm->stack) - argc - 1,
                        buzzvm_stack_at(vm, 1));
      buzzvm_pop(vm);
   }
   /* Call the closure */
   return buzzvm_closure_call(vm, argc);
}

/****************************************/
/****************************************/

int buzzvm_function_cmp(const void* a, const void* b) {
   if(*(uintptr_t*)a < *(uintptr_t*)b) return -1;
   if(*(uintptr_t*)a > *(uintptr_t*)b) return  1;
   return 0;
}

uint32_t buzzvm_function_register(buzzvm_t vm,
                                  buzzvm_funp funp) {
   /* Look for function pointer to avoid duplicates */
   uint32_t fpos = buzzdarray_find(vm->flist, buzzvm_function_cmp, funp);
   if(fpos == buzzdarray_size(vm->flist)) {
      /* Add function to the list */
      buzzdarray_push(vm->flist, &funp);
   }
   return fpos;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_call(buzzvm_t vm, int isswrm) {
   /* Get argument number and pop it */
   buzzvm_stack_assert(vm, 1);
   buzzvm_type_assert(vm, 1, BUZZTYPE_INT);
   int32_t argn = buzzvm_stack_at(vm, 1)->i.value;
   buzzvm_pop(vm);
   /* Make sure the stack has enough elements */
   buzzvm_stack_assert(vm, argn+1);
   /* Make sure the closure is where expected */
   buzzvm_type_assert(vm, argn+1, BUZZTYPE_CLOSURE);
   buzzobj_t c = buzzvm_stack_at(vm, argn+1);
   /* Make sure that that data about C closures is correct */
   if((!c->c.value.isnative) &&
      ((c->c.value.ref) >= buzzdarray_size(vm->flist))) {
      buzzvm_seterror(vm, BUZZVM_ERROR_FLIST, NULL);
      return vm->state;
   }
   /* Create a new local symbol list copying the parent's */
   vm->lsyms =
      buzzvm_lsyms_new(isswrm,
                       buzzdarray_clone(c->c.value.actrec));
   buzzdarray_push(vm->lsymts, &(vm->lsyms));
   /* Add function arguments to the local symbols */
   int32_t i;
   for(i = argn; i > 0; --i)
      buzzdarray_push(vm->lsyms->syms,
                      &buzzdarray_get(vm->stack,
                                      buzzdarray_size(vm->stack) - i,
                                      buzzobj_t));
   /* Get rid of the function arguments */
   for(i = argn+1; i > 0; --i)
      buzzdarray_pop(vm->stack);
   /* Pop unused self table */
   buzzdarray_pop(vm->stack);
   /* Push return address */
   buzzvm_pushi((vm), vm->pc);
   /* Make a new stack for the function */
   vm->stack = buzzdarray_new(1, sizeof(buzzobj_t), NULL);
   buzzdarray_push(vm->stacks, &(vm->stack));
   /* Jump to/execute the function */
   if(c->c.value.isnative) {
      vm->oldpc = vm->pc;
      vm->pc = c->c.value.ref;
   }
   else buzzdarray_get(vm->flist,
                       c->c.value.ref,
                       buzzvm_funp)(vm);
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_pop(buzzvm_t vm) {
   if(buzzdarray_isempty(vm->stack)) {
      buzzvm_seterror(vm, BUZZVM_ERROR_STACK, "empty stack");
      return vm->state;
   }
   else {
      buzzdarray_pop(vm->stack);
   }
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_dup(buzzvm_t vm) {
   if(buzzdarray_isempty(vm->stack)) {
      buzzvm_seterror(vm, BUZZVM_ERROR_STACK, "empty stack");
      return vm->state;
   }
   else {
      buzzobj_t x = buzzvm_stack_at(vm, 1);
      buzzdarray_push(vm->stack, &x);
   }
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_push(buzzvm_t vm, buzzobj_t v) {
   buzzdarray_push(vm->stack, &v);
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_pushu(buzzvm_t vm, void* v) {
   buzzobj_t o = buzzheap_newobj(vm, BUZZTYPE_USERDATA);
   o->u.value = v;
   buzzvm_push(vm, o);
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_pushnil(buzzvm_t vm) {
   buzzobj_t o = buzzheap_newobj(vm, BUZZTYPE_NIL);
   buzzvm_push(vm, o);
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_pushc(buzzvm_t vm, int32_t rfrnc, int32_t nat) {
   buzzobj_t o = buzzheap_newobj(vm, BUZZTYPE_CLOSURE);
   o->c.value.isnative = nat;
   o->c.value.ref = rfrnc;
   buzzobj_t nil = buzzheap_newobj(vm, BUZZTYPE_NIL);
   buzzdarray_push(o->c.value.actrec, &nil);
   buzzvm_push(vm, o);
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_pushi(buzzvm_t vm, int32_t v) {
   buzzobj_t o = buzzheap_newobj(vm, BUZZTYPE_INT);
   o->i.value = v;
   buzzvm_push(vm, o);
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_pushf(buzzvm_t vm, float v) {
   buzzobj_t o = buzzheap_newobj(vm, BUZZTYPE_FLOAT);
   o->f.value = v;
   buzzvm_push(vm, o);
   return vm->state;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_pushs(buzzvm_t vm, uint16_t strid) {
   if(!buzzstrman_get(vm->strings, strid)) {
      buzzvm_seterror(vm,
                      BUZZVM_ERROR_STRING,
                      "id read = %" PRIu16,
                      strid);
      return vm->state;
   }
   buzzobj_t o = buzzheap_newobj(vm, BUZZTYPE_STRING);
   o->s.value.sid = (strid);
   o->s.value.str = buzzstrman_get(vm->strings, strid);
   buzzvm_push(vm, o);
   return BUZZVM_STATE_READY;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_pushl(buzzvm_t vm, int32_t addr) {
   buzzobj_t o = buzzheap_newobj(vm, BUZZTYPE_CLOSURE);
   o->c.value.isnative = 1;
   o->c.value.ref = addr;
   if(vm->lsyms) {
      int i;
      for(i = 0; i < buzzdarray_size(vm->lsyms->syms); ++i)
         buzzdarray_push(o->c.value.actrec,
                         &buzzdarray_get(vm->lsyms->syms,
                                         i, buzzobj_t));
   }
   else {
      buzzobj_t nil = buzzheap_newobj(vm, BUZZTYPE_NIL);
      buzzdarray_push(o->c.value.actrec,
                      &nil);
   }
   return buzzvm_push(vm, o);
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_tput(buzzvm_t vm) {
   buzzvm_stack_assert(vm, 3);
   buzzvm_type_assert(vm, 3, BUZZTYPE_TABLE);
   buzzobj_t v = buzzvm_stack_at(vm, 1);
   buzzobj_t k = buzzvm_stack_at(vm, 2);
   buzzobj_t t = buzzvm_stack_at(vm, 3);
   buzzvm_pop(vm);
   buzzvm_pop(vm);
   buzzvm_pop(vm);
   if(k->o.type != BUZZTYPE_INT &&
      k->o.type != BUZZTYPE_FLOAT &&
      k->o.type != BUZZTYPE_STRING) {
      buzzvm_seterror(vm, BUZZVM_ERROR_TYPE, "a %s value can't be used as table key", buzztype_desc[k->o.type]);
      return vm->state;
   }
   if(v->o.type == BUZZTYPE_NIL) {
      /* Nil, erase entry */
      buzzdict_remove(t->t.value, &k);
   }
   else if(v->o.type == BUZZTYPE_CLOSURE) {
      /* Method call */
      int i;
      buzzobj_t o = buzzheap_newobj(vm, BUZZTYPE_CLOSURE);
      o->c.value.isnative = v->c.value.isnative;
      o->c.value.ref = v->c.value.ref;
      buzzdarray_push(o->c.value.actrec, &t);
      for(i = 1; i < buzzdarray_size(v->c.value.actrec); ++i)
         buzzdarray_push(o->c.value.actrec,
                         &buzzdarray_get(v->c.value.actrec,
                                         i, buzzobj_t));
      buzzdict_set(t->t.value, &k, &o);
   }
   else {
      buzzdict_set(t->t.value, &k, &v);
   }
   return BUZZVM_STATE_READY;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_tget(buzzvm_t vm) {
   buzzvm_stack_assert(vm, 2);
   buzzvm_type_assert(vm, 2, BUZZTYPE_TABLE);
   buzzobj_t k = buzzvm_stack_at(vm, 1);
   buzzobj_t t = buzzvm_stack_at(vm, 2);
   buzzvm_pop(vm);
   buzzvm_pop(vm);
   if(k->o.type != BUZZTYPE_INT &&
      k->o.type != BUZZTYPE_FLOAT &&
      k->o.type != BUZZTYPE_STRING) {
      buzzvm_seterror(vm, BUZZVM_ERROR_TYPE, "a %s value can't be used as table key", k->o.type);
      return vm->state;
   }
   const buzzobj_t* v = buzzdict_get(t->t.value, &k, buzzobj_t);
   if(v) buzzvm_push(vm, *v);
   else buzzvm_pushnil(vm);
   return BUZZVM_STATE_READY;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_gload(buzzvm_t vm) {
   buzzvm_stack_assert(vm, 1);
   buzzvm_type_assert(vm, 1, BUZZTYPE_STRING);
   buzzobj_t str = buzzvm_stack_at(vm, 1);
   buzzvm_pop(vm);
   const buzzobj_t* o = buzzdict_get(vm->gsyms, &(str->s.value.sid), buzzobj_t);
   if(!o) { buzzvm_pushnil(vm); }
   else { buzzvm_push(vm, (*o)); }
   return BUZZVM_STATE_READY;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_gstore(buzzvm_t vm) {
   buzzvm_stack_assert((vm), 2);
   buzzvm_type_assert((vm), 2, BUZZTYPE_STRING);
   buzzobj_t str = buzzvm_stack_at((vm), 2);
   buzzobj_t o = buzzvm_stack_at((vm), 1);
   buzzvm_pop(vm);
   buzzvm_pop(vm);
   buzzdict_set((vm)->gsyms, &(str->s.value.sid), &o);
   return BUZZVM_STATE_READY;
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_ret0(buzzvm_t vm) {
   /* Pop swarm stack */
   if(vm->lsyms->isswarm)
      buzzdarray_pop(vm->swarmstack);
   /* Pop local symbol table */
   buzzdarray_pop(vm->lsymts);
   /* Set local symbol table pointer */
   vm->lsyms = !buzzdarray_isempty(vm->lsymts) ?
      buzzdarray_last(vm->lsymts, buzzvm_lsyms_t) :
      NULL;
   /* Pop stack */
   buzzdarray_pop(vm->stacks);
   /* Set stack pointer */
   vm->stack = buzzdarray_last(vm->stacks, buzzdarray_t);
   /* Make sure the stack contains at least one element */
   buzzvm_stack_assert(vm, 1);
   /* Make sure that element is an integer */
   buzzvm_type_assert(vm, 1, BUZZTYPE_INT);
   /* Use that element as program counter */
   vm->oldpc = vm->pc;
   vm->pc = buzzvm_stack_at(vm, 1)->i.value;
   /* Pop the return address */
   return buzzvm_pop(vm);
}

/****************************************/
/****************************************/

buzzvm_state buzzvm_ret1(buzzvm_t vm) {
   /* Pop swarm stack */
   if(vm->lsyms->isswarm)
      buzzdarray_pop(vm->swarmstack);
   /* Pop local symbol table */
   buzzdarray_pop(vm->lsymts);
   /* Set local symbol table pointer */
   vm->lsyms = !buzzdarray_isempty(vm->lsymts) ?
      buzzdarray_last(vm->lsymts, buzzvm_lsyms_t) :
      NULL;
   /* Make sure there's an element on the stack */
   buzzvm_stack_assert(vm, 1);
   /* Save it, it's the return value to pass to the lower stack */
   buzzobj_t ret = buzzvm_stack_at(vm, 1);
   /* Pop stack */
   buzzdarray_pop(vm->stacks);
   /* Set stack pointer */
   vm->stack = buzzdarray_last(vm->stacks, buzzdarray_t);
   /* Make sure the stack contains at least one element */
   buzzvm_stack_assert(vm, 1);
   /* Make sure that element is an integer */
   buzzvm_type_assert(vm, 1, BUZZTYPE_INT);
   /* Use that element as program counter */
   vm->oldpc = vm->pc;
   vm->pc = buzzvm_stack_at(vm, 1)->i.value;
   /* Pop the return address */
   buzzvm_pop(vm);
   /* Push the return value */
   return buzzvm_push(vm, ret);
}

/****************************************/
/****************************************/

buzzdarray_t buzzvm_vm_serialize(buzzvm_t vm) {
    printf("Size of gsyms %u \n",buzzdict_size(vm->gsyms));
   buzzdarray_t a = buzzvm_gsyms_serialize(vm);
    printf("Size of gsyms serialization is : %i\n",buzzdarray_size(a));
   //buzzbstig_seralize_blb_stigs(vm, a);
   // printf("Size of after blob serialization is : %i\n",buzzdarray_size(a));
   return a;
}

/****************************************/
/****************************************/

void buzzvm_vm_deserialize_set(buzzvm_t vm, buzzdarray_t a) {
   int64_t pos = buzzvm_gsyms_deserialize_set(vm,a,0);
    printf("Size after gsyms deserialization is : %d\n",pos);
   //pos = buzzbstig_deseralize_blb_stigs_set(vm, a, pos);
   // printf("Size after blob deserialization is : %d\n",pos);
}

/****************************************/
/****************************************/

#include <arpa/inet.h>

struct vm_gsyms_serialize_param{
   buzzvm_t vm;
   buzzdarray_t gm;
   uint32_t* ssize;

};

void buzzvm_gsyms_serialize_foreach(const void* key, void* data, void* params){
   struct vm_gsyms_serialize_param p = *(struct vm_gsyms_serialize_param*) params;
   buzzvm_t vm = p.vm;
   buzzdarray_t gm = p.gm;
   uint16_t sid = *(uint16_t*)key;
   const char* keystring = buzzstrman_get(vm->strings, sid);
   const buzzobj_t* op = (buzzobj_t*)data;
   const buzzobj_t o = *(buzzobj_t*)op;
   const char* id_Str = "id";
   if(!strcmp(keystring,id_Str)) return;
   if(o->o.type == BUZZTYPE_INT   ||
      o->o.type == BUZZTYPE_FLOAT ||
      o->o.type == BUZZTYPE_STRING ){
      buzzmsg_serialize_string(gm,keystring);
      buzzobj_serialize(gm,o);
      printf("Serlised key %s -> value %u\n",keystring,o->i.value);
      (*(uint32_t*)p.ssize)++;
   }
   
   // else if(o->o.type == BUZZTYPE_TABLE){
   //    printf("\t [%u] BUZZTYPE_TABLE : %s -> [table] \n", sid,keystring);
   // }

}


buzzdarray_t buzzvm_gsyms_serialize(buzzvm_t vm) {
   buzzdarray_t gm = buzzdarray_new(20, sizeof(uint8_t), NULL);
   /* size of gsyms just with int,float and strings*/
   uint32_t size = 0;
   struct vm_gsyms_serialize_param p ={.vm=vm, .gm=gm, .ssize=&size };
   buzzmsg_serialize_u32(gm,size);
   buzzdict_foreach(vm->gsyms,buzzvm_gsyms_serialize_foreach,&p);
   uint32_t x = htonl(size);
   buzzdarray_set(gm,0,(uint8_t*)(&x));
   buzzdarray_set(gm,1,(uint8_t*)(&x)+1);
   buzzdarray_set(gm,2,(uint8_t*)(&x)+2);
   buzzdarray_set(gm,3,(uint8_t*)(&x)+3);
   uint32_t tesize;
   buzzmsg_deserialize_u32(&tesize,gm,0);
    printf("size of gsyms after ser %u\n",tesize );
   return gm;
}
/****************************************/
/****************************************/
int64_t buzzvm_gsyms_deserialize_set(buzzvm_t vm,buzzdarray_t a, int64_t pos){
   uint32_t size; 
   pos = buzzmsg_deserialize_u32(&size,a,pos);
    printf("size of gsyms %u\n",size );
   for (int i = 0; i < size; ++i){
       char* gstr;
       buzzobj_t o;  
       pos = buzzmsg_deserialize_string(&gstr,a,pos);
       pos= buzzobj_deserialize(&o,a,pos, vm);
       if(o->o.type == BUZZTYPE_INT){
         buzzvm_pushs(vm, buzzvm_string_register(vm, gstr,1));
         buzzvm_pushi(vm, o->i.value);
         printf("Deserialized key %s -> value  %u\n",gstr,o->i.value );
         buzzvm_gstore(vm);
       }
       else if(o->o.type == BUZZTYPE_FLOAT){
         buzzvm_pushs(vm, buzzvm_string_register(vm, gstr,1));
         buzzvm_pushf(vm, o->f.value);
         printf("Deserialized key %s -> value  %u\n",gstr,o->f.value );
         buzzvm_gstore(vm);
       }
       else if(o->o.type == BUZZTYPE_STRING){
         buzzvm_pushs(vm, buzzvm_string_register(vm, gstr,1));
         buzzvm_pushs(vm, buzzvm_string_register(vm, o->s.value.str,1));
         printf("Deserialized key %s -> value  %u\n",gstr,o->s.value.str );
         buzzvm_gstore(vm);
       }
       free(gstr);
   } 
   return pos;
}

/****************************************/
/****************************************/