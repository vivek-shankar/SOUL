#include "buzzvm.h"
#include "buzzheap.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <math.h>
#include <string.h>

/****************************************/
/****************************************/

/*
 * Broadcast message data
 */
struct buzzoutmsg_broadcast_s {
   int type;
   buzzobj_t topic;
   buzzobj_t value;
};

/*
 * Swarm message data
 */
struct buzzoutmsg_swarm_s {
   int type;
   uint16_t* ids;
   uint16_t size;
};

/*
 * Virtual stigmergy message data
 */
struct buzzoutmsg_vstig_s {
   int type;
   uint16_t id;
   buzzobj_t key;
   buzzvstig_elem_t data;
};

/*
 * Blob stigmergy message data
 */
struct buzzoutmsg_bstig_s {
   int type;
   uint16_t id;
   buzzobj_t key;
   buzzbstig_elem_t data;
   uint32_t blob_size;
   uint8_t  blob_entry;
};

/*
 * Blob stigmergy blob chunk data
 */
struct buzzoutmsg_bstig_chunk_s {
   int type;
   uint16_t id;            // bstig id
   buzzobj_t key;          // bstig key the blob belongs
   buzzbstig_elem_t data;  // bstig  entry in bstig.
   uint32_t blob_size;     // blob size
   uint16_t chunk_index;   // Index of the chunk 
   uint16_t receiver;      // BROADCAST_MESSAGE_CONSTANT
                           // for broadcast, or receiver of the message
   buzzblob_chunk_t cdata; // chunk data 
};

/*
 * Blob stigmergy blob status
 */
struct buzzoutmsg_bstig_status_s {
   int type;
   uint16_t id;            // bstig id
   uint16_t key;           // bstig key the blob belongs
   uint8_t  status;        // status of blob
   uint16_t requester;     // rid requesting status change
};

/*
 * Blob request message 
 *
 */
struct buzzoutmsg_blob_request_s {
   int type;
   uint16_t id;
   uint16_t key; 
   uint16_t receiver;
   uint16_t sender;
};
/*
 * Blob stigmergy blob status
 */
struct buzzoutmsg_bstig_reloc_s {
   int type;
   uint16_t id;            // bstig id
   uint16_t key;           // bstig key the blob belongs
   uint16_t cid;
   uint16_t receiver;
   uint8_t subtype;
   uint16_t  msg;        // status of blob
};

/*
 * Blob chunk removal data
 */
struct buzzoutmsg_bstig_chunkremoval_s {
   int type;
   uint16_t id;            // bstig id
   uint16_t key;           // bstig key the blob belongs
   uint16_t cid;           // bstig  cid in bstig.
   uint16_t subtype;       // Subtype of message
   uint16_t timer;
};

/*
 * Blob bidder data
 */
struct buzzoutmsg_bstig_bidder_s {
   int type;
   uint16_t id;            // bstig id
   uint16_t key;           // bstig key the blob belongs
   uint16_t bidderid;      // bidder id
   uint32_t blob_size;       // size of blob
   uint32_t hash;           // hash of the chunk
   uint16_t availablespace; // available space to store chunks
   uint8_t  subtype;       // message subtype
   uint8_t getter;
   uint16_t timer;         // time to destroy
   uint16_t receiver;
};


/*
 * Generic message data
 */
union buzzoutmsg_u {
   int type;
   struct buzzoutmsg_broadcast_s         bc;
   struct buzzoutmsg_swarm_s             sw;
   struct buzzoutmsg_vstig_s             vs;
   struct buzzoutmsg_bstig_s             bs;
   struct buzzoutmsg_bstig_chunk_s       bsc;
   struct buzzoutmsg_bstig_status_s      bss;
   struct buzzoutmsg_bstig_reloc_s       brl;
   struct buzzoutmsg_bstig_chunkremoval_s cr;
   struct buzzoutmsg_bstig_bidder_s      bid;
   struct buzzoutmsg_blob_request_s      brm;
};
typedef union buzzoutmsg_u* buzzoutmsg_t;

/****************************************/
/****************************************/

uint32_t buzzoutmsg_obj_hash(const void* key) {
   return buzzobj_hash(*(buzzobj_t*)key);
}

int buzzoutmsg_obj_cmp(const void* a, const void* b) {
   return buzzobj_cmp(*(const buzzobj_t*)a, *(const buzzobj_t*)b);
}

void buzzoutmsg_destroy(uint32_t pos, void* data, void* params) {
   buzzoutmsg_t m = *(buzzoutmsg_t*)data;
   switch(m->type) {
      case BUZZMSG_BROADCAST:
         break;
      case BUZZMSG_SWARM_JOIN:
      case BUZZMSG_SWARM_LEAVE:
      case BUZZMSG_SWARM_LIST:
         if(m->sw.size > 0) free(m->sw.ids);
         break;
      case BUZZMSG_VSTIG_PUT:
      case BUZZMSG_VSTIG_QUERY:
         free(m->vs.data);
         break;
      case BUZZMSG_BSTIG_PUT:
      case BUZZMSG_BSTIG_QUERY:
         free(m->bs.data);
         break;
      case BUZZMSG_BSTIG_CHUNK_PUT_P2P:
      case BUZZMSG_BSTIG_CHUNK_PUT:
         free(m->bsc.data);
         free(m->bsc.cdata->chunk);
         free(m->bsc.cdata);
         break;
      case BUZZMSG_BSTIG_STATUS:        
      case BUZZMSG_BSTIG_CHUNK_STATUS_QUERY:
      case BUZZMSG_BSTIG_CHUNK_REMOVED:
      case BUZZMSG_BSTIG_BLOB_REQUEST:
         break;
      
   }
   
   free(m);
}

void buzzp2poutmsg_destroy(uint32_t pos, void* data, void* params){

}
void buzzoutmsg_vstig_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   buzzdict_destroy((buzzdict_t*)data);
   free(data);
}
void buzzoutmsg_bstig_chunkremoval_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   buzzoutmsg_t m = *(buzzoutmsg_t*)data;
   free(m);
   free(data);
}
int buzzoutmsg_vstig_cmp(const void* a, const void* b) {
   if((uintptr_t)(*(buzzoutmsg_t*)a) < (uintptr_t)(*(buzzoutmsg_t*)b)) return -1;
   if((uintptr_t)(*(buzzoutmsg_t*)a) > (uintptr_t)(*(buzzoutmsg_t*)b)) return  1;
   return 0;
}

void buzzoutmsg_bstig_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   buzzdict_destroy((buzzdict_t*)data);
   free(data);
}

void buzzoutmsg_bstig_p2p_destroy(const void* key, void* data, void* params) {
   free((void*)key);
   buzzdarray_destroy((buzzdarray_t*)data);
   free(data);
}

int buzzoutmsg_bstig_cmp(const void* a, const void* b) {
   if((uintptr_t)(*(buzzoutmsg_t*)a) < (uintptr_t)(*(buzzoutmsg_t*)b)) return -1;
   if((uintptr_t)(*(buzzoutmsg_t*)a) > (uintptr_t)(*(buzzoutmsg_t*)b)) return  1;
   return 0;
}

int buzzoutmsg_bstig_keyquery_cmp(const void* a, const void* b) {
   if((uint16_t)( (*(buzzoutmsg_t*)b)->bs.id) < (*(uint16_t*)a)) return -1;
   if((uint16_t)( (*(buzzoutmsg_t*)b)->bs.id) > (*(uint16_t*)a)) return  1;
   return 0;
}

/****************************************/
/****************************************/

buzzoutmsg_queue_t buzzoutmsg_queue_new() {
   buzzoutmsg_queue_t q = (buzzoutmsg_queue_t)malloc(sizeof(struct buzzoutmsg_queue_s));
   q->queues[BUZZMSG_BROADCAST]           = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_SWARM_LIST]  	      = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_SWARM_JOIN]  	      = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_SWARM_LEAVE] 	      = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_VSTIG_PUT]   	      = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_VSTIG_QUERY] 	      = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_BSTIG_PUT]   	      = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_BSTIG_QUERY] 	      = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_BSTIG_STATUS]    = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_BSTIG_BLOB_BID] = buzzdarray_new(1, sizeof(buzzoutmsg_t), NULL);
   q->queues[BUZZMSG_BSTIG_CHUNK_STATUS_QUERY]         = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_BSTIG_CHUNK_REMOVED] = buzzdarray_new(1, sizeof(buzzoutmsg_t), NULL);
   q->queues[BUZZMSG_BSTIG_CHUNK_PUT]         = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_BSTIG_CHUNK_QUERY]         = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P]       = buzzdarray_new(1, sizeof(buzzoutmsg_t), buzzoutmsg_destroy);
   q->vstig = buzzdict_new(10,
                           sizeof(uint16_t),
                           sizeof(buzzdict_t),
                           buzzdict_uint16keyhash,
                           buzzdict_uint16keycmp,
                           buzzoutmsg_vstig_destroy);
   q->bstig = buzzdict_new(10,
                           sizeof(uint16_t),
                           sizeof(buzzdict_t),
                           buzzdict_uint16keyhash,
                           buzzdict_uint16keycmp,
                           buzzoutmsg_bstig_destroy);
   q->bstigstatus = buzzdict_new(10,
                           sizeof(uint16_t),
                           sizeof(buzzdict_t),
                           buzzdict_uint16keyhash,
                           buzzdict_uint16keycmp,
                           buzzoutmsg_bstig_destroy);
   q->chunkbstig = buzzdict_new(10,
                           sizeof(uint16_t),
                           sizeof(buzzdict_t),
                           buzzdict_uint16keyhash,
                           buzzdict_uint16keycmp,
                           buzzoutmsg_bstig_destroy);
   q->chunkp2p = buzzdict_new(10,
                           sizeof(uint16_t),
                           sizeof(buzzdarray_t),
                           buzzdict_uint16keyhash,
                           buzzdict_uint16keycmp,
                           buzzoutmsg_bstig_p2p_destroy);
   q->bidprotect = buzzdict_new(10,
                           sizeof(uint16_t),
                           sizeof(buzzdict_t),
                           buzzdict_uint16keyhash,
                           buzzdict_uint16keycmp,
                           buzzoutmsg_bstig_destroy);
   q->cremovalprotect = buzzdict_new(10,
                           sizeof(uint16_t),
                           sizeof(buzzdict_t),
                           buzzdict_uint16keyhash,
                           buzzdict_uint16keycmp,
                           buzzoutmsg_bstig_destroy);
   q->bstigrecon = buzzdarray_new(10, sizeof(buzzoutmsg_t), NULL);
   return q;
}

/****************************************/
/****************************************/

void buzzoutmsg_queue_destroy(buzzoutmsg_queue_t* msgq) {
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_BROADCAST]));
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_SWARM_LIST]));
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_SWARM_JOIN]));
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_SWARM_LEAVE]));
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_VSTIG_PUT]));
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_VSTIG_QUERY]));
   buzzdict_destroy(&((*msgq)->vstig));
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_BSTIG_PUT]));
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_BSTIG_QUERY]));
   buzzdict_destroy(&((*msgq)->bstig));
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_BSTIG_STATUS])); 
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_BSTIG_BLOB_BID])); 
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_BSTIG_CHUNK_REMOVED])); 
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_BSTIG_CHUNK_STATUS_QUERY]));
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_BSTIG_CHUNK_PUT]));
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_BSTIG_CHUNK_QUERY])); 
   buzzdarray_destroy(&((*msgq)->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P])); 
   buzzdict_destroy(&((*msgq)->chunkbstig));
   buzzdict_destroy(&((*msgq)->bstigstatus));
   buzzdict_destroy(&((*msgq)->bidprotect));
   buzzdict_destroy(&((*msgq)->cremovalprotect));
   buzzdict_destroy(&((*msgq)->chunkp2p));
   buzzdarray_destroy(&((*msgq)->bstigrecon));
   free(*msgq);
}

/****************************************/
/****************************************/

uint32_t buzzoutmsg_queue_size(buzzvm_t vm) {
   return
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BROADCAST]) +
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_SWARM_LIST]) +
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_SWARM_JOIN]) +
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_SWARM_LEAVE]) +
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_VSTIG_PUT]) +
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_VSTIG_QUERY])+
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_PUT]) +
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_QUERY]) +
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_STATUS]) +
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_BLOB_BID]) +
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_REMOVED]) +
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_STATUS_QUERY]) +
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT])+
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_QUERY]);
}

uint32_t buzzoutmsg_chunk_queue_size(buzzvm_t vm) {
   return
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT]);
}

uint32_t buzzoutmsg_p2p_chunk_queue_size(buzzvm_t vm){
   return 
      buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P]);
}
/****************************************/
/****************************************/


/****************************************/
/****************************************/

void buzzoutmsg_queue_append_broadcast(buzzvm_t vm,
                                       buzzobj_t topic,
                                       buzzobj_t value) {
   /* Make a new BROADCAST message */
   buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
   m->bc.type = BUZZMSG_BROADCAST;
   m->bc.topic = buzzheap_clone(vm, topic);
   m->bc.value = buzzheap_clone(vm, value);
   /* Queue it */
   buzzdarray_push(vm->outmsgs->queues[BUZZMSG_BROADCAST], &m);   
}

/****************************************/
/****************************************/

struct dict_to_array_s {
   size_t count; /* Current element count */
   size_t size;  /* Current data buffer size */
   uint16_t* data; /* Data buffer */
};

void dict_to_array(const void* key, void* data, void* params) {
   /* (*key)   is a uint16_t, the swarm id
    * (*data)  is a uint8_t, the membership (1 -> in, 0 -> out)
    * (params) is a (struct dict_to_array_s*)
    */
   struct dict_to_array_s* p = (struct dict_to_array_s*)params;
   if(*(uint8_t*)data) {
      /* Grow buffer if necessary */
      if(p->count >= p->size) {
         p->size *= 2;
         p->data = realloc(p->data, p->size * sizeof(uint16_t));
      }
      /* Append swarm id */
      p->data[p->count] = *(uint16_t*)key;
      ++p->count;
   }
}

void buzzoutmsg_queue_append_swarm_list(buzzvm_t vm,
                                        const buzzdict_t ids) {
   /* Invariants:
    * - Only one list message can be queued at any time;
    * - If a list message is already queued, join/leave messages are not
    */
   /* Delete every existing SWARM related message */
   buzzdarray_clear(vm->outmsgs->queues[BUZZMSG_SWARM_LIST], 1);
   buzzdarray_clear(vm->outmsgs->queues[BUZZMSG_SWARM_JOIN], 1);
   buzzdarray_clear(vm->outmsgs->queues[BUZZMSG_SWARM_LEAVE], 1);
   /* Make an array of current swarm id dictionary */
   struct dict_to_array_s da = {
      .count = 0,
      .size = 1,
      .data = (uint16_t*)malloc(sizeof(uint16_t))
   };
   buzzdict_foreach(ids, dict_to_array, &da);
   /* Make a new LIST message */
   buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
   m->sw.type = BUZZMSG_SWARM_LIST;
   m->sw.size = da.count;
   m->sw.ids = (uint16_t*)malloc(m->sw.size * sizeof(uint16_t));
   memcpy(m->sw.ids, da.data, m->sw.size * sizeof(uint16_t));
   free(da.data);
   /* Queue the new LIST message */
   buzzdarray_push(vm->outmsgs->queues[BUZZMSG_SWARM_LIST], &m);
}

/****************************************/
/****************************************/

static void append_to_swarm_queue(buzzdarray_t q, uint16_t id, int type) {
   /* Is the queue empty? */
   if(buzzdarray_isempty(q)) {
      /* Yes, add the element at the end */
      buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
      m->sw.type = type;
      m->sw.size = 1;
      m->sw.ids = (uint16_t*)malloc(sizeof(uint16_t));
      m->sw.ids[0] = id;
      buzzdarray_push(q, &m);
   }
   else {
      /* Queue not empty - look for a message with the same id */
      int found = 0;
      uint32_t i;
      for(i = 0; i < buzzdarray_size(q) && !found; ++i) {
         found = buzzdarray_get(q, i, buzzoutmsg_t)->sw.ids[0] == id;
      }
      /* Message found? */
      if(!found) {
         /* No, append a new message the passed id */
         buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
         m->sw.type = type;
         m->sw.size = 1;
         m->sw.ids = (uint16_t*)malloc(sizeof(uint16_t));
         m->sw.ids[0] = id;
         buzzdarray_push(q, &m);
      }
   }
}

static void remove_from_swarm_queue(buzzdarray_t q, uint16_t id) {
   /* Is the queue empty? If so, nothing to do */
   if(buzzdarray_isempty(q)) return;
   /* Queue not empty - look for a message with the same id */
   uint32_t i;
   for(i = 0; i < buzzdarray_size(q); ++i) {
      if(buzzdarray_get(q, i, buzzoutmsg_t)->sw.ids[0] == id) break;
   }
   /* Message found? If so, remove it */
   if(i < buzzdarray_size(q)) buzzdarray_remove(q, i);
}

void buzzoutmsg_queue_append_swarm_joinleave(buzzvm_t vm,
                                             int type,
                                             uint16_t id) {
   /* Invariants:
    * - Only one list message can be qeueued at any time;
    * - If a list message is already queued, join/leave messages are not
    */
   /* Is there a LIST message? */
   if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_SWARM_LIST])) {
      /* Yes, get a handle to the message */
      buzzoutmsg_t l = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_SWARM_LIST], 0, buzzoutmsg_t);
      /* Go through the ids in the list and look for the passed id */
      uint16_t i = 0;
      while(i < l->sw.size && l->sw.ids[i] != id) ++i;
      /* Id found? */
      if(i < l->sw.size) {
         /* Yes; is the message a LEAVE? */
         if(type == BUZZMSG_SWARM_LEAVE) {
            /* Yes: remove it from the list */
            --(l->sw.size);
            memmove(l->sw.ids+i, l->sw.ids+i+1, (l->sw.size-i) * sizeof(uint16_t));
         }
         /* If the message is a JOIN, there's nothing to do */
      }
      else {
         /* Id not found; is the message a JOIN? */
         if(type == BUZZMSG_SWARM_JOIN) {
            /* Yes: add it to the list */
            ++(l->sw.size);
            l->sw.ids = realloc(l->sw.ids, l->sw.size * sizeof(uint16_t));
            l->sw.ids[l->sw.size-1] = id;
         }
         /* If the message is a LEAVE, there's nothing to do */
      }
   }
   else {
      /* No LIST message present - send an individual message */
      if(type == BUZZMSG_SWARM_JOIN) {
         /* Look for a duplicate in the JOIN queue - if not add one  */
         append_to_swarm_queue(vm->outmsgs->queues[BUZZMSG_SWARM_JOIN], id, BUZZMSG_SWARM_JOIN);
         /* Look for an entry in the LEAVE queue and remove it  */
         remove_from_swarm_queue(vm->outmsgs->queues[BUZZMSG_SWARM_LEAVE], id);
      }
      else if(type == BUZZMSG_SWARM_LEAVE) {
         /* Look for an entry in the JOIN queue and remove it */
         remove_from_swarm_queue(vm->outmsgs->queues[BUZZMSG_SWARM_JOIN], id);
         /* Look for a duplicate in the LEAVE queue - if not add one  */
         append_to_swarm_queue(vm->outmsgs->queues[BUZZMSG_SWARM_LEAVE], id, BUZZMSG_SWARM_LEAVE);
      }
   }
}

/****************************************/
/****************************************/

void buzzoutmsg_queue_append_vstig(buzzvm_t vm,
                                   int type,
                                   uint16_t id,
                                   const buzzobj_t key,
                                   const buzzvstig_elem_t data) {
   /* Look for a duplicate message in the dictionary */
   const struct buzzoutmsg_vstig_s** e = NULL;
   /* Virtual stigmergy to actually use */
   buzzdict_t vs = NULL;
   /* Look for the virtual stigmergy */
   const buzzdict_t* tvs = buzzdict_get(vm->outmsgs->vstig, &id, buzzdict_t);
   if(tvs) {
      /* Virtual stigmergy found, look for the key */
      vs = *tvs;
      e = buzzdict_get(vs, &key, struct buzzoutmsg_vstig_s*);
   }
   else {
      /* Virtual stigmergy not found, create it */
      vs = buzzdict_new(10,
                        sizeof(buzzobj_t),
                        sizeof(struct buzzoutmsg_vstig_s*),
                        buzzoutmsg_obj_hash,
                        buzzoutmsg_obj_cmp,
                        NULL);
      buzzdict_set(vm->outmsgs->vstig, &id, &vs);
   }
   /* Do we have a more recent duplicate? */
   int etype = -1, eidx = -1; /* No message to remove from the queue */
   if(e) {
      /* Yes; if the duplicate is newer than the passed message, nothing to do */
      if((*e)->data->timestamp >= data->timestamp) return;
      /* The duplicate is older, store data to remove it later */
      etype = (*e)->type;
      eidx = buzzdarray_find(vm->outmsgs->queues[etype], buzzoutmsg_vstig_cmp, e);
   }
   /* Create a new message */
   buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
   m->vs.type = type;
   m->vs.id = id;
   m->vs.key = buzzheap_clone(vm, key);
   m->vs.data = buzzvstig_elem_clone(vm, data);
   /* Update the dictionary - this also invalidates e */
   buzzdict_set(vs, &m->vs.key, &m);
   if(etype > -1) {
      /* Remove the entry from the queue */
      buzzdarray_remove(vm->outmsgs->queues[etype], eidx);
   }
   /* Add a new message to the queue */
   buzzdarray_push(vm->outmsgs->queues[type], &m);
}

/****************************************/
/****************************************/

void buzzoutmsg_queue_append_bstig(buzzvm_t vm,
                                   int type,
                                   uint16_t id,
                                   const buzzobj_t key,
                                   const buzzbstig_elem_t data,
                                   uint8_t blob_entry) { 
   // if(id==1 && type==BUZZMSG_BSTIG_PUT){
   //    printf(" [ RID %u] Added a chunk stig put message key %u \n", vm->robot,key->i.value);
   // }
   // else if (id==1 && type == BUZZMSG_BSTIG_QUERY){
   //    printf(" [ RID %u] Added a chunk stig QUERY message key %u \n", vm->robot,key->i.value);
   // }
   /* Look for a duplicate message in the dictionary */
   const struct buzzoutmsg_bstig_s** e = NULL;
   /* blob stigmergy to actually use */
   buzzdict_t bs = NULL;
   /* Look for the blob stigmergy */
   const buzzdict_t* tbs = buzzdict_get(vm->outmsgs->bstig, &id, buzzdict_t);
   if(tbs) {
      /* Virtual stigmergy found, look for the key */
      bs = *tbs;
      e = buzzdict_get(bs, &key, struct buzzoutmsg_bstig_s*);
   }
   else {
      /* Virtual stigmergy not found, create it */
      bs = buzzdict_new(10,
                        sizeof(buzzobj_t),
                        sizeof(struct buzzoutmsg_bstig_s*),
                        buzzoutmsg_obj_hash,
                        buzzoutmsg_obj_cmp,
                        NULL);
      buzzdict_set(vm->outmsgs->bstig, &id, &bs);
   }
   /* Do we have a more recent duplicate? */
   int etype = -1, eidx = -1; /* No message to remove from the queue */
   if(e) {
      /* Yes; if the duplicate is newer than the passed message, nothing to do */
      if((*e)->data->timestamp >= data->timestamp) return;
      /* The duplicate is older, store data to remove it later */
      etype = (*e)->type;
      eidx = buzzdarray_find(vm->outmsgs->queues[etype], buzzoutmsg_bstig_cmp, e);
   }
   /* Create a new message */
   buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
   m->bs.type = type;
   m->bs.blob_entry = blob_entry;
   m->bs.id = id;
   m->bs.key = buzzheap_clone(vm, key);
   m->bs.data = buzzbstig_elem_clone(vm, data);
   if(data->data->o.type == BUZZTYPE_NIL || !blob_entry){
      m->bs.blob_size=0;
   }
   else{
      /* Retrive blob size : TODO: Can be handled better than this */
      buzzdict_t s = *(buzzdict_get(vm->blobs, &id, buzzdict_t));
      const buzzblob_elem_t* v_blob = buzzdict_get(s, &(key->i.value), buzzblob_elem_t);
      m->bs.blob_size=(*v_blob)->size;
   }
   //printf("[DEBUG] bstig blob put size: %u \n", m->bs.blob_size);
   /* Update the dictionary - this also invalidates e */
   buzzdict_set(bs, &m->bs.key, &m);
   if(etype > -1) {
      /* Remove the entry from the queue */
      buzzdarray_remove(vm->outmsgs->queues[etype], eidx);
   }
   /* Add a new message to the queue */
   buzzdarray_push(vm->outmsgs->queues[type], &m);
}

/****************************************/
/****************************************/

void buzzoutmsg_queue_append_chunk(buzzvm_t vm,
                                   int type,
                                   uint16_t id,
                                   const buzzobj_t key,
                                   const buzzbstig_elem_t data,
                                   uint32_t blob_size,
                                   uint16_t chunk_index,
                                   const buzzblob_chunk_t cdata,
                                   uint16_t receiver) { 
   if(receiver == BROADCAST_MESSAGE_CONSTANT){ // chunk has to be broadcasted
      /* Look for bstig element in id */
      const buzzdict_t* e = NULL;
      /* blob stigmergy to actually use */
      buzzdict_t bs = NULL;
      /* Look for the blob stigmergy */
      const buzzdict_t* tbc = buzzdict_get(vm->outmsgs->chunkbstig, &id, buzzdict_t);
      if(tbc) {
         /* Virtual stigmergy found, look for the key */
         bs = *tbc;
         e = buzzdict_get(bs, &key->i.value, buzzdict_t);
         //e = buzzdict_get(bs, &key, struct buzzoutmsg_bstig_chunk_s*);
      }
      else {
         /* Blob stigmergy not found, create it */
         bs = buzzdict_new(10,
                           sizeof(uint16_t),
                           sizeof(buzzdict_t),
                           buzzdict_uint16keyhash,
                           buzzdict_uint16keycmp,
                           buzzoutmsg_bstig_destroy);
         buzzdict_set(vm->outmsgs->chunkbstig, &id, &bs);
      }
      /* look for chunk inside key dict  */
      const struct buzzoutmsg_bstig_chunk_s** c = NULL; 
      buzzdict_t bsc = NULL;
   
      if(e) {
         bsc=*e;
         /* Get the out msg chunk element */
         c = buzzdict_get(bsc, &chunk_index, struct buzzoutmsg_bstig_chunk_s*);
      }
      else {
         bsc = buzzdict_new(10,
                           sizeof(uint16_t),
                           sizeof(struct buzzoutmsg_bstig_chunk_s*),
                           buzzdict_uint16keyhash,
                           buzzdict_uint16keycmp,
                           NULL);
         buzzdict_set(bs, &(key->i.value), &bsc);
      }
      int ctype = -1, cidx = -1; /* No message to remove from the queue */
      if(c){

         /* Yes; if the duplicate is newer than the passed message, nothing to do */
         if((*c)->data->timestamp >= data->timestamp) return;
         /* Is passed message a put message then remove the query */
         if((*c)->type == BUZZMSG_BSTIG_CHUNK_QUERY &&
            type == BUZZMSG_BSTIG_CHUNK_PUT );
         else return;
         /* The duplicate is a query new is a put, store data to remove it later :
         TODO: versioning not completely implemented  */
         ctype = (*c)->type;
         cidx = buzzdarray_find(vm->outmsgs->queues[ctype], buzzoutmsg_bstig_cmp, c);
      }
      /* Create a new message */
      buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
      m->bsc.type = type;
      m->bsc.id = id;
      m->bsc.key = buzzheap_clone(vm, key);
      m->bsc.data = buzzbstig_elem_clone(vm, data);
      m->bsc.blob_size = blob_size;
      m->bsc.chunk_index = chunk_index;
      m->bsc.receiver = receiver;
      m->bsc.cdata = buzzbstig_chunk_new(cdata->hash,cdata->chunk);
      /* Update the dictionary - this also invalidates e */
      buzzdict_set(bsc, &m->bsc.chunk_index, &m);
      if(ctype > -1) {
         /* Remove the entry from the queue */
         buzzdarray_remove(vm->outmsgs->queues[ctype], cidx);
      }
      /* Add a new message to the queue */
      buzzdarray_push(vm->outmsgs->queues[type], &m);
   }
   else{ // P2P message to be sent
      /* Retrive the p2p dict for fast optimisation */ 
      buzzdarray_t rq = NULL;
      /* Look for the receiver queue */
      const buzzdarray_t* prq = buzzdict_get(vm->outmsgs->chunkp2p, &receiver, buzzdarray_t);
      if(!prq) {
         rq =  buzzdarray_new(10,sizeof(buzzoutmsg_t),NULL);
         buzzdict_set(vm->outmsgs->chunkp2p, &receiver, &rq);
      }
      else {
         /* queue found  */
         rq = *prq;
      }
       /* Create a new message */
      buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
      m->bsc.type = BUZZMSG_BSTIG_CHUNK_PUT_P2P;
      m->bsc.id = id;
      m->bsc.key = buzzheap_clone(vm, key);
      m->bsc.data = buzzbstig_elem_clone(vm, data);
      m->bsc.blob_size = blob_size;
      m->bsc.chunk_index = chunk_index;
      m->bsc.receiver = receiver;
      m->bsc.cdata = buzzbstig_chunk_new(cdata->hash,cdata->chunk);
      /* Add a new message to the out msg queue */
      buzzdarray_push(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P], &m);
      /* Add the message to fast optimization queue */
      buzzdarray_push(rq, &m);
   }
}



/****************************************/
/****************************************/

void buzzoutmsg_queue_append_blob_status(buzzvm_t vm,
                                         int type,
                                         uint16_t id,
                                         uint16_t key,
                                         uint8_t  status,
                                         uint16_t requester) { 
   // printf("[rid :%u] Got an append request for blob status\n",vm->robot );
  /* Look for bstig element in id */
   const struct buzzoutmsg_bstig_s** e = NULL;
   /* blob stigmergy to actually use */
   buzzdict_t bs = NULL;
   /* Look for the blob stigmergy */
   const buzzdict_t* tbs = buzzdict_get(vm->outmsgs->bstigstatus, &id, buzzdict_t);
   if(tbs) {
      /* Virtual stigmergy found, look for the key */
      bs = *tbs;
      e = buzzdict_get(bs, &key, struct buzzoutmsg_bstig_s*);
   }
   else {
      /* Virtual stigmergy not found, create it */
      bs = buzzdict_new(10,
                        sizeof(uint16_t),
                        sizeof(struct buzzoutmsg_bstig_s*),
                        buzzdict_uint16keyhash,
                        buzzdict_uint16keycmp,
                        NULL);
      buzzdict_set(vm->outmsgs->bstigstatus, &id, &bs);
   }
   /* Do we have a more recent duplicate? */
   int etype = -1, eidx = -1; /* No message to remove from the queue */
   if(e) {
      // printf(" [RID : %u] Chunk Status message exsists \n",vm->robot);
      etype = type;
      eidx = buzzdarray_find(vm->outmsgs->queues[type], buzzoutmsg_bstig_cmp, e);
   }
   /* Create a new message */
   buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
   m->bss.type = type;
   m->bss.id = id;
   m->bss.key = key;
   m->bss.status = status;
   m->bss.requester = requester;
   // printf("status msg added to queue to out msg : id: %u key %u status %u\n",m->bss.id, m->bss.key,m->bss.status );
            
   /* Update the dictionary - this also invalidates e */
   buzzdict_set(bs, &key, &m);
   if(etype > -1) {
      /* Remove the entry from the queue */
      buzzdarray_remove(vm->outmsgs->queues[type], eidx);
   }
   /* Add a new message to the queue */
   buzzdarray_push(vm->outmsgs->queues[type], &m);
}

/****************************************/
/****************************************/

int buzzoutmsg_chunk_Reloc_cmp(const void* a, const void* b){
   buzzoutmsg_t c = *(buzzoutmsg_t*) a;
   buzzoutmsg_t d = *(buzzoutmsg_t*) b;
   if((uint16_t)c->brl.id == (uint16_t)d->brl.id && (uint16_t)c->brl.key == (uint16_t)d->brl.key && (uint16_t)c->brl.cid == (uint16_t)d->brl.cid && 
      (uint16_t)c->brl.subtype == (uint16_t)d->brl.subtype && (uint16_t)c->brl.receiver == (uint16_t)d->brl.receiver && (uint16_t)c->brl.msg == (uint16_t)d->brl.msg )
      return 0;
   else return -1;
}
void buzzoutmsg_queue_append_chunk_reloc(buzzvm_t vm,
                                         int type,
                                         uint16_t id,
                                         uint16_t key,
                                         uint16_t chunk,
                                         uint16_t receiver,
                                         uint8_t subtype,
                                         uint16_t  msg) { 
   /* Create a new message */
   buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
   m->brl.type = type;
   m->brl.id = id;
   m->brl.key = key;
   m->brl.cid = chunk;
   m->brl.receiver = receiver;
   m->brl.subtype = subtype;
   m->brl.msg = msg;
   /* Check for duplicates :TODO better duplicate management than this, too much time consuming */
   uint16_t index = buzzdarray_find(vm->outmsgs->queues[type],buzzoutmsg_chunk_Reloc_cmp,&m);
   if(index == buzzdarray_size(vm->outmsgs->queues[type])){
      /* Add a new message to the queue */
      buzzdarray_push(vm->outmsgs->queues[type], &m);
   }
   else{
      /* Message already exsist cleanup */
      free(m);
   }
   
}

int buzzoutmsg_blob_request_cmp(const void* a, const void* b){
   buzzoutmsg_t c = *(buzzoutmsg_t*) a;
   buzzoutmsg_t d = *(buzzoutmsg_t*) b;
   if((uint16_t)c->brm.id == (uint16_t)d->brm.id && (uint16_t)c->brm.key == (uint16_t)d->brm.key 
      && (uint16_t)c->brm.receiver == (uint16_t)d->brm.receiver)
      return 0;
   else return -1;
}

void buzzoutmsg_queue_append_blob_request(buzzvm_t vm,
                                         int type,
                                         uint16_t id,
                                         uint16_t key,
                                         uint16_t receiver,
                                         uint16_t sender) { 
   /* Create a new message */
   buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
   m->brm.type = type;
   m->brm.id = id;
   m->brm.key = key;
   m->brm.receiver = receiver;
   m->brm.sender = sender;
   /* Retrive the p2p dict for fast optimisation */ 
   buzzdarray_t rq = NULL;
   /* Look for the receiver queue */
   const buzzdarray_t* prq = buzzdict_get(vm->outmsgs->chunkp2p, &receiver, buzzdarray_t);
   if(!prq) {
      rq =  buzzdarray_new(10,sizeof(buzzoutmsg_t),NULL);
      buzzdict_set(vm->outmsgs->chunkp2p, &receiver, &rq);
   }
   else {
      /* queue found  */
      rq = *prq;
   }
   // printf("GOt a blob request message id %u k %u recev %u send %u\n",id,key,receiver,sender );
   /* Check for duplicates :TODO better duplicate management than this, too much time consuming */
   uint16_t index = buzzdarray_find(vm->outmsgs->bstigrecon,buzzoutmsg_blob_request_cmp,&m);
   if(index == buzzdarray_size(vm->outmsgs->bstigrecon)){
      /* Add a new message to the dup manager */
      buzzdarray_push(vm->outmsgs->bstigrecon, &m);
      /* Add a new message to the out msg queue */
      buzzdarray_insert(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P],0, &m);
      /* Add the message to fast optimization queue */
      buzzdarray_insert(rq,0, &m);
   }
   else{
      /* Message already exsist cleanup */
      free(m);
   }
   
}
/****************************************/
/****************************************/

void buzzoutmsg_queue_append_chunk_removal(buzzvm_t vm,
                                         uint8_t type,
                                         uint16_t id,
                                         uint16_t key,
                                         uint16_t cid,
                                         uint8_t subtype){
   
   uint16_t subtype16 = (uint16_t)subtype; // Hack TODO FIx it 
   
   /* Look for bstig element with id */
   const buzzdict_t* e = NULL;
   /* blob stigmergy to actually use */
   buzzdict_t bs = NULL;
   // printf("[rid : %u]outmsg chunk removal msg : id: %u, key : %u, cid : %u, subtype : %u \n",vm->robot, id,key,cid,subtype );
   /* Look for the blob stigmergy */
   const buzzdict_t* tbc = buzzdict_get(vm->outmsgs->cremovalprotect, &id, buzzdict_t);
   if(tbc) {
      /* Virtual stigmergy found, look for the key */
      bs = *tbc;
      e = buzzdict_get(bs, &key, buzzdict_t);
      //e = buzzdict_get(bs, &key, struct buzzoutmsg_bstig_chunk_s*);
   }
   else {
      /* Blob stigmergy not found, create it */
      bs = buzzdict_new(10,
                        sizeof(uint16_t),
                        sizeof(buzzdict_t),
                        buzzdict_uint16keyhash,
                        buzzdict_uint16keycmp,
                        buzzoutmsg_bstig_destroy);
      buzzdict_set(vm->outmsgs->cremovalprotect, &id, &bs);
   }
   /* look for chunk inside key dict  */
   const buzzdict_t* c = NULL; 
   buzzdict_t bsc = NULL;

   if(e) {
      bsc=*e;
      /* Get the out msg chunk element */
      c = buzzdict_get(bsc, &cid, buzzdict_t);
   }
   else {
      bsc = buzzdict_new(10,
                        sizeof(uint16_t),
                        sizeof(buzzdict_t),
                        buzzdict_uint16keyhash,
                        buzzdict_uint16keycmp,
                        buzzoutmsg_bstig_destroy);
      buzzdict_set(bs, &key, &bsc);
   }
   /* Look for subype in dict */
   const buzzoutmsg_t* sbt = NULL; 
   buzzdict_t bsbt = NULL;
   if(c){
      bsbt =*c;
      sbt = buzzdict_get(bsbt, &subtype16, buzzoutmsg_t);
      
   }
   else{
      bsbt = buzzdict_new(10,
                        sizeof(uint16_t),
                        sizeof(buzzoutmsg_t),
                        buzzdict_uint16keyhash,
                        buzzdict_uint16keycmp,
                        buzzoutmsg_bstig_chunkremoval_destroy);
      buzzdict_set(bsc, &cid, &bsbt);
   }
   if(sbt) return;
   // printf("[Adding a new msg] \n");
   /* Create a new message */
   buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
   m->cr.type = type;
   m->cr.id = id;
   m->cr.key = key;
   m->cr.cid = cid;
   m->cr.subtype = subtype16;
   if(subtype16 < BUZZCHUNK_NEIGHBOUR_QUERY)
      m->cr.timer = MAX_TIME_TO_REMOVE_FLOODING_PROTECTION; // A global message has to be protected from flooding
   else
      m->cr.timer = 0;  // A local message can be removed as soon as it is sent. Anti-flooder takes care of this.
   
   /* Add the entry to dictionary - used for anti-flooding */
   buzzdict_set(bsbt, &subtype16, &m);
   
   /* Add a new message to the queue */
   buzzdarray_push(vm->outmsgs->queues[type], &m);

}


/****************************************/
/****************************************/

void buzzoutmsg_queue_append_bid(buzzvm_t vm,
                                uint8_t type,
                                uint16_t id,
                                uint16_t key,
                                uint16_t bidderid,
                                uint16_t availablespace,
                                uint32_t blob_size,
                                uint32_t hash,
                                uint8_t getter,
                                uint16_t subtype,
                                uint16_t receiver){
   /* Look for bstig element with id */
   const buzzdict_t* e = NULL;
   /* blob stigmergy to actually use */
   buzzdict_t bs = NULL;
   // printf("[rid : %u]outmsg BID msg : id: %u, key : %u, subtype : %u bidderid: %u, bidsize: %u receiver %u \n",vm->robot, id,key,subtype, bidderid,availablespace,receiver );
   /* Look for the blob stigmergy */
   const buzzdict_t* tbc = buzzdict_get(vm->outmsgs->bidprotect, &id, buzzdict_t);
   if(tbc) {
      /* Virtual stigmergy found, look for the key */
      bs = *tbc;
      e = buzzdict_get(bs, &key, buzzdict_t);
      //e = buzzdict_get(bs, &key, struct buzzoutmsg_bstig_chunk_s*);
   }
   else {
      /* Blob stigmergy not found, create it */
      bs = buzzdict_new(10,
                        sizeof(uint16_t),
                        sizeof(buzzdict_t),
                        buzzdict_uint16keyhash,
                        buzzdict_uint16keycmp,
                        buzzoutmsg_bstig_destroy);
      buzzdict_set(vm->outmsgs->bidprotect, &id, &bs);
   }
   /* look for chunk inside key dict  */
   const buzzdict_t* c = NULL; 
   buzzdict_t bsc = NULL;

   if(e) {
      bsc=*e;
      /* Get the out msg chunk element */
      c = buzzdict_get(bsc, &bidderid, buzzdict_t);
   }
   else {
      bsc = buzzdict_new(10,
                        sizeof(uint16_t),
                        sizeof(buzzdict_t),
                        buzzdict_uint16keyhash,
                        buzzdict_uint16keycmp,
                        buzzoutmsg_bstig_destroy);
      buzzdict_set(bs, &key, &bsc);
   }
   /* Look for subype in dict */
   const buzzoutmsg_t* sbt = NULL; 
   buzzdict_t bsbt = NULL;
   if(c){
      bsbt =*c;
      sbt = buzzdict_get(bsbt, &subtype, buzzoutmsg_t);
      
   }
   else{
      bsbt = buzzdict_new(10,
                        sizeof(uint16_t),
                        sizeof(buzzoutmsg_t),
                        buzzdict_uint16keyhash,
                        buzzdict_uint16keycmp,
                        buzzoutmsg_bstig_chunkremoval_destroy);
      buzzdict_set(bsc, &bidderid, &bsbt);
   }
   if(sbt) return;
   // printf("[Adding a new msg] \n");
   /* Create a new message */
   buzzoutmsg_t m = (buzzoutmsg_t)malloc(sizeof(union buzzoutmsg_u));
   m->bid.type = type;
   m->bid.id = id;
   m->bid.key = key;
   m->bid.bidderid = bidderid;
   m->bid.subtype = subtype;
   m->bid.availablespace = availablespace;
   m->bid.blob_size =blob_size;
   m->bid.hash = hash;
   m->bid.getter = BUZZBLOB_GETTER_OPEN;
   m->bid.getter = getter;
   m->bid.timer = MAX_TIME_TO_REMOVE_FLOODING_PROTECTION; // A global message has to be protected from flooding
   m->bid.receiver = receiver;

   if(subtype == BUZZBSTIG_BID_NEW){
      /* Add the entry to dictionary - used for anti-flooding */
      buzzdict_set(bsbt, &subtype, &m);
      /* Add a new message to the queue */
      buzzdarray_push(vm->outmsgs->queues[type], &m);
      // printf("added to broadcast queue\n");
   }
   else{
      /* Retrive the p2p dict for fast optimisation */ 
      buzzdarray_t rq = NULL;
      /* Look for the receiver queue */
      const buzzdarray_t* prq = buzzdict_get(vm->outmsgs->chunkp2p, &receiver, buzzdarray_t);
      if(!prq) {
         rq =  buzzdarray_new(10,sizeof(buzzoutmsg_t),NULL);
         buzzdict_set(vm->outmsgs->chunkp2p, &receiver, &rq);
      }
      else {
         /* queue found  */
         rq = *prq;
      }     
      /* Add a new message to the out msg queue */
      buzzdarray_insert(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P],0, &m);
      /* Add the message to fast optimization queue */
      buzzdarray_insert(rq,0, &m);
      // printf("added to p2p queue\n");

   }

}
/****************************************/
/****************************************/

int buzzoutmsg_check_chunk_put_msg(buzzvm_t vm,
                                   uint16_t id,
                                   uint16_t key, 
                                   uint16_t chunk_index ){
   /* Check out msg queue for put message */
   const buzzdict_t* tbc = buzzdict_get(vm->outmsgs->chunkbstig, &id, buzzdict_t);
   if(tbc){
      const buzzdict_t* e = buzzdict_get(*tbc, &key, buzzdict_t);
      if(e){
         const struct buzzoutmsg_bstig_chunk_s** c =
                  buzzdict_get(*e, &chunk_index, struct buzzoutmsg_bstig_chunk_s*);
         if(c) return 1;
      }
   }
   return 0;
}

/****************************************/
/****************************************/
void buzzoutmsg_remove_chunk_put_msg_for_each(const void* key, void* data, void* params){
   buzzvm_t vm = *(buzzvm_t*)params;
   struct buzzoutmsg_bstig_chunk_s**  c = (struct buzzoutmsg_bstig_chunk_s**)data;
   /* strip the messages from queue and delete */
   int cidx = buzzdarray_find(vm->outmsgs->queues[(*c)->type], buzzoutmsg_bstig_cmp, c);
   /* Remove the entry from the queue */
   buzzdarray_remove(vm->outmsgs->queues[(*c)->type], cidx);
}

void buzzoutmsg_remove_chunk_put_msg(buzzvm_t vm,
                                    uint16_t id,
                                    uint16_t key){
   /* Check out msg queue for put message  and delete them*/
   const buzzdict_t* tbc = buzzdict_get(vm->outmsgs->chunkbstig, &id, buzzdict_t);
   if(tbc){
      const buzzdict_t* e = buzzdict_get(*tbc, &key, buzzdict_t);
      if(e){
         buzzdict_foreach(*e, buzzoutmsg_remove_chunk_put_msg_for_each, &vm);
         buzzdict_remove(*tbc,&key);
         // if(buzzdict_get(*tbc, &key, buzzdict_t)) printf("after removal dict exsist\n");
         // else printf("Out msg dict removed succesfully \n");
         // printf("out msg put chunk size : %u\n",buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT]) );
      }

   }
}

/****************************************/
/****************************************/

struct buzzoutmsg_antiflood_removelist_param
{
   uint16_t id;
   uint16_t key;      // Here it is assumed blobs are inserted in an assending order
   uint16_t cid;
   uint16_t subtype;
};

struct buzzoutmsg_antiflood_blackboard_param
{
   uint16_t id;
   uint16_t key;      // Here it is assumed blobs are inserted in an assending order
   uint16_t cid;
   buzzdarray_t a;
};

void buzzoutmsg_anitflooding_darray_destroy(uint32_t pos, void* data, void* params) {
   struct buzzoutmsg_antiflood_removelist_param* m = *(struct buzzoutmsg_antiflood_removelist_param**) data;
   free(m);
}

void buzzoutmsg_anitflooding_subtypeloop(const void* key, void* data, void* params){ // chunk loop 
   buzzoutmsg_t m=*(buzzoutmsg_t*) data;
   struct buzzoutmsg_antiflood_blackboard_param* a = (struct buzzoutmsg_antiflood_blackboard_param*)params;
   uint16_t subtype = *(uint16_t*) key;
   // printf("[subtype : %u, timer : %u ] ", subtype, m->cr.timer);
   if(m->type == BUZZMSG_BSTIG_CHUNK_REMOVED){
      if(m->cr.timer <= 0 ){
         /* This entry has to be removed add to the array */
         struct buzzoutmsg_antiflood_removelist_param* e = 
            (struct buzzoutmsg_antiflood_removelist_param*)malloc(sizeof(struct buzzoutmsg_antiflood_removelist_param));
         e->id = a->id;
         e->key = a->key;
         e->cid = a->cid;
         e->subtype = subtype;
         buzzdarray_push((a)->a,&e);
      }
      else{
         m->cr.timer--;
      }
   }
   else if (m->type == BUZZMSG_BSTIG_BLOB_BID){
      if(m->bid.timer <= 0 ){
         /* This entry has to be removed add to the array */
         struct buzzoutmsg_antiflood_removelist_param* e = 
            (struct buzzoutmsg_antiflood_removelist_param*)malloc(sizeof(struct buzzoutmsg_antiflood_removelist_param));
         e->id = a->id;
         e->key = a->key;
         e->cid = a->cid;
         e->subtype = subtype;
         buzzdarray_push((a)->a,&e);
      }
      else{
         m->bid.timer--;
      }

   }

}

void buzzoutmsg_anitflooding_chunkloop(const void* key, void* data, void* params){ // key loop 
   buzzdict_t m_dict = *(buzzdict_t*) data;
   struct buzzoutmsg_antiflood_blackboard_param* a = (struct buzzoutmsg_antiflood_blackboard_param*)params;
   a->cid= *(uint16_t*)key;
   // printf("chunk : %u ", *(uint16_t*)key);
   buzzdict_foreach(m_dict, buzzoutmsg_anitflooding_subtypeloop, params);
}

void buzzoutmsg_anitflooding_keyloop(const void* key, void* data, void* params){ // key loop 
   buzzdict_t m_dict = *(buzzdict_t*) data;
   struct buzzoutmsg_antiflood_blackboard_param* a = (struct buzzoutmsg_antiflood_blackboard_param*)params;
   a->key= *(uint16_t*)key;
   // printf("key : %u ", *(uint16_t*)key);
   buzzdict_foreach(m_dict, buzzoutmsg_anitflooding_chunkloop, params);
}

void buzzoutmsg_anitflooding_idloop(const void* key, void* data, void* params){ // Id loop 
   buzzdict_t m_dict = *(buzzdict_t*) data;
   struct buzzoutmsg_antiflood_blackboard_param* a = (struct buzzoutmsg_antiflood_blackboard_param*)params;
   a->id= *(uint16_t*)key;
   // printf("id : %u ", *(uint16_t*)key);
   buzzdict_foreach(m_dict, buzzoutmsg_anitflooding_keyloop, params);
}

void buzzoutmsg_update_antiflooding_entry(buzzvm_t vm){
   /* create a darray for storing the entries to remove */
   // printf("rid %u ",vm->robot);
   buzzdarray_t a = buzzdarray_new(5, sizeof(struct buzzoutmsg_antiflood_removelist_param*), 
                                          buzzoutmsg_anitflooding_darray_destroy);
   struct buzzoutmsg_antiflood_blackboard_param p = {.id=0, .key=0, .cid=0, .a=a};
   buzzdict_foreach(vm->outmsgs->cremovalprotect, buzzoutmsg_anitflooding_idloop, &p);   
   for(uint16_t i=0; i < buzzdarray_size(a); i++){
      const struct buzzoutmsg_antiflood_removelist_param* e = buzzdarray_get(a,i,struct buzzoutmsg_antiflood_removelist_param*);
      // printf("array to remove: index : %u id : %u , key: %u, cid %u, subtype: %u\n",i ,e->id,e->key,e->cid, e->subtype );
      const buzzdict_t* trid = buzzdict_get(vm->outmsgs->cremovalprotect, &(e->id), buzzdict_t);
      const buzzdict_t* trkey = buzzdict_get(*trid, &(e->key), buzzdict_t);
      const buzzdict_t* trcid = buzzdict_get(*trkey, &(e->cid), buzzdict_t);
      const buzzoutmsg_t*  sbt = buzzdict_get(*trcid, &(e->subtype), buzzoutmsg_t);
      /* strip the messages from dict and delete, if they donot exsist in outmsg queue */
      int cidx = buzzdarray_find(vm->outmsgs->queues[(*sbt)->type], buzzoutmsg_bstig_cmp, sbt);
      if(cidx == buzzdarray_size(vm->outmsgs->queues[(*sbt)->type])){
         buzzdict_remove(*trcid, &(e->subtype));
      }
   }
   buzzdarray_destroy(&a);

   /* Bidder anti-flooding management */
   a = buzzdarray_new(5, sizeof(struct buzzoutmsg_antiflood_removelist_param*), 
                                          buzzoutmsg_anitflooding_darray_destroy);
   struct buzzoutmsg_antiflood_blackboard_param  pr = {.id=0, .key=0, .cid=0, .a=a};
   buzzdict_foreach(vm->outmsgs->bidprotect, buzzoutmsg_anitflooding_idloop, &pr);   
   for(uint16_t i=0; i < buzzdarray_size(a); i++){
      const struct buzzoutmsg_antiflood_removelist_param* e = buzzdarray_get(a,i,struct buzzoutmsg_antiflood_removelist_param*);
      // printf("array to remove: index : %u id : %u , key: %u, cid %u, subtype: %u\n",i ,e->id,e->key,e->cid, e->subtype );
      const buzzdict_t* trid = buzzdict_get(vm->outmsgs->bidprotect, &(e->id), buzzdict_t);
      const buzzdict_t* trkey = buzzdict_get(*trid, &(e->key), buzzdict_t);
      const buzzdict_t* trcid = buzzdict_get(*trkey, &(e->cid), buzzdict_t);
      const buzzoutmsg_t*  sbt = buzzdict_get(*trcid, &(e->subtype), buzzoutmsg_t);
      /* strip the messages from dict and delete, if they donot exsist in outmsg queue */
      int cidx = buzzdarray_find(vm->outmsgs->queues[(*sbt)->type], buzzoutmsg_bstig_cmp, sbt);
      if(cidx == buzzdarray_size(vm->outmsgs->queues[(*sbt)->type])){
         buzzdict_remove(*trcid, &(e->subtype));
      }
   }
   buzzdarray_destroy(&a);
   // printf("\n");
}

/****************************************/
/****************************************/

buzzmsg_payload_t buzzoutmsg_queue_first(buzzvm_t vm) {
   if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BROADCAST])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BROADCAST],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_BROADCAST);
      buzzobj_serialize(m, f->bc.topic);
      buzzobj_serialize(m, f->bc.value);
      /* Return message */
      return m;
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_SWARM_LIST])) {
      uint16_t i;
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_SWARM_LIST],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_SWARM_LIST);
      buzzmsg_serialize_u16(m, f->sw.size);
      for(i = 0; i < f->sw.size; ++i) {
         buzzmsg_serialize_u16(m, f->sw.ids[i]);
      }
      /* Return message */
      return m;      
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_VSTIG_PUT])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_VSTIG_PUT],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_VSTIG_PUT);
      buzzmsg_serialize_u16(m, f->vs.id);
      buzzvstig_elem_serialize(m, f->vs.key, f->vs.data);
      /* Return message */
      return m;
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_VSTIG_QUERY])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_VSTIG_QUERY],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_VSTIG_QUERY);
      buzzmsg_serialize_u16(m, f->vs.id);
      buzzvstig_elem_serialize(m, f->vs.key, f->vs.data);
      /* Return message */
      return m;
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_PUT])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_PUT],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_PUT);
      buzzmsg_serialize_u8(m, f->bs.blob_entry);
      if(f->bs.blob_entry){
         buzzmsg_serialize_u32(m, f->bs.blob_size);   
      }
      buzzmsg_serialize_u16(m, f->bs.id);
      buzzbstig_elem_serialize(m, f->bs.key, f->bs.data);
      /* Return message */
      return m;
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_QUERY])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_QUERY],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_QUERY);
      buzzmsg_serialize_u8(m, f->bs.blob_entry);
      if(f->bs.blob_entry){
         buzzmsg_serialize_u32(m, f->bs.blob_size);   
      }
      buzzmsg_serialize_u16(m, f->bs.id);
      buzzbstig_elem_serialize(m, f->bs.key, f->bs.data);
      /* Return message */
      return m;
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_SWARM_JOIN])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_SWARM_JOIN],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(5);
      buzzmsg_serialize_u8(m, BUZZMSG_SWARM_JOIN);
      buzzmsg_serialize_u16(m, f->sw.ids[0]);
      /* Return message */
      return m;      
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_SWARM_LEAVE])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_SWARM_LEAVE],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(5);
      buzzmsg_serialize_u8(m, BUZZMSG_SWARM_LEAVE);
      buzzmsg_serialize_u16(m, f->sw.ids[0]);
      /* Return message */
      return m;      
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_STATUS])) {
     /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_STATUS],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_STATUS);
      buzzmsg_serialize_u16(m, f->bss.id);   
      buzzmsg_serialize_u16(m, f->bss.key);
      buzzmsg_serialize_u8(m, f->bss.status);
      buzzmsg_serialize_u16(m, f->bss.requester);
      /* Return message */
      return m;
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_BLOB_BID])) {
     /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_BLOB_BID],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_BLOB_BID);
      buzzmsg_serialize_u16(m, f->bid.id);   
      buzzmsg_serialize_u16(m, f->bid.key);
      buzzmsg_serialize_u16(m, f->bid.bidderid);
      uint8_t subtype8 = f->bid.subtype;
      buzzmsg_serialize_u8(m, subtype8);
      if(subtype8 == BUZZBSTIG_BID_NEW){
         buzzmsg_serialize_u32(m,f->bid.blob_size);
         buzzmsg_serialize_u32(m,f->bid.hash);
      }
      else if(subtype8 == BUZZBSITG_BID_REPLY){
         buzzmsg_serialize_u8(m, f->bid.getter);
         buzzmsg_serialize_u16(m, f->bid.availablespace);
      }
      else if (subtype8 == BUZZCHUNK_BID_FORCE_ALLOCATION ||
               subtype8 == BUZZCHUNK_BID_FORCE_ALLOCATION_REJECT){
         uint16_t removeid = f->bid.blob_size;
         uint16_t removekey = f->bid.hash;
         buzzmsg_serialize_u16(m,removeid);
         buzzmsg_serialize_u16(m,removekey);
         buzzmsg_serialize_u16(m, f->bid.availablespace);
      }
      else 
         buzzmsg_serialize_u16(m, f->bid.availablespace);
      // printf("[RID: %u] sent a bidder msg id: %u, key: %u, bidderid : %u, subtype: %u \n",vm->robot,f->bid.id, f->bid.key, f->bid.bidderid, f->bid.subtype );
      // printf("[RID : %u] queue size : %u\n",vm->robot, buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_BLOB_BID]));
      
      /* Return message */
      return m;
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_REMOVED])) {
     /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_REMOVED],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_CHUNK_REMOVED);
      buzzmsg_serialize_u16(m, f->cr.id);   
      buzzmsg_serialize_u16(m, f->cr.key);
      buzzmsg_serialize_u16(m, f->cr.cid);
      uint8_t subtype8 = f->cr.subtype;
      buzzmsg_serialize_u8(m, subtype8);
      // printf("[RID: %u] sent a chunk removal msg id: %u, key: %u, cid : %u, subtype: %u \n",vm->robot,f->cr.id, f->cr.key, f->cr.cid, f->cr.subtype );
      // printf("[RID : %u] queue size : %u\n",vm->robot, buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_REMOVED]));
      
      /* Return message */
      return m;
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_STATUS_QUERY])) {
     /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_STATUS_QUERY],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_CHUNK_STATUS_QUERY);
      buzzmsg_serialize_u16(m, f->brl.receiver);
      buzzmsg_serialize_u16(m, f->brl.id);   
      buzzmsg_serialize_u16(m, f->brl.key);
      buzzmsg_serialize_u16(m, f->brl.cid);
      buzzmsg_serialize_u8(m, f->brl.subtype);
      buzzmsg_serialize_u16(m, f->brl.msg);
      // if(f->brl.subtype ==BUZZRELOCATION_REQUEST )
      // printf("BUZZRELOCATION_REQUEST out msg sent from queue : receiver: %u cid %u msg %u\n",f->brl.receiver, f->brl.cid, f->brl.msg );
      // if(f->brl.subtype ==BUZZRELOCATION_RESPONCE )
      // printf("BUZZRELOCATION_RESPONCE out msg sent from queue : receiver: %u cid %u msg %u\n",f->brl.receiver, f->brl.cid, f->brl.msg );
   
      /* Return message */
      return m;
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_CHUNK_PUT);
      buzzmsg_serialize_u32(m, f->bsc.blob_size);   
      buzzmsg_serialize_u16(m, f->bsc.id);
      buzzbstig_elem_serialize(m, f->bsc.key, f->bsc.data);
      buzzmsg_serialize_u16(m, f->bsc.chunk_index);
      buzzbstig_chunk_serialize(m, f->bsc.cdata);
      /* Return message */
      return m;
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_QUERY])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_QUERY],
                                      0, buzzoutmsg_t);
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_CHUNK_QUERY);
      buzzmsg_serialize_u32(m, f->bsc.blob_size);   
      buzzmsg_serialize_u16(m, f->bsc.id);
      buzzbstig_elem_serialize(m, f->bsc.key, f->bsc.data);
      buzzmsg_serialize_u16(m, f->bsc.chunk_index);
      /* Return message */
      return m;
   }
   /* Empty queue */
   return NULL;
}

/****************************************/
/****************************************/

void buzzoutmsg_queue_next(buzzvm_t vm) {
   if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BROADCAST])) {
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BROADCAST], 0);
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_SWARM_LIST])) {
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_SWARM_LIST], 0);
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_VSTIG_PUT])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_VSTIG_PUT],
                                      0, buzzoutmsg_t);
      /* Remove the element in the vstig dictionary */
      buzzdict_remove(
         *buzzdict_get(vm->outmsgs->vstig, &f->vs.id, buzzdict_t),
         &f->vs.key);
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_VSTIG_PUT], 0);
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_VSTIG_QUERY])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_VSTIG_QUERY],
                                      0, buzzoutmsg_t);
      /* Remove the element in the vstig dictionary */
      buzzdict_remove(
         *buzzdict_get(vm->outmsgs->vstig, &f->vs.id, buzzdict_t),
         &f->vs.key);
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_VSTIG_QUERY], 0);
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_PUT])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_PUT],
                                      0, buzzoutmsg_t);
      /* Remove the element in the bstig dictionary */
         buzzdict_remove(
         *buzzdict_get(vm->outmsgs->bstig, &f->bs.id, buzzdict_t),
         &f->bs.key);
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_PUT], 0);
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_QUERY])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_QUERY],
                                      0, buzzoutmsg_t);
      /* Remove the element in the bstig dictionary */
         buzzdict_remove(
            *buzzdict_get(vm->outmsgs->bstig, &f->bs.id, buzzdict_t),
            &f->bs.key);
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_QUERY], 0);
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_SWARM_JOIN])) {
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_SWARM_JOIN], 0);
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_SWARM_LEAVE])) {
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_SWARM_LEAVE], 0);
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_STATUS])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_STATUS],
                                      0, buzzoutmsg_t);
      /* Remove the element in the bstig status dictionary */
         buzzdict_remove(
            *buzzdict_get(vm->outmsgs->bstigstatus, &(f->bss.id), buzzdict_t)
               , &(f->bss.key));
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_STATUS], 0);
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_BLOB_BID])) {
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_BLOB_BID], 0);
      // printf("[RID : %u] Removed bid msg queue size : %u \n",vm->robot, buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_BLOB_BID]));
      
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_REMOVED])) {
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_REMOVED], 0);
      // printf("[RID : %u]Removed chunk remove msg queue size : %u \n",vm->robot, buzzdarray_size(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_REMOVED]));
      
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_STATUS_QUERY])) {
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_STATUS_QUERY], 0);
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT],
                                      0, buzzoutmsg_t);
      
      /* Remove the element in the bstig chunk dictionary */
         buzzdict_remove(
            *buzzdict_get(*buzzdict_get(vm->outmsgs->chunkbstig, &(f->bsc.id), buzzdict_t)
            , &(f->bsc.key->i.value), buzzdict_t), &(f->bsc.chunk_index));
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT], 0);
   }
   else if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_QUERY])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_QUERY],
                                      0, buzzoutmsg_t);
      /* Remove the element in the bstig chunk dictionary */
         buzzdict_remove(
            *buzzdict_get(*buzzdict_get(vm->outmsgs->chunkbstig, &f->bsc.id, buzzdict_t)
            , &f->bsc.key->i.value, buzzdict_t), &f->bsc.chunk_index);
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_QUERY], 0);
   }
}

/****************************************/
/****************************************/

buzzmsg_payload_t buzzoutmsg_chunk_queue_first(buzzvm_t vm){
   if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT])) {
      
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT],
                                      0, buzzoutmsg_t);
     
      /* Make a new message */
      buzzmsg_payload_t m = buzzmsg_payload_new(10);
      buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_CHUNK_PUT);
      buzzmsg_serialize_u32(m, f->bsc.blob_size);   
      buzzmsg_serialize_u16(m, f->bsc.id);
      buzzbstig_elem_serialize(m, f->bsc.key, f->bsc.data);
      buzzmsg_serialize_u16(m, f->bsc.chunk_index);
      buzzbstig_chunk_serialize(m, f->bsc.cdata);
      /* Return message */
      return m;
   }
   /* Empty queue */
   return NULL;
}

/****************************************/
/****************************************/

buzzp2poutmsg_payload_t buzzoutmsg_p2p_chunk_queue_first(buzzvm_t vm){
   if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P])) {
      
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P],
                                      0, buzzoutmsg_t);
      if(f->type == BUZZMSG_BSTIG_CHUNK_PUT_P2P){                              
         /* Make a new message */
         buzzmsg_payload_t m = buzzmsg_payload_new(10);
         buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_CHUNK_PUT);
         buzzmsg_serialize_u32(m, f->bsc.blob_size);   
         buzzmsg_serialize_u16(m, f->bsc.id);
         buzzbstig_elem_serialize(m, f->bsc.key, f->bsc.data);
         buzzmsg_serialize_u16(m, f->bsc.chunk_index);
         buzzbstig_chunk_serialize(m, f->bsc.cdata);
         buzzp2poutmsg_payload_t p2pm = (buzzp2poutmsg_payload_t)malloc(sizeof(struct buzzp2poutmsg_payload_s));
         p2pm->msg = m;
         p2pm->receiver = f->bsc.receiver;
         return p2pm;
      }
      else if(f->type == BUZZMSG_BSTIG_BLOB_REQUEST){
         /* Make a new message */
         buzzmsg_payload_t m = buzzmsg_payload_new(10);
         buzzmsg_serialize_u8(m,  BUZZMSG_BSTIG_BLOB_REQUEST);
         buzzmsg_serialize_u16(m, f->brm.id);   
         buzzmsg_serialize_u16(m, f->brm.key);
         buzzmsg_serialize_u16(m, f->brm.sender);
         // printf("GOt a blob request message id %u k %u recev %u send %u\n",f->brm.id,f->brm.key,f->brm.receiver,f->brm.sender );
   
         buzzp2poutmsg_payload_t p2pm = (buzzp2poutmsg_payload_t)malloc(sizeof(struct buzzp2poutmsg_payload_s));
         p2pm->msg = m;
         p2pm->receiver = f->brm.receiver;
         return p2pm;
      }
      else if(f->type == BUZZMSG_BSTIG_BLOB_BID){
         /* Make a new message */
         buzzmsg_payload_t m = buzzmsg_payload_new(10);
         buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_BLOB_BID);
         buzzmsg_serialize_u16(m, f->bid.id);   
         buzzmsg_serialize_u16(m, f->bid.key);
         buzzmsg_serialize_u16(m, f->bid.bidderid);
         uint8_t subtype8 = f->bid.subtype;
         buzzmsg_serialize_u8(m, subtype8);
         if(subtype8 == BUZZBSITG_BID_REPLY){
            buzzmsg_serialize_u8(m, f->bid.getter);
            buzzmsg_serialize_u16(m, f->bid.availablespace);
         }
         else if (subtype8 == BUZZCHUNK_BID_FORCE_ALLOCATION ||
                  subtype8 == BUZZCHUNK_BID_FORCE_ALLOCATION_REJECT){
            uint16_t removeid = f->bid.blob_size;
            uint16_t removekey = f->bid.hash;
            buzzmsg_serialize_u16(m,removeid);
            buzzmsg_serialize_u16(m,removekey);
            buzzmsg_serialize_u16(m, f->bid.availablespace);
         }
         else {
            buzzmsg_serialize_u16(m, f->bid.availablespace);
         }

         buzzp2poutmsg_payload_t p2pm = (buzzp2poutmsg_payload_t)malloc(sizeof(struct buzzp2poutmsg_payload_s));
         p2pm->msg = m;
         p2pm->receiver = f->bid.receiver;
         return p2pm;
      }
      /* Return message */
      
   }
   /* Empty queue */
   return NULL;
}
void buzzoutmsg_getp2pqueue_destroy(uint32_t pos, void* data, void* params){
 buzzp2poutmsg_payload_t m = *(buzzp2poutmsg_payload_t*) data;
 free(m);  
}

void buzzoutmsg_getp2pqueue_foreach(const void* key, void* data, void* params){
   buzzdarray_t p = *(buzzdarray_t*) params;
   buzzdarray_t p2pq = *(buzzdarray_t*) data;
   if(buzzdarray_size(p2pq)>0){
      buzzp2poutmsg_payload_t p2pm = (buzzp2poutmsg_payload_t)malloc(sizeof(struct buzzp2poutmsg_payload_s));
      p2pm->msg = p2pq;
      p2pm->receiver = *(uint16_t*)key;
      buzzdarray_push(p,&p2pm);
   }      
}

buzzdarray_t buzzoutmsg_getp2pqueue(buzzvm_t vm){
   /* create a darray for storing the id and key in accending order of priority */
   buzzdarray_t p = buzzdarray_new(10, sizeof(buzzp2poutmsg_payload_t), 
                                          buzzoutmsg_getp2pqueue_destroy);
   /* Update the priority list of blobs */
   buzzdict_foreach(vm->outmsgs->chunkp2p, buzzoutmsg_getp2pqueue_foreach, &p);
   return p;
}
/****************************************/
/****************************************/
void buzzoutmsg_p2p_chunk_queue_next(buzzvm_t vm){
   if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P],
                                      0, buzzoutmsg_t);
      if(f->type == BUZZMSG_BSTIG_CHUNK_PUT_P2P){                              
         /* Find the index of this message in receiver id queue and remove*/
         const buzzdarray_t* rqp = buzzdict_get(vm->outmsgs->chunkp2p, &(f->bsc.receiver), buzzdarray_t);
         uint32_t index = buzzdarray_find(*rqp,buzzoutmsg_bstig_cmp, &f);
         buzzdarray_remove(*rqp, index);
         /* Remove the first message in the queue */
         buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P], 0);
      }
      else if(f->type == BUZZMSG_BSTIG_BLOB_REQUEST){
         /* Find the index of this message in receiver id queue and remove*/
         const buzzdarray_t* rqp = buzzdict_get(vm->outmsgs->chunkp2p, &(f->brm.receiver), buzzdarray_t);
         uint32_t index = buzzdarray_find(*rqp,buzzoutmsg_bstig_cmp, &f);
         buzzdarray_remove(*rqp, index);
         /* Remove it from the duplicate manger */
         index = buzzdarray_find(vm->outmsgs->bstigrecon,buzzoutmsg_bstig_cmp, &f);
         buzzdarray_remove(vm->outmsgs->bstigrecon, index);
         /* Remove the first message in the queue */
         buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P], 0);

      }
      else if(f->type == BUZZMSG_BSTIG_BLOB_BID){
         /* Find the index of this message in receiver id queue and remove*/
         const buzzdarray_t* rqp = buzzdict_get(vm->outmsgs->chunkp2p, &(f->bid.receiver), buzzdarray_t);
         uint32_t index = buzzdarray_find(*rqp,buzzoutmsg_bstig_cmp, &f);
         buzzdarray_remove(*rqp, index);
         /* Remove the first message in the queue */
         buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P], 0);

      }
   }

}

/****************************************/
/****************************************/
void buzzoutmsg_chunk_queue_next(buzzvm_t vm){
   if(!buzzdarray_isempty(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT])) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT],
                                      0, buzzoutmsg_t);
      
      /* Remove the element in the bstig dictionary */
         buzzdict_remove(
            *buzzdict_get(*buzzdict_get(vm->outmsgs->chunkbstig, &(f->bsc.id), buzzdict_t)
            , &(f->bsc.key->i.value), buzzdict_t), &f->bsc.chunk_index);
      /* Remove the first message in the queue */
      buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT], 0);
   }

}

buzzmsg_payload_t buzzoutmsg_p2p_chunk_receiver_queue_first(buzzvm_t vm, uint16_t receiver){
   /* Find the next message for the same receiver */
   const buzzdarray_t* rqp = buzzdict_get(vm->outmsgs->chunkp2p, &(receiver), buzzdarray_t);
   buzzdarray_t rq = *rqp;
   if(!buzzdarray_isempty(rq)) {
      
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(rq,
                                      0, buzzoutmsg_t);

      if(f->type == BUZZMSG_BSTIG_CHUNK_PUT_P2P){
         /* Make a new message */
         buzzmsg_payload_t m = buzzmsg_payload_new(10);
         buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_CHUNK_PUT);
         buzzmsg_serialize_u32(m, f->bsc.blob_size);   
         buzzmsg_serialize_u16(m, f->bsc.id);
         buzzbstig_elem_serialize(m, f->bsc.key, f->bsc.data);
         buzzmsg_serialize_u16(m, f->bsc.chunk_index);
         buzzbstig_chunk_serialize(m, f->bsc.cdata);
         /* Return message */
         return m;
      }
      else if(f->type == BUZZMSG_BSTIG_BLOB_REQUEST){
         /* Make a new message */
         buzzmsg_payload_t m = buzzmsg_payload_new(10);
         buzzmsg_serialize_u8(m,  BUZZMSG_BSTIG_BLOB_REQUEST);
         buzzmsg_serialize_u16(m, f->brm.id);   
         buzzmsg_serialize_u16(m, f->brm.key);
         buzzmsg_serialize_u16(m, f->brm.sender);
         /* Return message */
         return m;
      }
      else if(f->type == BUZZMSG_BSTIG_BLOB_BID){
         /* Make a new message */
         buzzmsg_payload_t m = buzzmsg_payload_new(10);
         buzzmsg_serialize_u8(m, BUZZMSG_BSTIG_BLOB_BID);
         buzzmsg_serialize_u16(m, f->bid.id);   
         buzzmsg_serialize_u16(m, f->bid.key);
         buzzmsg_serialize_u16(m, f->bid.bidderid);
         uint8_t subtype8 = f->bid.subtype;
         buzzmsg_serialize_u8(m, subtype8);
         if(subtype8 == BUZZBSITG_BID_REPLY){
            buzzmsg_serialize_u8(m, f->bid.getter);
            buzzmsg_serialize_u16(m, f->bid.availablespace);
         }
         else if (subtype8 == BUZZCHUNK_BID_FORCE_ALLOCATION ||
                  subtype8 == BUZZCHUNK_BID_FORCE_ALLOCATION_REJECT){
            uint16_t removeid = f->bid.blob_size;
            uint16_t removekey = f->bid.hash;
            buzzmsg_serialize_u16(m,removeid);
            buzzmsg_serialize_u16(m,removekey);
            buzzmsg_serialize_u16(m, f->bid.availablespace);
         }
         else {
            buzzmsg_serialize_u16(m, f->bid.availablespace);
         }
         return m;
      }
   }
   /* Empty queue */
   return NULL;
}

void buzzoutmsg_p2p_chunk_receiver_queue_next(buzzvm_t vm, uint16_t receiver){
   /* Find the next message for the same receiver */
   const buzzdarray_t* rqp = buzzdict_get(vm->outmsgs->chunkp2p, &(receiver), buzzdarray_t);
   buzzdarray_t rq = *rqp;
   if(!buzzdarray_isempty(rq)) {
      /* Take the first message in the queue */
      buzzoutmsg_t f = buzzdarray_get(rq,
                                      0, buzzoutmsg_t);
      if(f->type == BUZZMSG_BSTIG_CHUNK_PUT_P2P){
         uint32_t index = buzzdarray_find(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P],buzzoutmsg_bstig_cmp, &f);
         buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P], index);
         /* Remove the first message in the receiver queue */
         buzzdarray_remove(rq, 0);
      }
      else if(f->type == BUZZMSG_BSTIG_BLOB_REQUEST){
         /* Remove it from the duplicate manger */
         uint32_t index = buzzdarray_find(vm->outmsgs->bstigrecon,buzzoutmsg_bstig_cmp, &f);
         buzzdarray_remove(vm->outmsgs->bstigrecon, index);
         /* Remove it from the p2p queue */
         index = buzzdarray_find(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P],buzzoutmsg_bstig_cmp, &f);
         buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P], index);
         /* Remove the first message in the receiver queue */
         buzzdarray_remove(rq, 0);
      }
      else if(f->type == BUZZMSG_BSTIG_BLOB_BID){
         /* Remove it from the p2p queue */
         uint32_t index = buzzdarray_find(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P],buzzoutmsg_bstig_cmp, &f);
         buzzdarray_remove(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P], index);
         /* Remove the first message in the receiver queue */
         buzzdarray_remove(rq, 0);
      }
   }

}
/****************************************/
/****************************************/

void buzzoutmsg_broadcast_mark(uint32_t pos, void* data, void* params) {
   struct buzzoutmsg_broadcast_s* msg = *(struct buzzoutmsg_broadcast_s**)data;
   buzzvm_t vm = (buzzvm_t)params;
   buzzheap_obj_mark(msg->topic, vm);
   buzzheap_obj_mark(msg->value, vm);
}

void buzzoutmsg_vstig_mark(uint32_t pos, void* data, void* params) {
   struct buzzoutmsg_vstig_s* msg = *(struct buzzoutmsg_vstig_s**)data;
   buzzvm_t vm = (buzzvm_t)params;
   buzzheap_vstigobj_mark(&msg->key, &msg->data, vm);
}

void buzzoutmsg_bstig_mark(uint32_t pos, void* data, void* params) {
   struct buzzoutmsg_bstig_s* msg = *(struct buzzoutmsg_bstig_s**)data;
   buzzvm_t vm = (buzzvm_t)params;
   buzzheap_bstigobj_mark(&msg->key, &msg->data, vm);
}

void buzzoutmsg_p2p_mark(uint32_t pos, void* data, void* params) {
   buzzoutmsg_t msg = *(buzzoutmsg_t*)data;
   if(msg->type == BUZZMSG_BSTIG_CHUNK_PUT_P2P){
      buzzvm_t vm = (buzzvm_t)params;
      buzzheap_bstigobj_mark(&(msg->bsc.key), &(msg->bsc.data), vm);
   }
}

void buzzoutmsg_gc(struct buzzvm_s* vm) {
   /* Go through all the broadcast topic strings and mark them */
   buzzdarray_foreach(vm->outmsgs->queues[BUZZMSG_BROADCAST],
                      buzzoutmsg_broadcast_mark,
                      vm);
   /* Go through all the vstig keys and values and mark them */
   buzzdarray_foreach(vm->outmsgs->queues[BUZZMSG_VSTIG_PUT],
                      buzzoutmsg_vstig_mark,
                      vm);
   buzzdarray_foreach(vm->outmsgs->queues[BUZZMSG_VSTIG_QUERY],
                      buzzoutmsg_vstig_mark,
                      vm);
   /* Go through all the bstig keys and values and mark them */
   buzzdarray_foreach(vm->outmsgs->queues[BUZZMSG_BSTIG_PUT],
                      buzzoutmsg_bstig_mark,
                      vm);
   buzzdarray_foreach(vm->outmsgs->queues[BUZZMSG_BSTIG_QUERY],
                      buzzoutmsg_bstig_mark,
                      vm);
    buzzdarray_foreach(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_QUERY],
                      buzzoutmsg_bstig_mark,
                      vm);
   buzzdarray_foreach(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT],
                      buzzoutmsg_bstig_mark,
                      vm);
   buzzdarray_foreach(vm->outmsgs->queues[BUZZMSG_BSTIG_CHUNK_PUT_P2P],
                      buzzoutmsg_p2p_mark,
                      vm);
   }


/* For data logging */



/****************************************/
/****************************************/
