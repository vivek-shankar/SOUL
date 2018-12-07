#ifndef BUZZBMSTIG_H
#define BUZZBMSTIG_H

#include <buzz/buzztype.h>
#include <buzz/buzzdict.h>

# define BLOB_CHUNK_SIZE 100
# define MAX_BLOB_CHUNKS 2 // Max number of chunk storage 
# define RELOCATION_OF_CHUNKS_AT 90 // in percent
# define STOP_RELOCATION_AT 80      // in percent
/* Message constant for bstig chunk broadcast */
# define BROADCAST_MESSAGE_CONSTANT 8000 
/* TIME to die for chunk query messages */
# define BSTIG_CHUNK_TIME_TO_DIE 1 
/* Max chunk query messages */
# define MAX_CHUNK_RELOCATION_MEESAGE 5 
/* Time to wait for a neighbour to respond */
# define MAX_TIME_TO_WAIT_FOR_NEIGHBOUR 10
/* Time to wait before declaring a blob is lost */
# define MAX_TIME_TO_DECLARE_LOST_CHUNK 100
/* time for the bidder */
# define MAX_TIME_FOR_THE_BIDDER 20 
/* Size of queue clogging protector */
# define QUEUE_CLOGGING_PROTECTOR_QUEUE_SIZE 200 

# define MAX_TIME_TO_HOLD_SPACE_FOR_A_BID 200 
/* Uint16 max value */
# define MAX_UINT16 65535
/* Time to refresh getters list */
# define TIME_TO_REFRESH_GETTER_STATE 10

#ifdef __cplusplus
extern "C" {
#endif

    /*
    * Buzz chunk status type.
    * 
    */
   typedef enum {
      BUZZCHUNK_READY = 0,          // chunk is free for manipulation
      BUZZCHUNK_RESERVED,           // Chunk is reserved by a robot
      BUZZCHUNK_MARKED,             // Marked as checked and can't be relocated
      BUZZCHUNK_RELOCATED,          // has been relocated
      BUZZCHUNK_LOST,               // has been lost.
      BUZZCHUNK_STATUS_COUNT        // How many Buzz chunk status have been defined
   } buzzblob_chunk_status_e;

   /*
    * Buzz blob relocation status type.
    * 
    */
   typedef enum {
      BUZZBLOB_OPEN = 0,            // blob is open
      BUZZBLOB_SINK,                // blob receiver
      BUZZBLOB_SOURCE,              // blob creater
      BUZZBLOB_HOST,                // blob is hosted from bidding
      BUZZBLOB_HOST_FORWARDING,     // blob is a host and got a status message    
      BUZZBLOB_FORWARDING,          // blob is being forwarded
      BUZZBLOB_SETGETTER            // This robot is a getter 
   } buzzblob_relocstate_status_e;

   /*
    * Buzz blob status type.
    * 
    */
   typedef enum {
      BUZZBLOB_BUFFERING = 0,     // blob is buffering
      BUZZBLOB_BUFFERED,          // blob transmission complete
      BUZZBLOB_READY,             // blob is ready for a get
      BUZZBLOB_OBSOLETE,          // blob can be removed
   } buzzblob_status_e;

    /*
    * Buzz blob getter type.
    * 
    */
   typedef enum {
      BUZZBLOB_GETTER_OPEN     = 0,         // State unassigned  
      BUZZBLOB_GETTER_NEIGH,          // neigbour to blob getter
      BUZZBLOB_GETTER               // blob getter
   } buzzblobstigmery_getter_status_e;

   /*
    * An entry in blob stigmergy.
    */
   struct buzzbstig_elem_s {
      /* The hash of data associated to the entry */
      buzzobj_t data;
      /* The blob data associated to the entry*/
      //buzzobj_t blob;
      /* The timestamp (Lamport clock) */
      uint16_t timestamp;
      /* The robot id */
      uint16_t robot;
   };
   typedef struct buzzbstig_elem_s* buzzbstig_elem_t;

   /*
    * The virtual stigmergy data.
    */
   struct buzzbstig_s {
      buzzdict_t data;
      uint32_t keys_hash;
      uint8_t getter;
      buzzobj_t onconflict;
      buzzobj_t onconflictlost;
   };
   typedef struct buzzbstig_s* buzzbstig_t;

   /*
    * An blob entry data
    */
   struct buzzblob_elem_s
   {
     uint32_t hash; // Hash of blob 
     uint32_t size;
     uint8_t priority;
     buzzdarray_t available_list;
     buzzdarray_t locations;
     buzzdict_t data;
     uint8_t relocstate;
     uint8_t status;
     uint16_t request_time;
   };
   typedef struct buzzblob_elem_s* buzzblob_elem_t;

   /*
    * An blob chunk
    */
   struct buzzblob_chunk_s
   {
     uint32_t hash; // Hash of chunk
     char* chunk;
     uint8_t status;
   };
   typedef struct buzzblob_chunk_s* buzzblob_chunk_t;

   /*
    * An blob bidder element
    */
   struct buzzblob_bidder_s
   {
     uint16_t rid;
     uint8_t role; 
     uint16_t availablespace;
   };
   typedef struct buzzblob_bidder_s* buzzblob_bidder_t;


   typedef buzzblob_bidder_t buzzblob_location_t;

   struct buzzbstig_blobpriority_list_param
  {
   uint16_t id;
   uint32_t key;      // Here it is assumed blobs are inserted in an assending order
   uint8_t priority;
   uint8_t stat;
  };

   /*
    * Forward declaration of the Buzz VM.
    */
   struct buzzvm_s;

   /*
    * Registers the virtual stigmergy methods in the vm.
    * @param vm The state of the 
    * @param vm The Buzz VM state.
    * @return The updated VM state.
    */
   extern int buzzbstig_register(struct buzzvm_s* vm);

   /*
    * TODO
    *
    */
   extern buzzblob_chunk_t buzzbstig_chunk_new(uint32_t hash,
                                                char* chunk);

   /*
      TODO
 
   */
   extern void buzz_blob_slot_holders_new(struct buzzvm_s* vm, 
                                          uint16_t id, 
                                          uint16_t k, 
                                          uint32_t blob_size,
                                          uint32_t hash);
   /*
    * Creates a new virtual stigmergy entry.
    * @param data The data associated to the entry.
    * @param timestamp The timestamp (Lamport clock).
    * @param robot The robot id.
    * @return The new virtual stigmergy entry.
    */
   extern buzzbstig_elem_t buzzbstig_elem_new(buzzobj_t data,
                                              uint16_t timestamp,
                                              uint16_t robot);


   /*
    * Creates a new blob stigmergy entry with a blob.
    * @param vm The Buzz VM state.
    * @param data The data associated to the entry.
    * @param timestamp The timestamp (Lamport clock).
    * @param robot The robot id.
    * @return The new virtual stigmergy entry.
    */
   extern buzzbstig_elem_t buzzbstig_blob_elem_new(struct buzzvm_s* vm,
                                                   uint16_t id, 
                                                   buzzobj_t k, 
                                                   buzzobj_t data,
                                                   uint16_t timestamp,
                                                   uint16_t robot);
   /*
    * Clones a virtual stigmergy entry.
    * @param vm The Buzz VM state.
    * @param e The entry to clone.
    * @return A new virtual stigmergy entry.
    */
   extern buzzbstig_elem_t buzzbstig_elem_clone(struct buzzvm_s* vm,
                                                const buzzbstig_elem_t e);

   /*
    *TODO
    */
   extern buzzblob_elem_t buzzblob_elem_new(uint32_t hash, uint32_t size); 

   /*
    * Creates a new virtual stigmergy structure.
    * @return The new virtual stigmergy structure.
    */
   extern buzzbstig_t buzzbstig_new();

   /*
    * Destroys a virtual stigmergy structure.
    * @param vs The virtual stigmergy structure.
    */
   extern void buzzbstig_destroy(buzzbstig_t* vs);


   /*
    * Serializes an element in the virtual stigmergy.
    * The data is appended to the given buffer. The buffer is treated as a
    * dynamic array of uint8_t.
    * @param buf The output buffer where the serialized data is appended.
    * @param key The key of the element to serialize.
    * @param data The data of the element to serialize.
    */
   extern void buzzbstig_elem_serialize(buzzmsg_payload_t buf,
                                        const buzzobj_t key,
                                        const buzzbstig_elem_t data);

   /*
    * Deserializes a virtual stigmergy element.
    * The data is read from the given buffer starting at the given position.
    * The buffer is treated as a dynamic array of uint8_t.
    * @param key The deserialized key of the element.
    * @param data The deserialized data of the element.
    * @param buf The input buffer where the serialized data is stored.
    * @param pos The position at which the data starts.
    * @param vm The Buzz VM data.
    * @return The new position in the buffer, of -1 in case of error.
    */
   extern int64_t buzzbstig_elem_deserialize(buzzobj_t* key,
                                             buzzbstig_elem_t* data,
                                             buzzmsg_payload_t buf,
                                             uint32_t pos,
                                             struct buzzvm_s* vm);

   extern void buzzbstig_chunk_serialize(buzzmsg_payload_t buf, buzzblob_chunk_t cdata);

   extern int64_t buzzbstig_chunk_deserialize(char** cdata,
                                              buzzmsg_payload_t buf,
                                              uint32_t pos);

   extern void buzzbstig_chunkstig_update(struct buzzvm_s* vm);

   extern void buzzbstig_create_generic(struct buzzvm_s* vm, uint16_t id);

   /*
    * Buzz C closure to create a new stigmergy object.
    * @param vm The Buzz VM state.
    * @return The updated VM state.
    */
   extern int buzzbstig_create(struct buzzvm_s* vm);

   /*
    * Buzz C closure to get the number of elements in a virtual stigmergy structure.
    * @param vm The Buzz VM state.
    * @return The updated VM state.
    */
   extern int buzzbstig_size(struct buzzvm_s* vm);

   /*
    * Buzz C closure to put a blbo in a stigmergy object.
    * @param vm The Buzz VM state.
    * @return The updated VM state.
    */
   extern int buzzbstig_putblob(struct buzzvm_s* vm);

    /*
    * Buzz C closure to put an element in a stigmergy object.
    * @param vm The Buzz VM state.
    * @return The updated VM state.
    */
   extern int buzzbstig_put(struct buzzvm_s* vm);

   extern int buzzbstig_getblobseqdone(struct buzzvm_s* vm);

   /*
    * TODO 
    */
   extern void buzzbstig_get_generic(struct buzzvm_s* vm, 
                                    uint16_t id, 
                                    buzzobj_t k);

   /*
    * Buzz C closure to get an element from a stigmergy object.
    * @param vm The Buzz VM state.
    * @return The updated VM state.
    */
   extern int buzzbstig_get(struct buzzvm_s* vm);

   extern void buzzbstig_neigh_loop(const void* key, void* data, void* params);

   extern int buzzbstig_blobstatus(struct buzzvm_s* vm);

   extern void buzzbstig_blobrequest_update(struct buzzvm_s* vm);

   extern void buzzbstig_blobgetters_update(struct buzzvm_s* vm);

   extern int buzzbstig_getblob(struct buzzvm_s* vm);

   extern int buzzbstig_blob_bidderpriority_key_cmp(const void* a, const void* b);
   /*
    * Buzz C closure to loop through the elements of a stigmergy object.
    * @param vm The Buzz VM state.
    * @return The updated VM state.
    */
   extern int buzzbstig_foreach(struct buzzvm_s* vm);

   /*
    * Buzz C closure to set the function to call on write conflict.
    * @param vm The Buzz VM state.
    * @return The updated VM state.
    */
   extern int buzzbstig_onconflict(struct buzzvm_s* vm);

   /*
    * Buzz C closure to set the function to call on loss of conflict.
    * @param vm The Buzz VM state.
    * @return The updated VM state.
    */
   extern int buzzbstig_onconflictlost(struct buzzvm_s* vm);


   extern int buzzbstig_setgetter(struct buzzvm_s* vm);
   /*
    * Calls the write conflict manager.
    * @param vm The Buzz VM state.
    * @return The updated VM state.
    */
   extern buzzbstig_elem_t buzzbstig_onconflict_call(struct buzzvm_s* vm,
                                                     buzzbstig_t vs,
                                                     buzzobj_t k,
                                                     buzzbstig_elem_t lv,
                                                     buzzbstig_elem_t rv);


   extern void buzzbstig_allocate_chunks(struct buzzvm_s* vm,
                                          uint16_t id,
                                          uint16_t key,
                                          uint16_t bidderid,
                                          uint16_t bidsize);
   /*
    * Calls the lost conflict manager.
    * @param vm The Buzz VM state.
    */
   extern void buzzbstig_onconflictlost_call(struct buzzvm_s* vm,
                                             buzzbstig_t vs,
                                             buzzobj_t k,
                                             buzzbstig_elem_t lv);

   extern void buzzbstig_blob_bidderelem_destroy(uint32_t pos, void* data, void* param); 

   extern int buzzbstig_blob_bidderelem_key_cmp(const void* a, const void* b);

   extern void buzzbstig_relocation_bider_add(struct buzzvm_s* vm,
                                    uint16_t id,
                                    uint16_t key,
                                    uint16_t availablespace,
                                    uint32_t blob_size,
                                    uint32_t hash,
                                    uint8_t subtype,
                                    uint16_t receiver);
   extern int buzzbstig_priority_cmp(const void* a, const void* b);

   extern buzzdarray_t buzzbstig_get_prioritylist(struct buzzvm_s* vm);
   /*
    * TODO
    *
    */  
    extern void buzzblob_split_put_bstig(struct buzzvm_s* vm,
                                        uint16_t id,
                                        buzzobj_t blob,
                                        buzzblob_elem_t blb_struct,
                                        buzzobj_t key,
                                        buzzbstig_elem_t e);

    /*
    * TODO
    *
    */
    extern buzzobj_t buzzbstig_construct_blob(struct buzzvm_s* vm, buzzblob_elem_t v_blob);

   /*
    * TODO
    *
    */  
    extern int buzzbstig_store_chunk(struct buzzvm_s* vm,
                                      uint16_t id,
                                      buzzobj_t k,
                                      buzzbstig_elem_t v,
                                      uint32_t blob_size,
                                      uint16_t chunk_index,
                                      buzzblob_chunk_t cdata);

    extern buzzdarray_t buzzbstig_getlocations(struct buzzvm_s* vm, uint16_t id, uint16_t key);

    extern void buzzbstig_blobstatus_update(struct buzzvm_s* vm);

   /*
    * Generates a md5 hash.
    * @param blob The blob to gererate md5 hash
    * @param size The size of blob
    * @return pointer to the hash and the user is incharge of freeing the pointer. 
    */
   extern uint32_t* buzzbstig_md5(const char* blob, uint32_t size);

   extern void buzzbstig_seralize_blb_stigs(struct buzzvm_s* vm, buzzdarray_t bm);

   extern int64_t buzzbstig_deseralize_blb_stigs_set(struct buzzvm_s* vm,buzzdarray_t a,int64_t pos);

#ifdef __cplusplus
}
#endif

/*
 * Looks for an element in a virtual stigmergy structure.
 * @param vs The virtual stigmergy structure.
 * @param key The key to look for.
 * @return The data associated to the key, or NULL if not found.
 */
#define buzzbstig_fetch(vs, key) buzzdict_get((vs)->data, (key), buzzbstig_elem_t)

/*
 * Puts data into a virtual stigmergy structure.
 * @param vs The virtual stigmergy structure.
 * @param key The key.
 * @param el The element.
 */
#define buzzbstig_store(vs, key, el) buzzdict_set((vs)->data, (key), (el));

/*
 * Deletes data from a virtual stigmergy structure.
 * @param vs The virtual stigmergy structure.
 * @param key The key.
 */
#define buzzbstig_remove(vs, key) buzzdict_remove((vs)->data, (key)); 

/*
 * Applies the given function to each element in the virtual stigmergy structure.
 * @param vs The virtual stigmergy structure.
 * @param fun The function.
 * @param params A buffer to pass along.
 * @see buzzdict_foreach()
 */
#define buzzbstig_foreach_elem(vs, fun, params) buzzdict_foreach((vs)->data, fun, params);

/* Function to left rotate used during md5 hash computation */
#define LEFTROTATE(x, c) (((x) << (c)) | ((x) >> (32 - (c))))

#endif
