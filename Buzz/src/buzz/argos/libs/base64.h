#ifndef _BASE64_H_
#define _BASE64_H_
#include <string>
#include <string.h>
std::string base64_encode(unsigned char const* bytes_to_encode , unsigned int in_len, char* out_endoded);
int base64_decode(const char* s, char* out);
unsigned int b64d_size(unsigned int in_size);
#endif 
