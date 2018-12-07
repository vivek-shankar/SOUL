#include "base64.h"

static const char base64_chars[] ="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static const std::string strbase64_chars = 
             "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
             "abcdefghijklmnopqrstuvwxyz"
             "0123456789+/";

unsigned int b64d_size(unsigned int in_size) {

  return ((3*in_size)/4);
}


static inline bool is_base64(unsigned char c) {
  return (isalnum(c) || (c == '+') || (c == '/'));
}

std::string base64_encode(unsigned char const* bytes_to_encode, unsigned int in_len, char* out_endoded) {
  std::string ret;
  int i = 0;
  int j = 0;
  int out_cntr = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];

  while (in_len--) {
    char_array_3[i++] = *(bytes_to_encode++);
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for(i = 0; (i <4) ; i++){
        ret += base64_chars[char_array_4[i]];
        out_endoded[out_cntr]= base64_chars[char_array_4[i]];
        out_cntr++;
      }
      i = 0;
    }
  }

  if (i)
  {
    for(j = i; j < 3; j++)
      char_array_3[j] = '\0';

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
    char_array_4[3] = char_array_3[2] & 0x3f;

    for (j = 0; (j < i + 1); j++){
      ret += base64_chars[char_array_4[j]];
      out_endoded[out_cntr]= base64_chars[char_array_4[j]];
      out_cntr++;
    }

    while((i++ < 3)){
      ret += '=';
      out_endoded[out_cntr]= '=';
      out_cntr++;
    }

  }
  out_endoded[out_cntr]= '\0';
  return ret;

}

int base64_decode(const char* s, char* out) {
  size_t in_len = strlen(s);
  size_t i = 0;
  size_t j = 0;
  int in_ = 0;
  int out_cntr = 0;
  unsigned char char_array_4[4], char_array_3[3];
  std::string ret;

  while (in_len-- && ( s[in_] != '=') && is_base64(s[in_])) {
    char_array_4[i++] = s[in_]; in_++;
    if (i ==4) {
      for (i = 0; i <4; i++){
        char* findchar = strchr((char*)base64_chars,char_array_4[i]);
        int position = findchar - base64_chars;
        char_array_4[i] = (unsigned char)(position);
      }

      char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
      char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
      char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

      for (i = 0; (i < 3); i++){
        ret += char_array_3[i];
        out[out_cntr]= char_array_3[i];
        out_cntr++;
      }
      i = 0;
    }
  }

  if (i) {
    for (j = i; j <4; j++)
      char_array_4[j] = 0;

    for (j = 0; j <4; j++){
      char* findchar = strchr((char*)base64_chars,char_array_4[j]);
      int position = findchar - base64_chars;
      char_array_4[j] = (unsigned char)(position);
    }

    char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
    char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
    char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

    for (j = 0; (j < i - 1); j++){
      ret += char_array_3[j];
      out[out_cntr]= char_array_3[j];
      out_cntr++;
    }
  }
  out[out_cntr]= '\0';
  return out_cntr;
  //return ret;
}
