#!/bin/bash

date

time shuf      -n20 -r -e INSERT,a,1 UPDATE,a,2 DELETE,a GET,a FOO,a,5 | ./$1/kv

date

time shuf -n1000000 -r -e INSERT,a,1 UPDATE,a,2 DELETE,a GET,a FOO,a,5 | ./$1/kv 2>/dev/null

date
s
time shuf -n1000000 -r -e INSERT,a,1 UPDATE,a,2 DELETE,a GET,a \
                          INSERT,b,2 UPDATE,b,3 DELETE,b GET,b \
                          INSERT,c,3 UPDATE,c,4 DELETE,c GET,c \
                          INSERT,d,4 UPDATE,d,5 DELETE,d GET,d \
                          INSERT,e,5 UPDATE,e,6 DELETE,e GET,e \
                          INSERT,f,6 UPDATE,f,7 DELETE,f GET,f \
                          INSERT,g,7 UPDATE,g,8 DELETE,g GET,g \
                          INSERT,h,8 UPDATE,h,9 DELETE,h GET,h FOO,a,5 | ./$1/kv 2>/dev/null
