#! /bin/bash
for i in {1..6}
do
   cat test_stdin_inputs/$i.txt | python3 ../main.py
   echo
done