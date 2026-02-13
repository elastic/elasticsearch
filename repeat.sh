#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <command>"
  exit 1
fi

COMMAND_TO_RUN="$@"

for i in {1..100}; do
   echo "Running iteration: $i"
   # Execute the command
   eval $COMMAND_TO_RUN
   # Check the exit code
   if [ $? -ne 0 ]; then
       echo "Command failed on iteration $i with exit code $?"
       break
   fi
done

echo "Loop finished."
