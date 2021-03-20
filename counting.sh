#!/bin/bash

for file in `find . -type f | egrep '.*.java*' `
do
        sed -i 's/getMemoryStats_01/getMemoryStats01/g' $file
        sed -i 's/getMemoryStats_02/getMemoryStats02/g' $file
        sed -i 's/getMemoryStats_04/getMemoryStats04/g' $file
        sed -i 's/getMemoryStats_08/getMemoryStats08/g' $file
        sed -i 's/getMemoryStats_16/getMemoryStats16/g' $file
        sed -i 's/getMemoryStats_32/getMemoryStats32/g' $file
        sed -i 's/getMemoryStats_64/getMemoryStats64/g' $file
done
