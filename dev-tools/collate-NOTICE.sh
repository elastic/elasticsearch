#!/bin/bash
cd `dirname $0`/..

exec > NOTICE.txt

year=$(date +"%Y")
echo "Elasticsearch"
echo "Copyright 2009-$year Elasticsearch"
echo
echo "This product includes software developed by The Apache Software Foundation"
echo "(http://www.apache.org/)."
echo
echo

for file in $(find core/licenses/ -name '*LICENSE*' -or -name '*NOTICE*' | sort) ; do
  if [ -s $file ]
    then
      filename=`basename $file`
      filename=${filename//.txt/}
      echo "================================================================================"
      echo $filename;
      echo "================================================================================"
      cat $file
      echo;
      echo;
  fi;
done

