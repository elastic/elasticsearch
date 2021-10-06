#!/bin/bash

if [ -z "$BRANCH" ] ; then
    echo "BRANCH is unset"
    exit 1
fi

folders=("jobs" "templates" "views")
for folder in "${folders[@]}"
do
  rm -Rf .ci/$folder;
  mkdir -p .ci/$folder
  cp -r .ci/${folder}.t/* .ci/$folder/
  sed -i "s/%BRANCH%/${BRANCH}/g" .ci/$folder/*.yml
done
