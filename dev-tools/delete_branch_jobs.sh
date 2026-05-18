#!/usr/bin/env bash
set -e

if [ "$#" -ne 1 ]; then
    printf 'Usage: %s <branch> \n' "$(basename "$0")"
    exit 0;
fi

BRANCH="$1"

if [ -z "${JENKINS_USERNAME}" ] || [ -z "${JENKINS_TOKEN}" ]; then
    echo "You must set JENKINS_USERNAME and JENKINS_TOKEN environment variables."
    exit 1;
fi

echo "Deleting Jenkins jobs..."
curl -s https://elasticsearch-ci.elastic.co/api/json \
  | jq -r ".jobs | .[] | select(.name | startswith(\"elastic+elasticsearch+${BRANCH}\")) | .url" \
  | xargs -L 1 curl -u $JENKINS_USERNAME:$JENKINS_TOKEN -X DELETE

echo "Deleting views..."
curl -s https://elasticsearch-ci.elastic.co/api/json \
  | jq -r ".views | .[] | select(.name == \"Elasticsearch ${BRANCH}\") | .url" \
  | xargs -L 1 -I '{}' curl -u $JENKINS_USERNAME:$JENKINS_TOKEN -X POST "{}/doDelete"

echo "Done."
