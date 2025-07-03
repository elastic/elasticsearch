#!/bin/sh
# Start script for start-local
# More information: https://github.com/elastic/start-local
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${SCRIPT_DIR}"
today=$(date +%s)
. ./.env
# Check disk space
available_gb=$(($(df -k / | awk 'NR==2 {print $4}') / 1024 / 1024))
required=$(echo "${ES_LOCAL_DISK_SPACE_REQUIRED}" | grep -Eo '[0-9]+')
if [ "$available_gb" -lt "$required" ]; then
  echo "----------------------------------------------------------------------------"
  echo "WARNING: Disk space is below the ${required} GB limit. Elasticsearch will be"
  echo "executed in read-only mode. Please free up disk space to resolve this issue."
  echo "----------------------------------------------------------------------------"
  echo "Press ENTER to confirm."
  read -r
fi
if [ -z "${ES_LOCAL_LICENSE:-}" ] && [ "$today" -gt 1752840833 ]; then
  echo "---------------------------------------------------------------------"
  echo "The one-month trial period has expired. You can continue using the"
  echo "Free and open Basic license or request to extend the trial for"
  echo "another 30 days using this form:"
  echo "https://www.elastic.co/trialextension"
  echo "---------------------------------------------------------------------"
  echo "For more info about the license: https://www.elastic.co/subscriptions"
  echo
  echo "Updating the license..."
  docker compose up --wait elasticsearch >/dev/null 2>&1
  result=$(curl -s -X POST "${ES_LOCAL_URL}/_license/start_basic?acknowledge=true" -H "Authorization: ApiKey ${ES_LOCAL_API_KEY}" -o /dev/null -w '%{http_code}\n')
  if [ "$result" = "200" ]; then
    echo "✅ Basic license successfully installed"
    echo "ES_LOCAL_LICENSE=basic" >> .env
  else 
    echo "Error: I cannot update the license"
    result=$(curl -s -X GET "${ES_LOCAL_URL}" -H "Authorization: ApiKey ${ES_LOCAL_API_KEY}" -o /dev/null -w '%{http_code}\n')
    if [ "$result" != "200" ]; then
      echo "Elasticsearch is not running."
    fi
    exit 1
  fi
  echo
fi
docker compose up --wait
