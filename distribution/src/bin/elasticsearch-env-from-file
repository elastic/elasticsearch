#!/bin/bash

set -e -o pipefail

# Allow environment variables to be set by creating a file with the
# contents, and setting an environment variable with the suffix _FILE to
# point to it. This can be used to provide secrets to a container, without
# the values being specified explicitly when running the container.
#
# Note that only supported environment variables are processed, in order
# to avoid unexpected failures when an environment sets a "*_FILE" variable
# that doesn't contain a filename.
#
# This script is intended to be sourced, not executed, and modifies the
# environment.

for VAR_NAME_FILE in ELASTIC_PASSWORD_FILE KEYSTORE_PASSWORD_FILE ; do
  if [[ -n "${!VAR_NAME_FILE}" ]]; then
    VAR_NAME="${VAR_NAME_FILE%_FILE}"

    if env | grep "^${VAR_NAME}="; then
      echo "ERROR: Both $VAR_NAME_FILE and $VAR_NAME are set. These are mutually exclusive." >&2
      exit 1
    fi

    if [[ ! -e "${!VAR_NAME_FILE}" ]]; then
      # Maybe the file doesn't exist, maybe we just can't read it due to file permissions.
      # Check permissions on each part of the path
      path=''
      if ! echo "${!VAR_NAME_FILE}" | grep -q '^/'; then
        path='.'
      fi

      dirname "${!VAR_NAME_FILE}" | tr '/' '\n' | while read part; do
        if [[ "$path" == "/" ]]; then
          path="${path}${part}"
        else
          path="$path/$part"
        fi

        if ! [[ -x "$path" ]]; then
          echo "ERROR: Cannot read ${!VAR_NAME_FILE} from $VAR_NAME_FILE, due to lack of permissions on '$path'" 2>&1
          exit 1
        fi
      done

      if ! [[ -r "${!VAR_NAME_FILE}" ]]; then
        echo "ERROR: File ${!VAR_NAME_FILE} from $VAR_NAME_FILE is not readable." 2>&1
      else
        echo "ERROR: File ${!VAR_NAME_FILE} from $VAR_NAME_FILE does not exist" >&2
      fi

      exit 1
    fi

    FILE_PERMS="$(stat -L -c '%a' ${!VAR_NAME_FILE})"

    if [[ "$FILE_PERMS" != "400" && "$FILE_PERMS" != "600" ]]; then
      if [[ -L "${!VAR_NAME_FILE}" ]]; then
        echo "ERROR: File $(readlink "${!VAR_NAME_FILE}") (target of symlink ${!VAR_NAME_FILE} from $VAR_NAME_FILE) must have file permissions 400 or 600, but actually has: $FILE_PERMS" >&2
      else
        echo "ERROR: File ${!VAR_NAME_FILE} from $VAR_NAME_FILE must have file permissions 400 or 600, but actually has: $FILE_PERMS" >&2
      fi
      exit 1
    fi

    echo "Setting $VAR_NAME from $VAR_NAME_FILE at ${!VAR_NAME_FILE}" >&2
    export "$VAR_NAME"="$(cat ${!VAR_NAME_FILE})"

    unset VAR_NAME
    # Unset the suffixed environment variable
    unset "$VAR_NAME_FILE"
  fi
done
