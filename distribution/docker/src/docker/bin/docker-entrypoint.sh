#!/bin/bash
set -e

# Files created by Elasticsearch should always be group writable too
umask 0002

run_as_other_user_if_needed() {
  if [[ "$(id -u)" == "0" ]]; then
    # If running as root, drop to specified UID and run command
    exec chroot --userspec=1000 / "${@}"
  else
    # Either we are running in Openshift with random uid and are a member of the root group
    # or with a custom --user
    exec "${@}"
  fi
}

# Allow user specify custom CMD, maybe bin/elasticsearch itself
# for example to directly specify `-E` style parameters for elasticsearch on k8s
# or simply to run /bin/bash to check the image
if [[ "$1" != "eswrapper" ]]; then
  if [[ "$(id -u)" == "0" && $(basename "$1") == "elasticsearch" ]]; then
    # centos:7 chroot doesn't have the `--skip-chdir` option and
    # changes our CWD.
    # Rewrite CMD args to replace $1 with `elasticsearch` explicitly,
    # so that we are backwards compatible with the docs
    # from the previous Elasticsearch versions<6
    # and configuration option D:
    # https://www.elastic.co/guide/en/elasticsearch/reference/5.6/docker.html#_d_override_the_image_8217_s_default_ulink_url_https_docs_docker_com_engine_reference_run_cmd_default_command_or_options_cmd_ulink
    # Without this, user could specify `elasticsearch -E x.y=z` but
    # `bin/elasticsearch -E x.y=z` would not work.
    set -- "elasticsearch" "${@:2}"
    # Use chroot to switch to UID 1000
    exec chroot --userspec=1000 / "$@"
  else
    # User probably wants to run something else, like /bin/bash, with another uid forced (Openshift?)
    exec "$@"
  fi
fi

# Allow environment variables to be set by creating a file with the
# contents, and setting an environment variable with the suffix _FILE to
# point to it. This can be used to provide secrets to a container, without
# the values being specified explicitly when running the container.
#
# This is also sourced in elasticsearch-env, and is only needed here
# as well because we use ELASTIC_PASSWORD below. Sourcing this script
# is idempotent.
source /usr/share/elasticsearch/bin/elasticsearch-env-from-file

if [[ -f bin/elasticsearch-users ]]; then
  # Check for the ELASTIC_PASSWORD environment variable to set the
  # bootstrap password for Security.
  #
  # This is only required for the first node in a cluster with Security
  # enabled, but we have no way of knowing which node we are yet. We'll just
  # honor the variable if it's present.
  if [[ -n "$ELASTIC_PASSWORD" ]]; then
    [[ -f /usr/share/elasticsearch/config/elasticsearch.keystore ]] || (run_as_other_user_if_needed elasticsearch-keystore create)
    if ! (run_as_other_user_if_needed elasticsearch-keystore has-passwd --silent) ; then
      # keystore is unencrypted
      if ! (run_as_other_user_if_needed elasticsearch-keystore list | grep -q '^bootstrap.password$'); then
        (run_as_other_user_if_needed echo "$ELASTIC_PASSWORD" | elasticsearch-keystore add -x 'bootstrap.password')
      fi
    else
      # keystore requires password
      if ! (run_as_other_user_if_needed echo "$KEYSTORE_PASSWORD" \
          | elasticsearch-keystore list | grep -q '^bootstrap.password$') ; then
        COMMANDS="$(printf "%s\n%s" "$KEYSTORE_PASSWORD" "$ELASTIC_PASSWORD")"
        (run_as_other_user_if_needed echo "$COMMANDS" | elasticsearch-keystore add -x 'bootstrap.password')
      fi
    fi
  fi
fi

if [[ "$(id -u)" == "0" ]]; then
  # If requested and running as root, mutate the ownership of bind-mounts
  if [[ -n "$TAKE_FILE_OWNERSHIP" ]]; then
    chown -R 1000:0 /usr/share/elasticsearch/{data,logs}
  fi
fi

run_as_other_user_if_needed /usr/share/elasticsearch/bin/elasticsearch <<<"$KEYSTORE_PASSWORD"
