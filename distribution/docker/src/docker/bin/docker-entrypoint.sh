#!/bin/bash
set -e

# Files created by Elasticsearch should always be group writable too
umask 0002

# Allow user specify custom CMD, maybe bin/elasticsearch itself
# for example to directly specify `-E` style parameters for elasticsearch on k8s
# or simply to run /bin/bash to check the image
if [[ "$1" == "eswrapper" || $(basename "$1") == "elasticsearch" ]]; then
  # Rewrite CMD args to remove the explicit command,
  # so that we are backwards compatible with the docs
  # from the previous Elasticsearch versions < 6
  # and configuration option:
  # https://www.elastic.co/guide/en/elasticsearch/reference/5.6/docker.html#_d_override_the_image_8217_s_default_ulink_url_https_docs_docker_com_engine_reference_run_cmd_default_command_or_options_cmd_ulink
  # Without this, user could specify `elasticsearch -E x.y=z` but
  # `bin/elasticsearch -E x.y=z` would not work. In any case,
  # we want to continue through this script, and not exec early.
  set -- "${@:2}"
else
  # Run whatever command the user wanted
  exec "$@"
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
    if ! (elasticsearch-keystore list | grep -q '^bootstrap.password$'); then
      (echo "$ELASTIC_PASSWORD" | elasticsearch-keystore add -x 'bootstrap.password')
    fi
  fi
fi

# Do not abort script if Elasticsearch returns error code
set +e

PID_FILE=/tmp/es.pid

term_handler() {
   echo "Caught SIGTERM"
   kill -TERM $(cat $PID_FILE)
}

# We need to ensure that TERM is sent only to Elasticsearch, not
# to the whole process group, as it can cause issues with forked
# processes appearing to have been exited abnormally.
trap term_handler SIGTERM

# Use a mini-init process to ensure any children are cleaned up and we
# aren't left with any zombies.
/sbin/my_init --skip-runit -- /usr/share/elasticsearch/bin/elasticsearch "$@" -p $PID_FILE &

# Wait for my_init to exit, which will happen when Elasticsearch exits.
INIT_PID=$!
wait "${INIT_PID}"

# my_init propagates the ES exit code.
ES_EXIT_CODE=$?
exit $ES_EXIT_CODE
