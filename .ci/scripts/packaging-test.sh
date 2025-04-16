#!/bin/bash

# opensuse 15 has a missing dep for systemd

if which zypper > /dev/null ; then
    sudo zypper install -y insserv-compat docker-buildx
fi

if [ -e /etc/sysctl.d/99-gce.conf ]; then
  # The GCE defaults disable IPv4 forwarding, which breaks the Docker
  # build. Workaround this by renaming the file so that it is executed
  # earlier than our own overrides.
  #
  # This ultimately needs to be fixed at the image level - see infra
  # issue 15654.
  sudo mv /etc/sysctl.d/99-gce.conf /etc/sysctl.d/98-gce.conf
fi

# Required by bats
sudo touch /etc/is_vagrant_vm
sudo useradd vagrant

set -e

. .ci/java-versions.properties
BUILD_JAVA_HOME=$HOME/.java/$ES_BUILD_JAVA

rm -Rfv $HOME/.gradle/init.d/ && mkdir -p $HOME/.gradle/init.d
cp -v .ci/init.gradle $HOME/.gradle/init.d

unset JAVA_HOME

if [ -f "/etc/os-release" ] ; then
    cat /etc/os-release
    . /etc/os-release
    if [[ "$ID" == "debian" || "$ID_LIKE" == "debian" ]] ; then
        # FIXME: The base image should not have rpm installed
        sudo rm -Rf /usr/bin/rpm
        # Work around incorrect lintian version
        #  https://github.com/elastic/elasticsearch/issues/48573
        if [ $VERSION_ID == 10 ] ; then
            sudo apt-get update -y || true
            sudo apt-get install -y --allow-downgrades lintian=2.15.0
        fi
    fi
    if [[ "$ID" == "rhel" ]] ; then
      # Downgrade containerd if necessary to work around runc bug
      # See: https://github.com/opencontainers/runc/issues/3551
      if containerd -version | grep -sF 1.6.7; then
        sudo yum downgrade -y containerd.io
      fi
    fi
else
    cat /etc/issue || true
fi

sudo bash -c 'cat > /etc/sudoers.d/elasticsearch_vars'  << SUDOERS_VARS
    Defaults   env_keep += "ES_JAVA_HOME"
    Defaults   env_keep += "JAVA_HOME"
    Defaults   env_keep += "SYSTEM_JAVA_HOME"
SUDOERS_VARS
sudo chmod 0440 /etc/sudoers.d/elasticsearch_vars

# Bats tests still use this locationa
sudo rm -Rf /elasticsearch
sudo mkdir -p /elasticsearch/qa/ && sudo chown jenkins /elasticsearch/qa/ && ln -s $PWD/qa/vagrant /elasticsearch/qa/

# Ensure since we're running as root that we can do git operations in this directory
# See: https://git-scm.com/docs/git-config/2.35.2#Documentation/git-config.txt-safedirectory
git config --global --add safe.directory $WORKSPACE

# sudo sets it's own PATH thus we use env to override that and call sudo annother time so we keep the secure root PATH
# run with --continue to run both bats and java tests even if one fails
# be explicit about Gradle home dir so we use the same even with sudo
sudo -E env \
  PATH=$BUILD_JAVA_HOME/bin:`sudo bash -c 'echo -n $PATH'` \
  --unset=ES_JAVA_HOME \
  --unset=JAVA_HOME \
  SYSTEM_JAVA_HOME=`readlink -f -n $BUILD_JAVA_HOME` \
  DOCKER_CONFIG="${HOME}/.docker" \
  ./gradlew -g $HOME/.gradle --console=plain --scan --parallel --build-cache -Dorg.elasticsearch.build.cache.url=https://gradle-enterprise.elastic.co/cache/ --continue $@

