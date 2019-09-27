!#/bin/sh

# opensuse 15 has a missing dep for systemd 
sudo zypper install -y insserv-compat

set -e
sudo rm -Rfv /root/.gradle/init.d
sudo mkdir -p /root/.gradle/init.d
sudo cp -v $WORKSPACE/.ci/init.gradle /root/.gradle/init.d

unset JAVA_HOME

git clone https://github.com/sstephenson/bats /tmp/bats
sudo /tmp/bats/install.sh /usr

sudo bash -c 'cat > /etc/sudoers.d/elasticsearch_vars'  << SUDOERS_VARS
    Defaults   env_keep += "ZIP"
    Defaults   env_keep += "TAR"
    Defaults   env_keep += "RPM"
    Defaults   env_keep += "DEB"
    Defaults   env_keep += "PACKAGING_ARCHIVES"
    Defaults   env_keep += "PACKAGING_TESTS"
    Defaults   env_keep += "BATS_UTILS"
    Defaults   env_keep += "BATS_TESTS"
    Defaults   env_keep += "SYSTEM_JAVA_HOME"
    Defaults   env_keep += "JAVA_HOME"
SUDOERS_VARS
sudo chmod 0440 /etc/sudoers.d/elasticsearch_vars


# Required by bats
sudo touch /etc/is_vagrant_vm
sudo useradd vagrant

# Bats tests still use this location
sudo mkdir -p /elasticsearch/qa/ && sudo chown jenkins /elasticsearch/qa/ && ln -s $PWD/qa/vagrant /elasticsearch/qa/

# sudo sets it's own PATH thus we use env to override that and call sudo annother time so we keep the secure root PATH 
# run with --continue to run both bats and java tests even if one fails
sudo -E env \
  PATH=$HOME/.java/$ES_BUILD_JAVA/bin:`sudo bash -c 'echo -n $PATH'` \
  RUNTIME_JAVA_HOME=`readlink -f -n $RUNTIME_JAVA_HOME` \
  --unset=JAVA_HOME \
  SYSTEM_JAVA_HOME=`readlink -f -n $RUNTIME_JAVA_HOME` \
  ./gradlew $@ --continue destructivePackagingTest

