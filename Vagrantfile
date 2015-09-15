# -*- mode: ruby -*-
# vi: set ft=ruby :

# This Vagrantfile exists to test packaging. Read more about its use in the
# vagrant section in TESTING.asciidoc.

# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

Vagrant.configure(2) do |config|
  config.vm.define "precise" do |config|
    config.vm.box = "ubuntu/precise64"
    ubuntu_common config
  end
  config.vm.define "trusty" do |config|
    config.vm.box = "ubuntu/trusty64"
    ubuntu_common config
  end
  config.vm.define "vivid" do |config|
    config.vm.box = "ubuntu/vivid64"
    ubuntu_common config
  end
  # Wheezy's backports don't contain Openjdk 8 and the backflips required to
  # get the sun jdk on there just aren't worth it. We have jessie for testing
  # debian and it works fine.
  config.vm.define "jessie" do |config|
    config.vm.box = "debian/jessie64"
    deb_common config,
      'echo deb http://http.debian.net/debian jessie-backports main > /etc/apt/sources.list.d/backports.list', 'backports'
  end
  config.vm.define "centos-6" do |config|
    config.vm.box = "boxcutter/centos67"
    rpm_common config
  end
  config.vm.define "centos-7" do |config|
    # There is a centos/7 box but it doesn't have rsync or virtualbox guest
    # stuff on there so its slow to use. So chef it is....
    config.vm.box = "boxcutter/centos71"
    rpm_common config
  end
  # This box hangs _forever_ on ```yum check-update```. I have no idea why.
  # config.vm.define "oel-6", autostart: false do |config|
  #   config.vm.box = "boxcutter/oel66"
  #   rpm_common(config)
  # end
  config.vm.define "oel-7" do |config|
    config.vm.box = "boxcutter/oel70"
    rpm_common config
  end
  config.vm.define "fedora-22" do |config|
    # Fedora hosts their own 'cloud' images that aren't in Vagrant's Atlas but
    # and are missing required stuff like rsync. It'd be nice if we could use
    # them but they much slower to get up and running then the boxcutter image.
    config.vm.box = "boxcutter/fedora22"
    dnf_common config
  end
  # Switch the default share for the project root from /vagrant to
  # /elasticsearch because /vagrant is confusing when there is a project inside
  # the elasticsearch project called vagrant....
  config.vm.synced_folder ".", "/vagrant", disabled: true
  config.vm.synced_folder "", "/elasticsearch"
  if Vagrant.has_plugin?("vagrant-cachier")
    config.cache.scope = :box
  end
  config.vm.defined_vms.each do |name, config|
    config.options[:autostart] = false
    set_prompt = lambda do |config|
      # Sets up a consistent prompt for all users. Or tries to. The VM might
      # contain overrides for root and vagrant but this attempts to work around
      # them by re-source-ing the standard prompt file.
      config.vm.provision "prompt", type: "shell", inline: <<-SHELL
        cat \<\<PROMPT > /etc/profile.d/elasticsearch_prompt.sh
export PS1='#{name}:\\w$ '
PROMPT
        grep 'source /etc/profile.d/elasticsearch_prompt.sh' ~/.bashrc |
          cat \<\<SOURCE_PROMPT >> ~/.bashrc
# Replace the standard prompt with a consistent one
source /etc/profile.d/elasticsearch_prompt.sh
SOURCE_PROMPT
        grep 'source /etc/profile.d/elasticsearch_prompt.sh' ~vagrant/.bashrc |
          cat \<\<SOURCE_PROMPT >> ~vagrant/.bashrc
# Replace the standard prompt with a consistent one
source /etc/profile.d/elasticsearch_prompt.sh
SOURCE_PROMPT
      SHELL
    end
    config.config_procs.push ['2', set_prompt]
  end
end

def ubuntu_common(config)
  deb_common config, 'apt-add-repository -y ppa:openjdk-r/ppa > /dev/null 2>&1', 'openjdk-r-*'
end

def deb_common(config, add_openjdk_repository_command, openjdk_list)
  # http://foo-o-rama.com/vagrant--stdin-is-not-a-tty--fix.html
  config.vm.provision "fix-no-tty", type: "shell" do |s|
      s.privileged = false
      s.inline = "sudo sed -i '/tty/!s/mesg n/tty -s \\&\\& mesg n/' /root/.profile"
  end
  provision(config,
    update_command: "apt-get update",
    update_tracking_file: "/var/cache/apt/archives/last_update",
    install_command: "apt-get install -y",
    java_package: "openjdk-8-jdk",
    extra: <<-SHELL
      export DEBIAN_FRONTEND=noninteractive
      ls /etc/apt/sources.list.d/#{openjdk_list}.list > /dev/null 2>&1 ||
        (echo "Importing java-8 ppa" &&
          #{add_openjdk_repository_command} &&
          apt-get update -o \
            Dir::Etc::sourcelist="$(ls /etc/apt/sources.list.d/#{openjdk_list}.list)" \
            -o Dir::Etc::sourceparts="-" -o APT::Get::List-Cleanup="0")
SHELL
  )
end

def rpm_common(config)
  provision(config,
    update_command: "yum check-update",
    update_tracking_file: "/var/cache/yum/last_update",
    install_command: "yum install -y",
    java_package: "java-1.8.0-openjdk-devel")
end

def dnf_common(config)
  provision(config,
    update_command: "dnf check-update",
    update_tracking_file: "/var/cache/dnf/last_update",
    install_command: "dnf install -y",
    java_package: "java-1.8.0-openjdk-devel")
  if Vagrant.has_plugin?("vagrant-cachier")
    # Autodetect doesn't work....
    config.cache.auto_detect = false
    config.cache.enable :generic, { :cache_dir => "/var/cache/dnf" }
  end
end

# Register the main box provisioning script.
# @param config Vagrant's config object. Required.
# @param update_command [String] The command used to update the package
#   manager. Required. Think `apt-get update`.
# @param update_tracking_file [String] The location of the file tracking the
#   last time the update command was run. Required. Should be in a place that
#   is cached by vagrant-cachier.
# @param install_command [String] The command used to install a package.
#   Required. Think `apt-get install #{package}`.
# @param java_package [String] The name of the java package. Required.
# @param extra [String] Extra provisioning commands run before anything else.
#   Optional. Used for things like setting up the ppa for Java 8.
def provision(config,
    update_command: 'required',
    update_tracking_file: 'required',
    install_command: 'required',
    java_package: 'required',
    extra: '')
  # Vagrant run ruby 2.0.0 which doesn't have required named parameters....
  raise ArgumentError.new('update_command is required') if update_command == 'required'
  raise ArgumentError.new('update_tracking_file is required') if update_tracking_file == 'required'
  raise ArgumentError.new('install_command is required') if install_command == 'required'
  raise ArgumentError.new('java_package is required') if java_package == 'required'
  config.vm.provision "bats dependencies", type: "shell", inline: <<-SHELL
    set -e
    set -o pipefail
    installed() {
      command -v $1 2>&1 >/dev/null
    }
    install() {
      # Only apt-get update if we haven't in the last day
      if [ ! -f #{update_tracking_file} ] || [ "x$(find #{update_tracking_file} -mtime +0)" == "x#{update_tracking_file}" ]; then
          #{update_command} || true
          touch #{update_tracking_file}
      fi
      #{install_command} $1
    }
    ensure() {
      installed $1 || install $1
    }

    #{extra}

    installed java || install #{java_package}
    ensure curl
    ensure unzip

    installed bats || {
      # Bats lives in a git repository....
      ensure git
      git clone https://github.com/sstephenson/bats /tmp/bats
      # Centos doesn't add /usr/local/bin to the path....
      /tmp/bats/install.sh /usr
      rm -rf /tmp/bats
    }
    cat \<\<VARS > /etc/profile.d/elasticsearch_vars.sh
export ZIP=/elasticsearch/distribution/zip/target/releases
export TAR=/elasticsearch/distribution/tar/target/releases
export RPM=/elasticsearch/distribution/rpm/target/releases
export DEB=/elasticsearch/distribution/deb/target/releases
export TESTROOT=/elasticsearch/qa/vagrant/target/testroot
export BATS=/elasticsearch/qa/vagrant/src/test/resources/packaging/scripts
VARS
  SHELL
end
