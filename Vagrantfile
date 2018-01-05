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
  config.vm.define "ubuntu-1404" do |config|
    config.vm.box = "elastic/ubuntu-14.04-x86_64"
    ubuntu_common config
  end
  config.vm.define "ubuntu-1604" do |config|
    config.vm.box = "elastic/ubuntu-16.04-x86_64"
    ubuntu_common config, extra: <<-SHELL
      # Install Jayatana so we can work around it being present.
      [ -f /usr/share/java/jayatanaag.jar ] || install jayatana
    SHELL
  end
  # Wheezy's backports don't contain Openjdk 8 and the backflips
  # required to get the sun jdk on there just aren't worth it. We have
  # jessie and stretch for testing debian and it works fine.
  config.vm.define "debian-8" do |config|
    config.vm.box = "elastic/debian-8-x86_64"
    deb_common config
  end
  config.vm.define "debian-9" do |config|
    config.vm.box = "elastic/debian-9-x86_64"
    deb_common config
  end
  config.vm.define "centos-6" do |config|
    config.vm.box = "elastic/centos-6-x86_64"
    rpm_common config
  end
  config.vm.define "centos-7" do |config|
    config.vm.box = "elastic/centos-7-x86_64"
    rpm_common config
  end
  config.vm.define "oel-6" do |config|
    config.vm.box = "elastic/oraclelinux-6-x86_64"
    rpm_common config
  end
  config.vm.define "oel-7" do |config|
    config.vm.box = "elastic/oraclelinux-7-x86_64"
    rpm_common config
  end
  config.vm.define "fedora-26" do |config|
    config.vm.box = "elastic/fedora-26-x86_64"
    dnf_common config
  end
  config.vm.define "fedora-27" do |config|
    config.vm.box = "elastic/fedora-27-x86_64"
    dnf_common config
  end
  config.vm.define "opensuse-42" do |config|
    config.vm.box = "elastic/opensuse-42-x86_64"
    opensuse_common config
  end
  config.vm.define "sles-12" do |config|
    config.vm.box = "elastic/sles-12-x86_64"
    sles_common config
  end
  # Switch the default share for the project root from /vagrant to
  # /elasticsearch because /vagrant is confusing when there is a project inside
  # the elasticsearch project called vagrant....
  config.vm.synced_folder ".", "/vagrant", disabled: true
  config.vm.synced_folder ".", "/elasticsearch"
  # Expose project directory
  PROJECT_DIR = ENV['VAGRANT_PROJECT_DIR'] || Dir.pwd
  config.vm.synced_folder PROJECT_DIR, "/project"
  config.vm.provider "virtualbox" do |v|
    # Give the boxes 3GB because Elasticsearch defaults to using 2GB
    v.memory = 3072
  end
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
      # Creates a file to mark the machine as created by vagrant. Tests check
      # for this file and refuse to run if it is not present so that they can't
      # be run unexpectedly.
      config.vm.provision "markerfile", type: "shell", inline: <<-SHELL
        touch /etc/is_vagrant_vm
      SHELL
    end
    config.config_procs.push ['2', set_prompt]
  end
end

def ubuntu_common(config, extra: '')
  deb_common config, extra: extra
end

def deb_common(config, extra: '')
  # http://foo-o-rama.com/vagrant--stdin-is-not-a-tty--fix.html
  config.vm.provision "fix-no-tty", type: "shell" do |s|
      s.privileged = false
      s.inline = "sudo sed -i '/tty/!s/mesg n/tty -s \\&\\& mesg n/' /root/.profile"
  end
  provision(config,
    update_command: "apt-get update",
    update_tracking_file: "/var/cache/apt/archives/last_update",
    install_command: "apt-get install -y",
    extra: extra)
end

def rpm_common(config)
  provision(config,
    update_command: "yum check-update",
    update_tracking_file: "/var/cache/yum/last_update",
    install_command: "yum install -y")
end

def dnf_common(config)
  provision(config,
    update_command: "dnf check-update",
    update_tracking_file: "/var/cache/dnf/last_update",
    install_command: "dnf install -y",
    install_command_retries: 5)
  if Vagrant.has_plugin?("vagrant-cachier")
    # Autodetect doesn't work....
    config.cache.auto_detect = false
    config.cache.enable :generic, { :cache_dir => "/var/cache/dnf" }
  end
end

def opensuse_common(config)
  suse_common config, ''
end

def suse_common(config, extra)
  provision(config,
    update_command: "zypper --non-interactive list-updates",
    update_tracking_file: "/var/cache/zypp/packages/last_update",
    install_command: "zypper --non-interactive --quiet install --no-recommends",
    extra: extra)
end

def sles_common(config)
  extra = <<-SHELL
    zypper rr systemsmanagement_puppet puppetlabs-pc1
    zypper --non-interactive install git-core
SHELL
  suse_common config, extra
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
# @param extra [String] Extra provisioning commands run before anything else.
#   Optional. Used for things like setting up the ppa for Java 8.
def provision(config,
    update_command: 'required',
    update_tracking_file: 'required',
    install_command: 'required',
    install_command_retries: 0,
    extra: '')
  # Vagrant run ruby 2.0.0 which doesn't have required named parameters....
  raise ArgumentError.new('update_command is required') if update_command == 'required'
  raise ArgumentError.new('update_tracking_file is required') if update_tracking_file == 'required'
  raise ArgumentError.new('install_command is required') if install_command == 'required'
  config.vm.provider "virtualbox" do |v|
    # Give the box more memory and cpu because our tests are beasts!
    v.memory = Integer(ENV['VAGRANT_MEMORY'] || 8192)
    v.cpus = Integer(ENV['VAGRANT_CPUS'] || 4)
  end
  config.vm.provision "dependencies", type: "shell", inline: <<-SHELL
    set -e
    set -o pipefail

    # Retry install command up to $2 times, if failed
    retry_installcommand() {
      n=0
      while true; do
        #{install_command} $1 && break
        let n=n+1
        if [ $n -ge $2 ]; then
          echo "==> Exhausted retries to install $1"
          return 1
        fi
        echo "==> Retrying installing $1, attempt $((n+1))"
        # Add a small delay to increase chance of metalink providing updated list of mirrors
        sleep 5
      done
    }

    installed() {
      command -v $1 2>&1 >/dev/null
    }

    install() {
      # Only apt-get update if we haven't in the last day
      if [ ! -f #{update_tracking_file} ] || [ "x$(find #{update_tracking_file} -mtime +0)" == "x#{update_tracking_file}" ]; then
        echo "==> Updating repository"
        #{update_command} || true
        touch #{update_tracking_file}
      fi
      echo "==> Installing $1"
      if [ #{install_command_retries} -eq 0 ]
      then
            #{install_command} $1
      else
            retry_installcommand $1 #{install_command_retries}
      fi
    }

    ensure() {
      installed $1 || install $1
    }

    #{extra}

    installed java || {
      echo "==> Java is not installed on vagrant box ${config.vm.box}"
      return 1
    }
    ensure tar
    ensure curl
    ensure unzip

    installed bats || {
      # Bats lives in a git repository....
      ensure git
      echo "==> Installing bats"
      git clone https://github.com/sstephenson/bats /tmp/bats
      # Centos doesn't add /usr/local/bin to the path....
      /tmp/bats/install.sh /usr
      rm -rf /tmp/bats
    }

    cat \<\<VARS > /etc/profile.d/elasticsearch_vars.sh
export ZIP=/elasticsearch/distribution/zip/build/distributions
export TAR=/elasticsearch/distribution/tar/build/distributions
export RPM=/elasticsearch/distribution/rpm/build/distributions
export DEB=/elasticsearch/distribution/deb/build/distributions
export BATS=/project/build/bats
export BATS_UTILS=/project/build/bats/utils
export BATS_TESTS=/project/build/bats/tests
export BATS_ARCHIVES=/project/build/bats/archives
VARS
    cat \<\<SUDOERS_VARS > /etc/sudoers.d/elasticsearch_vars
Defaults   env_keep += "ZIP"
Defaults   env_keep += "TAR"
Defaults   env_keep += "RPM"
Defaults   env_keep += "DEB"
Defaults   env_keep += "BATS"
Defaults   env_keep += "BATS_UTILS"
Defaults   env_keep += "BATS_TESTS"
Defaults   env_keep += "BATS_ARCHIVES"
SUDOERS_VARS
    chmod 0440 /etc/sudoers.d/elasticsearch_vars
  SHELL
end
