# -*- mode: ruby -*-
# vim: ft=ruby ts=2 sw=2 sts=2 et:

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

define_opts = {
  autostart: false
}.freeze

Vagrant.configure(2) do |config|

  config.vm.provider 'virtualbox' do |vbox|
    # Give the box more memory and cpu because our tests are beasts!
    vbox.memory = Integer(ENV['VAGRANT_MEMORY'] || 8192)
    vbox.cpus = Integer(ENV['VAGRANT_CPUS'] || 4)

    # see https://github.com/hashicorp/vagrant/issues/9524
    vbox.customize ["modifyvm", :id, "--audio", "none"]
  end

  # Switch the default share for the project root from /vagrant to
  # /elasticsearch because /vagrant is confusing when there is a project inside
  # the elasticsearch project called vagrant....
  config.vm.synced_folder '.', '/vagrant', disabled: true
  config.vm.synced_folder '.', '/elasticsearch'
  # TODO: make these syncs work for windows!!!
  config.vm.synced_folder "#{Dir.home}/.vagrant/gradle/caches/jars-3", "/root/.gradle/caches/jars-3",
      create: true,
      owner: "vagrant"
  config.vm.synced_folder "#{Dir.home}/.vagrant/gradle/caches/modules-2", "/root/.gradle/caches/modules-2",
      create: true,
      owner: "vagrant"
  config.vm.synced_folder "#{Dir.home}/.gradle/wrapper", "/root/.gradle/wrapper",
      create: true,
      owner: "vagrant"

  # Expose project directory. Note that VAGRANT_CWD may not be the same as Dir.pwd
  PROJECT_DIR = ENV['VAGRANT_PROJECT_DIR'] || Dir.pwd
  config.vm.synced_folder PROJECT_DIR, '/project'

  'ubuntu-1604'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/ubuntu-16.04-x86_64'
      deb_common config, box, extra: <<-SHELL
        # Install Jayatana so we can work around it being present.
        [ -f /usr/share/java/jayatanaag.jar ] || install jayatana
      SHELL
      ubuntu_docker config
    end
  end
  'ubuntu-1804'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/ubuntu-18.04-x86_64'
      deb_common config, box, extra: <<-SHELL
       # Install Jayatana so we can work around it being present.
       [ -f /usr/share/java/jayatanaag.jar ] || install jayatana
      SHELL
      ubuntu_docker config
    end
  end
  'debian-8'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/debian-8-x86_64'
      deb_common config, box, extra: <<-SHELL
        # this sometimes gets a bad ip, and doesn't appear to be needed
        rm -f /etc/apt/sources.list.d/http_debian_net_debian.list
      SHELL
    end
  end
  'debian-9'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/debian-9-x86_64'
      deb_common config, box
      deb_docker config
    end
  end
  'centos-6'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/centos-6-x86_64'
      rpm_common config, box
    end
  end
  'centos-7'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/centos-7-x86_64'
      rpm_common config, box
      rpm_docker config
    end
  end
  'oel-6'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/oraclelinux-6-x86_64'
      rpm_common config, box
    end
  end
  'oel-7'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/oraclelinux-7-x86_64'
      rpm_common config, box
    end
  end
  'fedora-28'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/fedora-28-x86_64'
      dnf_common config, box
      dnf_docker config
    end
  end
  'fedora-29'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/fedora-29-x86_64'
      dnf_common config, box
      dnf_docker config
    end
  end
  'opensuse-42'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/opensuse-42-x86_64'
      suse_common config, box
    end
  end
  'sles-12'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/sles-12-x86_64'
      sles_common config, box
    end
  end
  'rhel-8'.tap do |box|
    config.vm.define box, define_opts do |config|
      config.vm.box = 'elastic/rhel-8-x86_64'
      rpm_common config, box
    end
  end

  windows_2012r2_box = ENV['VAGRANT_WINDOWS_2012R2_BOX']
  if windows_2012r2_box && windows_2012r2_box.empty? == false
    'windows-2012r2'.tap do |box|
      config.vm.define box, define_opts do |config|
        config.vm.box = windows_2012r2_box
        windows_common config, box
      end
    end
  end

  windows_2016_box = ENV['VAGRANT_WINDOWS_2016_BOX']
  if windows_2016_box && windows_2016_box.empty? == false
    'windows-2016'.tap do |box|
      config.vm.define box, define_opts do |config|
        config.vm.box = windows_2016_box
        windows_common config, box
      end
    end
  end
end

def deb_common(config, name, extra: '')
  # http://foo-o-rama.com/vagrant--stdin-is-not-a-tty--fix.html
  config.vm.provision 'fix-no-tty', type: 'shell' do |s|
      s.privileged = false
      s.inline = "sudo sed -i '/tty/!s/mesg n/tty -s \\&\\& mesg n/' /root/.profile"
  end
  extra_with_lintian = <<-SHELL
    #{extra}
    install lintian
  SHELL
  linux_common(
    config,
    name,
    update_command: 'apt-get update',
    update_tracking_file: '/var/cache/apt/archives/last_update',
    install_command: 'apt-get install -y',
    extra: extra_with_lintian
  )
end

def ubuntu_docker(config)
  config.vm.provision 'install Docker using apt', type: 'shell', inline: <<-SHELL
    # Install packages to allow apt to use a repository over HTTPS
    apt-get install -y \
      apt-transport-https \
      ca-certificates \
      curl \
      gnupg2 \
      software-properties-common

    # Add Docker’s official GPG key
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -

    # Set up the stable Docker repository
    add-apt-repository \
      "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
      $(lsb_release -cs) \
      stable"

    # Install Docker. Unlike Fedora and CentOS, this also start the daemon.
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io

    # Add vagrant to the Docker group, so that it can run commands
    usermod -aG docker vagrant

    # Enable IPv4 forwarding
    sed -i '/net.ipv4.ip_forward/s/^#//' /etc/sysctl.conf
    systemctl restart networking
  SHELL
end


def deb_docker(config)
  config.vm.provision 'install Docker using apt', type: 'shell', inline: <<-SHELL
    # Install packages to allow apt to use a repository over HTTPS
    apt-get install -y \
      apt-transport-https \
      ca-certificates \
      curl \
      gnupg2 \
      software-properties-common

    # Add Docker’s official GPG key
    curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -

    # Set up the stable Docker repository
    add-apt-repository \
      "deb [arch=amd64] https://download.docker.com/linux/debian \
      $(lsb_release -cs) \
      stable"

    # Install Docker. Unlike Fedora and CentOS, this also start the daemon.
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io

    # Add vagrant to the Docker group, so that it can run commands
    usermod -aG docker vagrant
  SHELL
end

def rpm_common(config, name)
  linux_common(
    config,
    name,
    update_command: 'yum check-update',
    update_tracking_file: '/var/cache/yum/last_update',
    install_command: 'yum install -y'
  )
end

def rpm_docker(config)
  config.vm.provision 'install Docker using yum', type: 'shell', inline: <<-SHELL
    # Install prerequisites
    yum install -y yum-utils device-mapper-persistent-data lvm2

    # Add repository
    yum-config-manager -y --add-repo https://download.docker.com/linux/centos/docker-ce.repo

    # Install Docker
    yum install -y docker-ce docker-ce-cli containerd.io

    # Start Docker
    systemctl enable --now docker

    # Add vagrant to the Docker group, so that it can run commands
    usermod -aG docker vagrant
  SHELL
end

def dnf_common(config, name)
  # Autodetect doesn't work....
  if Vagrant.has_plugin?('vagrant-cachier')
    config.cache.auto_detect = false
    config.cache.enable :generic, { :cache_dir => '/var/cache/dnf' }
  end
  linux_common(
    config,
    name,
    update_command: 'dnf check-update',
    update_tracking_file: '/var/cache/dnf/last_update',
    install_command: 'dnf install -y',
    install_command_retries: 5
  )
end

def dnf_docker(config)
  config.vm.provision 'install Docker using dnf', type: 'shell', inline: <<-SHELL
    # Install prerequisites
    dnf -y install dnf-plugins-core

    # Add repository
    dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo

    # Install Docker
    dnf install -y docker-ce docker-ce-cli containerd.io

    # Start Docker
    systemctl enable --now docker

    # Add vagrant to the Docker group, so that it can run commands
    usermod -aG docker vagrant
  SHELL
end

def suse_common(config, name, extra: '')
  linux_common(
    config,
    name,
    update_command: 'zypper --non-interactive list-updates',
    update_tracking_file: '/var/cache/zypp/packages/last_update',
    install_command: 'zypper --non-interactive --quiet install --no-recommends',
    extra: extra
  )
end

def sles_common(config, name)
  extra = <<-SHELL
    zypper rr systemsmanagement_puppet puppetlabs-pc1
    zypper --non-interactive install git-core
  SHELL
  suse_common config, name, extra: extra
end

# Configuration needed for all linux boxes
# @param config Vagrant's config object. Required.
# @param name [String] The box name. Required.
# @param update_command [String] The command used to update the package
#   manager. Required. Think `apt-get update`.
# @param update_tracking_file [String] The location of the file tracking the
#   last time the update command was run. Required. Should be in a place that
#   is cached by vagrant-cachier.
# @param install_command [String] The command used to install a package.
#   Required. Think `apt-get install #{package}`.
# @param install_command_retries [Integer] Number of times to retry
#   a failed install command
# @param extra [String] Additional script to run before installing
#   dependencies
#
def linux_common(config,
                 name,
                 update_command: 'required',
                 update_tracking_file: 'required',
                 install_command: 'required',
                 install_command_retries: 0,
                 extra: '')

  raise ArgumentError, 'update_command is required' if update_command == 'required'
  raise ArgumentError, 'update_tracking_file is required' if update_tracking_file == 'required'
  raise ArgumentError, 'install_command is required' if install_command == 'required'

  if Vagrant.has_plugin?('vagrant-cachier')
    config.cache.scope = :box
  end

  config.vm.provision 'markerfile', type: 'shell', inline: <<-SHELL
    touch /etc/is_vagrant_vm
    touch /is_vagrant_vm # for consistency between linux and windows
  SHELL

  # This prevents leftovers from previous tests using the
  # same VM from messing up the current test
  config.vm.provision 'clean es installs in tmp', type: 'shell', inline: <<-SHELL
    rm -rf /tmp/elasticsearch*
  SHELL

  sh_set_prompt config, name
  sh_install_deps(
    config,
    update_command,
    update_tracking_file,
    install_command,
    install_command_retries,
    extra
  )
end

# Sets up a consistent prompt for all users. Or tries to. The VM might
# contain overrides for root and vagrant but this attempts to work around
# them by re-source-ing the standard prompt file.
def sh_set_prompt(config, name)
  config.vm.provision 'set prompt', type: 'shell', inline: <<-SHELL
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

def sh_install_deps(config,
                    update_command,
                    update_tracking_file,
                    install_command,
                    install_command_retries,
                    extra)
  config.vm.provision 'install dependencies', type: 'shell', inline:  <<-SHELL
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
      echo "==> Java is not installed"
      return 1
    }
    cat \<\<JAVA > /etc/profile.d/java_home.sh
if [ ! -z "\\\$JAVA_HOME" ]; then
  export SYSTEM_JAVA_HOME=\\\$JAVA_HOME
  unset JAVA_HOME
fi
JAVA
    ensure tar
    ensure curl
    ensure unzip
    ensure rsync
    ensure expect

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
export PACKAGING_TESTS=/project/build/packaging/tests
VARS
    cat \<\<SUDOERS_VARS > /etc/sudoers.d/elasticsearch_vars
Defaults   env_keep += "ZIP"
Defaults   env_keep += "TAR"
Defaults   env_keep += "RPM"
Defaults   env_keep += "DEB"
Defaults   env_keep += "PACKAGING_ARCHIVES"
Defaults   env_keep += "PACKAGING_TESTS"
Defaults   env_keep += "BATS_UTILS"
Defaults   env_keep += "BATS_TESTS"
Defaults   env_keep += "JAVA_HOME"
Defaults   env_keep += "SYSTEM_JAVA_HOME"
SUDOERS_VARS
    chmod 0440 /etc/sudoers.d/elasticsearch_vars
  SHELL
end

def windows_common(config, name)
  config.vm.provision 'markerfile', type: 'shell', inline: <<-SHELL
    $ErrorActionPreference = "Stop"
    New-Item C:/is_vagrant_vm -ItemType file -Force | Out-Null
  SHELL

  config.vm.provision 'set prompt', type: 'shell', inline: <<-SHELL
    $ErrorActionPreference = "Stop"
    $ps_prompt = 'function Prompt { "#{name}:$($ExecutionContext.SessionState.Path.CurrentLocation)>" }'
    $ps_prompt | Out-File $PsHome/Microsoft.PowerShell_profile.ps1
  SHELL

  config.vm.provision 'set env variables', type: 'shell', inline: <<-SHELL
    $ErrorActionPreference = "Stop"
    [Environment]::SetEnvironmentVariable("PACKAGING_ARCHIVES", "C:/project/build/packaging/archives", "Machine")
    [Environment]::SetEnvironmentVariable("PACKAGING_TESTS", "C:/project/build/packaging/tests", "Machine")
    [Environment]::SetEnvironmentVariable("JAVA_HOME", $null, "Machine")
  SHELL
end
