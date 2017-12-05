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

GRADLE_VERSION = '3.3'

boxes = {}
linux_boxes = {}

ubuntu_boxes = {
  'ubuntu-1404' => 'elastic/ubuntu-14.04-x86_64',
  'ubuntu-1604' => 'elastic/ubuntu-16.04-x86_64'
}.freeze
linux_boxes.merge!(ubuntu_boxes)

# Wheezy's backports don't contain Openjdk 8 and the backflips
# required to get the sun jdk on there just aren't worth it. We have
# jessie and stretch for testing debian and it works fine.
debian_boxes = {
  'debian-8' => 'elastic/debian-8-x86_64',
  'debian-9' => 'elastic/debian-9-x86_64'
}.freeze
linux_boxes.merge!(debian_boxes)

centos_boxes = {
  'centos-6' => 'elastic/centos-6-x86_64',
  'centos-7' => 'elastic/centos-7-x86_64'
}.freeze
linux_boxes.merge!(centos_boxes)

oel_boxes = {
  'oel-6' => 'elastic/oraclelinux-6-x86_64',
  'oel-7' => 'elastic/oraclelinux-7-x86_64'
}.freeze
linux_boxes.merge!(oel_boxes)

fedora_boxes = {
  'fedora-25' => 'elastic/fedora-25-x86_64',
  'fedora-26' => 'elastic/fedora-26-x86_64'
}.freeze
linux_boxes.merge!(fedora_boxes)

opensuse_boxes = {
  'opensuse-42' => 'elastic/opensuse-42-x86_64'
}.freeze
linux_boxes.merge!(opensuse_boxes)

sles_boxes = {
  'sles-12' => 'elastic/sles-12-x86_64'
}.freeze
linux_boxes.merge!(sles_boxes)

linux_boxes.freeze
boxes.merge!(linux_boxes)

windows_boxes = {}

windows_2012r2_box = ENV['VAGRANT_WINDOWS_2012R2_BOX']
if windows_2012r2_box && windows_2012r2_box.empty? == false
  windows_boxes['windows-2012r2'] = windows_2012r2_box
end

windows_2016_box = ENV['VAGRANT_WINDOWS_2016_BOX']
if windows_2016_box && windows_2016_box.empty? == false
  windows_boxes['windows-2016'] = windows_2016_box
end

windows_boxes.freeze

boxes = linux_boxes.merge(windows_boxes).freeze

def gradle_cache(guest_path)
  lambda do |config|
    config.vm.synced_folder "#{Dir.home}/.gradle/caches", guest_path,
      create: true,
      owner: 'vagrant'
  end
end

def sh_set_prompt(name)
  # Sets up a consistent prompt for all users. Or tries to. The VM might
  # contain overrides for root and vagrant but this attempts to work around
  # them by re-source-ing the standard prompt file.
  lambda do |config|
    config.vm.provision 'prompt sh', type: 'shell', inline: <<-SHELL
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
end

configuration_procs = Hash.new { |hash, key| hash[key] = Array.new }

# Share gradle cache
linux_boxes.keys.each do |name|
  configuration_procs[name] << gradle_cache('/home/vagrant/.gradle/caches')
end

windows_boxes.keys.each do |name|
  configuration_procs[name] << gradle_cache('/Users/vagrant/.gradle/caches')
end

# Workaround provisoning bug
# http://foo-o-rama.com/vagrant--stdin-is-not-a-tty--fix.html
(debian_boxes.keys + ubuntu_boxes.keys).each do |name|
  configuration_procs[name] << lambda do |config|
    config.vm.provision 'fix-no-tty', type: 'shell' do |s|
        s.privileged = false
        s.inline = "sudo sed -i '/tty/!s/mesg n/tty -s \\&\\& mesg n/' /root/.profile"
    end
  end
end

# Create vagrant vm markerfile
linux_boxes.keys.each do |name|
  configuration_procs[name] << lambda do |config|
    config.vm.provision 'markerfile', type: 'shell', inline: <<-SHELL
      touch /etc/is_vagrant_vm
    SHELL
  end
end

windows_boxes.keys.each do |name|
  configuration_procs[name] << lambda do |config|
    config.vm.provision 'markerfile', type: 'shell', inline: <<-SHELL
      New-Item \\is_vagrant_vm -ItemType file -Force | Out-Null
    SHELL
  end
end

# Set prompt to be machine name
linux_boxes.keys.each do |name|
  configuration_procs[name] << sh_set_prompt(name)

  configuration_procs[name] << lambda do |config|
    if Vagrant.has_plugin?('vagrant-cachier')
      config.cache.scope = :box
    end
  end
end

windows_boxes.keys.each do |name|
  configuration_procs[name] << lambda do |config|
    config.vm.provision 'prompt powershell', type: 'shell', inline: <<-SHELL
      $ErrorActionPreference = "Stop"
      # set powershell prompt for all users
      $ps_prompt = 'function Prompt { "#{name}:$($ExecutionContext.SessionState.Path.CurrentLocation)>" }'
      $ps_prompt | Out-File $PsHome\\Microsoft.PowerShell_profile.ps1
    SHELL
  end
end


# Install Jayatana so we can work around it being present.
configuration_procs['ubuntu-1604'] << lambda do |config|
  config.vm.provision 'install jayatana', type: 'shell', inline: sh_install_apt(
    '[ -f /usr/share/java/jayatanaag.jar ] || install jayatana'
  )
end
(debian_boxes.keys + ubuntu_boxes.keys).each do |name|
  configuration_procs[name] << lambda do |config|
    config.vm.provision 'install common deps (apt)', type: 'shell', inline: sh_install_apt(linux_deps)
  end
end

(centos_boxes.keys + oel_boxes.keys).each do |name|
  configuration_procs[name] << lambda do |config|
    config.vm.provision 'install common deps (rpm)', type: 'shell', inline: sh_install_rpm(linux_deps)
  end
end

fedora_boxes.keys.each do |name|
  configuration_procs[name] << lambda do |config|
    config.vm.provision 'install common deps (dnf)', type: 'shell', inline: sh_install_dnf(linux_deps)
    if Vagrant.has_plugin?("vagrant-cachier")
      # Autodetect doesn't work....
      config.cache.auto_detect = false
      config.cache.enable :generic, { :cache_dir => "/var/cache/dnf" }
    end
  end
end

sles_boxes.keys.each do |name|
  configuration_procs[name] << lambda do |config|
    config.vm.provision 'install puppet and git for sles', type: 'shell', inline: <<-SHELL
      zypper rr systemsmanagement_puppet puppetlabs-pc1
      zypper --non-interactive install git-core
    SHELL
  end
end

(sles_boxes.keys + opensuse_boxes.keys).each do |name|
  configuration_procs[name] << lambda do |config|
    config.vm.provision 'install common deps (zypper)', type: 'shell', inline: sh_install_zypper(linux_deps)
  end
end


windows_boxes.keys.each do |name|
  configuration_procs[name] << lambda do |config|
    config.vm.provision 'install common deps', type: 'shell', inline: windows_deps
  end
end

Vagrant.configure(2) do |config|

  config.vm.provider 'virtualbox' do |vbox|
    # Give the box more memory and cpu because our tests are beasts!
    vbox.memory = Integer(ENV['VAGRANT_MEMORY'] || 8192)
    vbox.cpus = Integer(ENV['VAGRANT_CPUS'] || 4)
  end

  # Vagrant and ruby stdlib expand '.' differently, so look for the build
  # in the directory containing the Vagrantfile
  vagrantfile_dir = File.expand_path('..', __FILE__)

  # Switch the h share for the project root from /vagrant to
  # /elasticsearch because /vagrant is confusing when there is a project inside
  # the elasticsearch project called vagrant....
  config.vm.synced_folder vagrantfile_dir, '/vagrant', disabled: true
  config.vm.synced_folder vagrantfile_dir, '/elasticsearch'

  extra_projects = "#{vagrantfile_dir}-extra"
  if Dir.exists?(extra_projects)
    config.vm.synced_folder extra_projects, '/elasticsearch-extra'
  end

  # Expose project directory
  PROJECT_DIR = ENV['VAGRANT_PROJECT_DIR'] || Dir.pwd
  config.vm.synced_folder PROJECT_DIR, '/project'

  boxes.each do |name, image|
    config.vm.define name, autostart: false do |config|
      config.vm.box = image
      configuration_procs[name].each do |configuration_proc|
        configuration_proc.call(config)
      end
    end
  end
end


def sh_install_apt(script)
  sh_install(
    script,
    update_command: 'apt-get update',
    update_tracking_file: '/var/cache/apt/archives/last_update',
    install_command: 'apt-get install -y'
  )
end

def sh_install_rpm(script)
  sh_install(
    script,
    update_command: 'yum check-update',
    update_tracking_file: '/var/cache/yum/last_update',
    install_command: 'yum install -y'
  )
end

def sh_install_dnf(script)
  sh_install(
    script,
    update_command: 'dnf check-update',
    update_tracking_file: '/var/cache/dnf/last_update',
    install_command: 'dnf install -y',
    install_command_retries: 5
  )
end

def sh_install_zypper(script)
  sh_install(
    script,
    update_command: 'zypper --non-interactive list-updates',
    update_tracking_file: '/var/cache/zypp/packages/last_update',
    install_command: 'zypper --non-interactive --quiet install --no-recommends'
  )
end

def linux_deps
  <<-SHELL
    installed java || {
      echo "==> Java is not installed"
      return 1
    }
    ensure tar
    ensure curl
    ensure unzip
    ensure rsync

    installed bats || {
      # Bats lives in a git repository....
      ensure git
      echo "==> Installing bats"
      git clone https://github.com/sstephenson/bats /tmp/bats
      # Centos doesn't add /usr/local/bin to the path....
      /tmp/bats/install.sh /usr
      rm -rf /tmp/bats
    }

    installed gradle || {
      echo "==> Installing Gradle"
      curl -sS -o /tmp/gradle.zip -L https://services.gradle.org/distributions/gradle-#{GRADLE_VERSION}-bin.zip
      unzip -q /tmp/gradle.zip -d /opt
      rm -rf /tmp/gradle.zip
      ln -s /opt/gradle-#{GRADLE_VERSION}/bin/gradle /usr/bin/gradle
      # make nfs mounted gradle home dir writeable
      chown vagrant:vagrant /home/vagrant/.gradle
    }

    cat \<\<VARS > /etc/profile.d/elasticsearch_vars.sh
export BATS=/project/build/packaging/bats
export BATS_UTILS=/project/build/packaging/bats/utils
export BATS_TESTS=/project/build/packaging/bats/tests
export BATS_ARCHIVES=/project/build/packaging/archives
export GRADLE_HOME=/opt/gradle-#{GRADLE_VERSION}
VARS
    cat \<\<SUDOERS_VARS > /etc/sudoers.d/elasticsearch_vars
Defaults   env_keep += "BATS"
Defaults   env_keep += "BATS_UTILS"
Defaults   env_keep += "BATS_TESTS"
Defaults   env_keep += "BATS_ARCHIVES"
SUDOERS_VARS
    chmod 0440 /etc/sudoers.d/elasticsearch_vars
  SHELL
end

def windows_deps
  <<-SHELL
    $ErrorActionPreference = "Stop"

    function Installed {
      Param(
        [string]$command
      )

      try {
        Get-Command $command
        return $true
      } catch {
        return $false
      }
    }

    if (-Not (Installed java)) {
      Write-Error "java is not installed"
    }

    if (-Not (Installed gradle)) {
      Write-Host "==> Installing gradle"
			$Source="https://services.gradle.org/distributions/gradle-#{GRADLE_VERSION}-bin.zip"
      $Zip="\\tmp\\gradle.zip"
      $Destination="\\gradle"
      New-Item (Split-Path $Zip) -Type Directory -ErrorAction Ignore | Out-Null
      (New-Object Net.WebClient).DownloadFile($Source, $Zip)
      Add-Type -assembly "System.IO.Compression.Filesystem"
      [IO.Compression.ZipFile]::ExtractToDirectory($Zip, $Destination)
      Remove-Item $Zip
      [Environment]::SetEnvironmentVariable("Path", $env:Path + ";$Destination\\gradle-#{GRADLE_VERSION}\\bin", "Machine")
      [Environment]::SetEnvironmentVariable("GRADLE_HOME", "$Destination\\gradle-#{GRADLE_VERSION}", "Machine")
    }
  SHELL
end

# Build the script for installing build dependencies
# @param script [String] the script to run with install statements
# @param update_command [String] The command used to update the package
#   manager. Required. Think `apt-get update`.
# @param update_tracking_file [String] The location of the file tracking the
#   last time the update command was run. Required. Should be in a place that
#   is cached by vagrant-cachier.
# @param install_command [String] The command used to install a package.
#   Required. Think `apt-get install #{package}`.
#
def sh_install(script,
                 update_command: 'required',
                 update_tracking_file: 'required',
                 install_command: 'required',
                 install_command_retries: 0)

  raise ArgumentError, 'update_command is required' if update_command == 'required'
  raise ArgumentError, 'update_tracking_file is required' if update_tracking_file == 'required'
  raise ArgumentError, 'install_command is required' if install_command == 'required'

  command = ''
  command << <<-SHELL
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
  SHELL
  command << script
  command
end
