## Vagrant build environment

This provides a vagrant box for building the C++ side of Prelert (and the Java side,
although that is easily accomplished outside vagrant).

Provisioning the box will take a fair amount of time, since it needs to download
and compile a number of dependencies.

A pre-provisioned box can be downloaded from: TODO

### Details
- Ubuntu Trusty64 (14.04.5 LTS)
- 25% of host's memory
- 100% host's cores
- Maps prelert source repository to `/home/vagrant/prelert/src`
  - Directory is shared with the host, so you can point your IDE to the prelert repo
    and build inside vagrant
- Maps prelert build directory to `/home/vagrant/prelert/build`
- Changes into `/home/vagrant/prelert/src` on login

### Usage

```bash
$ cd prelert-legacy
$ cd vagrant
$ vagrant up
  # ...
  # wait while vagrant provisions
  # ...
$ vagrant ssh

Welcome to Ubuntu 14.04.5 LTS (GNU/Linux 3.13.0-96-generic x86_64)

 * Documentation:  https://help.ubuntu.com/

  System information as of Tue Oct  4 16:06:31 UTC 2016

  System load:  0.0                Processes:           196
  Usage of /:   12.0% of 39.34GB   Users logged in:     0
  Memory usage: 2%                 IP address for eth0: 10.0.2.15
  Swap usage:   0%

  Graph this data and manage this system at:
    https://landscape.canonical.com/

  Get cloud support with Ubuntu Advantage Cloud Guest:
    http://www.ubuntu.com/business/services/cloud

New release '16.04.1 LTS' available.
Run 'do-release-upgrade' to upgrade to it.


Last login: Tue Oct  4 16:06:32 2016 from 10.0.2.2
vagrant@vagrant-ubuntu-trusty-64:~/prelert/src$
```

Once you've logged into the box, you'll be in the prelert source directory. You
can build immediately via:

```bash
vagrant@vagrant-ubuntu-trusty-64:~/prelert/src$ gradle cppmake
  # ...
  # much building
  # ...
```

### Suspending your box
Once you've provisioned a box, you can use `vagrant suspend` to "sleep" the box.
This has the advantage of rebooting quickly when you `vagrant up`, and returns you
to exactly what you were doing.  On the downside, it eats more disk space since it
needs to sleep the entire image.

You can alternatively use `vagrant halt`, which gracefully powers down the machine.
Rebooting via `vagrant up` takes longer since you are rebooting the entire OS,
but it saves more disk space.

### Fixing a broken box
If you accidentally kill the provisioning process before it completes, you can
attempt to reprovision it with `vagrant reload --provision`.  That will run
the provisioners that did not complete previously.

If your box is still horribly broken, you can destroy it with `vagrant destroy`
and try again with `vagrant up`  
