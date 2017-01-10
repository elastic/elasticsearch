## Vagrant build environment

This provides a vagrant box for building the C++ side of Ml (and the Java side,
although that is easily accomplished outside vagrant).

Provisioning the box will take a fair amount of time, since it needs to download
and compile a number of dependencies.


### Details
- Ubuntu Trusty64 (14.04.5 LTS)
- 25% of host's memory
- 100% host's cores
- Maps ml source repository to `/home/vagrant/ml/src`
  - Directory is shared with the host, so you can point your IDE to the ml repo
    and build inside vagrant
- Maps ml build directory to `/home/vagrant/ml/build`
- Changes into `/home/vagrant/ml/src` on login

### Pre-baked box
Don't feel like compiling the entire box?  No fear, there's a pre-baked box available
on S3.  It is ~1.1gb to download:

```bash
# Change into some random directory to download the box.
# Doesn't matter where this goes, but *cannot* go into prelert-legacy/vagrant
$ cd ~/some_directory

# Export the path to your prelert-legacy repo. This is so the box knows where
# to sync the folders
$ export ML_SRC_HOME=/path/to/prelert-legacy

# Download the box from S3
$ s3cmd get s3://ml-elastic-dump/ml_env.box
  # ...
  # Downloading...
  # ...

$ vagrant box add ml ml_env.box
$ vagrant init ml
$ vagrant up
$ vagrant ssh

  Welcome to Ubuntu 14.04.5 LTS (GNU/Linux 3.13.0-96-generic x86_64)
  ...
  ...
  Last login: Tue Oct  4 16:06:32 2016 from 10.0.2.2

vagrant@vagrant-ubuntu-trusty-64:~/ml/src$
```  

Once you've logged into the box, you'll be in the ml source directory. You
can build immediately via:

```bash
vagrant@vagrant-ubuntu-trusty-64:~/ml/src$ gradle cppmake
```
The pre-baked box has already compiled ml once, so subsequent compilations
should happen considerably faster.

### Compiling from Scratch

If you feel like compiling everything from scratch instead of downloading the pre-baked
box, simply `vagrant up` and let the provisioners run:

```bash
$ cd prelert-legacy/vagrant
$ vagrant up
  # ...
  # wait while vagrant provisions
  # ...
$ vagrant ssh

  Welcome to Ubuntu 14.04.5 LTS (GNU/Linux 3.13.0-96-generic x86_64)
  ...
  ...
  Last login: Tue Oct  4 16:06:32 2016 from 10.0.2.2

vagrant@vagrant-ubuntu-trusty-64:~/ml/src$
```

Once you've logged into the box, you'll be in the ml source directory. You
can build immediately via:

```bash
vagrant@vagrant-ubuntu-trusty-64:~/ml/src$ gradle cppmake
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
