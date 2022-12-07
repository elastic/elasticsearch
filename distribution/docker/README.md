# Elasticsearch Docker Distribution

The ES build can generate several types of Docker image. These are enumerated in
the [DockerBase] enum.

   * Default - this is what most people use, and is based on Ubuntu
   * UBI - the same as the default image, but based upon [RedHat's UBI
     images][ubi], specifically their minimal flavour.
   * Iron Bank - this is the US Department of Defence's repository of digitally
     signed, binary container images including both Free and Open-Source
     software (FOSS) and Commercial off-the-shelf (COTS). In practice, this is
     another UBI build, this time on the regular UBI image, with extra
     hardening. See below for more details.
   * Cloud - this is mostly the same as the default image, with some notable differences:
      * `filebeat` and `metricbeat` are included
      * `wget` is included
      * The `ENTRYPOINT` is just `/bin/tini`, and the `CMD` is
        `/app/elasticsearc.sh`. In normal use this file would be bind-mounted
        in, but the image ships a stub version of this file so that the image
        can still be tested.
   * Cloud ESS - this directly extends the Cloud image, and adds all ES plugins
     that the ES build generates in an archive directory. It also sets an
     environment variable that points at this directory. This allows plugins to
     be installed from the archive instead of the internet, speeding up
     deployment times.

The long-term goal is for both Cloud images to be retired in favour of the
default image.


## Build strategy

For all image flavours, the ES build implements a pipeline:

   1. Construct a Docker build context
   2. Transform the build context so that it is possible to build it locally
   3. Build the Docker image locally

Some images use (1) as the releasable artifact, some use (3).

**NOTE:** "Pipeline" isn't actually the correct term - in reality, each Gradle
task depends on the one before it. Notably, it is the transform tasks that
depend on a locally build `.tar.gz` Elasticsearch archive.


## Releasing on Docker Hub

Elasticsearch is an [official image on Docker
Hub](https://hub.docker.com/_/elasticsearch). On release day, we build the ES
Docker image and upload it to [Elastic's Docker
registry](https://www.docker.elastic.co/). Separately, we submit a build context
to Docker via the [elastic/dockerfiles](https://github.com/elastic/dockerfiles)
repository. Docker then builds the image, and uploads it to Docker Hub.
Unfortunately, this is an asynchronous process, and we don't hear back if
there's a failure, so even when everything works, there's a lag between
releasing a new version of Elasticsearch, and the image being available on
Docker Hub.

Being an official image puts additional constraints on how the Elasticsearch
image is built.

   * It must extend another official image
   * It must fetch any required artifacts - they cannot be supplied in the build
     context.
   * It must be platform-independent i.e. it can build on ARM and x64

The transform step in the [build strategy](#build-strategy) above replaces the
`curl` command in the `Dockerfile` that fetches an Elasticsearch `.tar.gz`
distribution with a `COPY` command, so that it is possible to build the ES image
locally.

## Iron Bank release process

Elastic does not release an Iron Bank image. Rather, for each release we provide
a Docker build context, and Iron Bank build the image themselves using a custom
build process.

The ES build still has a task to build an Iron Bank image, in order to test
something close to what Iron Bank build. The ES build does this by transforming
the files in the Docker build context slightly, and passing usable values for
the build variables (we use the regular UBI image instead of the DoD one).

The important takeaway here is that the releasable artifact is the Iron Bank
build context, not the image.


## Multi-architecture images

We publish [multi-architecture images][multi-arch] images, for use on both
`x86_64` (Intel) and `aarch64` (ARM). This works by essentially building two
images, and combining them with a Docker manifest. The Elasticsearch Delivery
team aren't responsible for this - rather, it happens during our unified release
process.

To build multi-architecture images on `x86_64` hosts using Docker[^1], you'll
need [buildx](https://docs.docker.com/build/buildx/install/) and ensure that it
supports both `linux/amd64` **and** `linux/arm64` targets.

You can verify the supported targets using `docker buildx ls`. For example, the
following output indicates that support for `linux/arm64` is missing:

```shell
$ docker buildx ls
NAME/NODE DRIVER/ENDPOINT STATUS  BUILDKIT PLATFORMS
default * docker
  default default         running 20.10.21 linux/amd64, linux/386
```

On Linux `x86_64` hosts, to enable `linux-arm64` you need to install
[qemu-user-static-binfmt](https://github.com/multiarch/qemu-user-static).
Installation details depend on the Linux distribution but, as described in the
[getting started docs](https://github.com/multiarch/qemu-user-static#getting-started),
running `docker run --rm --privileged multiarch/qemu-user-static --reset -p yes`
will add the necessary support (but will not persist across reboots):

```shell
$ docker buildx ls
NAME/NODE DRIVER/ENDPOINT STATUS  BUILDKIT PLATFORMS
default * docker
  default default         running 20.10.21 linux/amd64, linux/arm64, linux/riscv64, linux/ppc64le, linux/s390x, linux/386, linux/arm/v7, linux/arm/v6
```

## Testing

We have a suite of tests in the [qa/os](../../qa/os) subproject. Most of the
Docker tests are in the [DockerTests] class, but there are tests that use Docker
in other test classes.

The tests are mostly concerned with ensuring that the image has been built
correctly e.g. contents and permissions are correct. We also check that the
custom behaviour in the
[docker-entrypoint.sh](src/docker/bin/docker-entrypoint.sh) works as intended.


## Reliability

We go to some lengths to try and make the Docker build resilient to transient
network errors. This is why, when browsing the
[Dockerfile](src/docker/Dockerfile), you'll see many commands wrapped in looping
logic, so that if e.g. package installation fails, we try again. We also perform
explicit `docker pull` commands instead of relying on `docker run` to pull an
image down automatically, so that we can wrap the `pull` part in a retry.


## What are the export project for?

Our integration tests are set up so that the test task depends on the project
that creates the required artifacts. Note, it doesn't depend on a task, but a
project! Also, we used to use Vagrant for testing (this has largely since been
abandoned), which meant we needed to be able to build an image locally, export
it, and load it again inside a Vagrant VM.

Ideally this import / export stuff should be completely removed.


[DockerBase]: ../../build-tools-internal/src/main/java/org/elasticsearch/gradle/internal/DockerBase.java
[DockerTests]: ../../qa/os/src/test/java/org/elasticsearch/packaging/test/DockerTests.java
[multi-arch]: https://www.docker.com/blog/multi-arch-build-and-images-the-simple-way/
[ubi]: https://developers.redhat.com/products/rhel/ubi

[^1]: `podman/buildah` also [supports building multi-platform images](https://github.com/containers/buildah/issues/1590).
