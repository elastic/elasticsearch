#!/usr/bin/env bash
#
# Create a very basic Docker image that can be extended for running Elastic
# Stack images.
#
# Originally from:
# 
# https://github.com/moby/moby/blob/master/contrib/mkimage-yum.sh

set -e

usage() {
    cat <<EOOPTS
$(basename $0) <platform> <output_file>
EOOPTS
    exit 1
}

platform="$1"
output_file="$2"

if [[ -z $platform ]]; then
    usage
fi

if [[ -z "$output_file" ]]; then
    usage
fi

# Create a temporary directory into which we will install files
target=$(mktemp -d --tmpdir $(basename $0).XXXXXX)

set -x

# Create required devices
mkdir -m 755 "$target"/dev
mknod -m 600 "$target"/dev/console c 5 1
mknod -m 600 "$target"/dev/initctl p
mknod -m 666 "$target"/dev/full c 1 7
mknod -m 666 "$target"/dev/null c 1 3
mknod -m 666 "$target"/dev/ptmx c 5 2
mknod -m 666 "$target"/dev/random c 1 8
mknod -m 666 "$target"/dev/tty c 5 0
mknod -m 666 "$target"/dev/tty0 c 4 0
mknod -m 666 "$target"/dev/urandom c 1 9
mknod -m 666 "$target"/dev/zero c 1 5

ARCH="$(arch)"

TINI_URL=""
if [[ "$ARCH" == "x86_64" ]]; then
  TINI_URL="https://github.com/krallin/tini/releases/download/v0.18.0/tini_0.18.0-amd64.rpm"
fi

# Install files. We attempt to install a headless Java distro, and exclude a
# number of unnecessary dependencies. In so doing, we also filter out Java itself,
# but since Elasticsearch ships its own JDK, with its own libs, that isn't a problem
# and in fact is what we want.
#
# Note that we also skip coreutils, as it pulls in all kinds of stuff that
# we don't want.
#
# Note that I haven't yet verified that these dependencies are, in fact, unnecessary.
#
# We also include some utilities that we ship with the image.
#
#    * `pigz` is used for compressing large heaps dumps, and is considerably faster than `gzip` for this task.
#    * `tini` is a tiny but valid init for containers. This is used to cleanly control how ES and any child processes are shut down.
#
yum --installroot="$target" --releasever=/ --setopt=tsflags=nodocs \
  --setopt=group_package_types=mandatory -y  \
  -x copy-jdk-configs -x cups-libs -x javapackages-tools -x alsa-lib -x freetype -x libjpeg -x libjpeg-turbo \
  -x coreutils \
  --skip-broken \
  install \
    java-latest-openjdk-headless \
    bash zip pigz \
    $TINI_URL

if [[ "$ARCH" == "aarch64" ]]; then
  curl --retry 10 -L -o "$target"/bin/tini https://github.com/krallin/tini/releases/download/v0.18.0/tini-static-arm64
  chmod +x "$target"/bin/tini
fi

# Use busybox instead of installing more RPMs, which can pull in all kinds of
# stuff we don't want. Unforunately, there's no RPM for busybox available for CentOS.
BUSYBOX_URL="https://busybox.net/downloads/binaries/1.31.0-i686-uclibc/busybox"
if [[ "$ARCH" == "aarch64" ]]; then
  BUSYBOX_URL="https://www.busybox.net/downloads/binaries/1.31.0-defconfig-multiarch-musl/busybox-armv8l"
fi
curl --retry 10 -L -o "$target"/bin/busybox "$BUSYBOX_URL"
chmod +x "$target"/bin/busybox

set +x
# Add links for all the utilities (except sh, as we have bash)
for path in $( "$target"/bin/busybox --list-full | grep -v bin/sh ); do
  ln "$target"/bin/busybox "$target"/$path
done
set -x

# Copy in our mostly-static curl build
# TODO
# cp /build/curl "$target"/usr/bin/curl

# Curl needs files under here. More importantly, e.g. we change Elasticsearch's
# bundled JDK to use /etc/pki/ca-trust/extracted/java/cacerts instead of
# the bundled cacerts.
tar cf - /etc/pki | (cd "$target" && tar xf -)

yum --installroot="$target" -y clean all

# effectively: febootstrap-minimize --keep-zoneinfo --keep-rpmdb --keep-services "$target".
#  locales
rm -rf "$target"/usr/{{lib,share}/locale,{lib,lib64}/gconv,bin/localedef,sbin/build-locale-archive}
#  docs and man pages
rm -rf "$target"/usr/share/{awk,man,doc,info,games,gdb,ghostscript,gnome,groff,icons}
#  cracklib
rm -rf "$target"/usr/share/cracklib
#  i18n
rm -rf "$target"/usr/share/i18n
#  yum cache
rm -rf "$target"/var/cache/yum
#  sln
rm -rf "$target"/sbin/sln
#  ldconfig
rm -rf "$target"/etc/ld.so.cache "$target"/var/cache/ldconfig
mkdir -p --mode=0755 "$target"/var/cache/ldconfig

# Remove a bunch of other stuff that isn't required
rm -rf \
  "$target"/etc/yum* \
  "$target"/etc/csh* \
  "$target"/etc/centos-release* \
  "$target"/etc/profile* \
  "$target"/etc/skel* \
  "$target"/etc/X11 \
  "$target"/usr/share/awk \
  "$target"/usr/lib/systemd \
  "$target"/usr/lib/dracut \
  "$target"/var/log/yum.log \
  "$target"/var/lib/yum \
  "$target"/var/lib/rpm \
  "$target"/usr/lib/udev \
  "$target"/usr/share/centos-release \
  "$target"/usr/share/desktop-directories \
  "$target"/usr/share/gcc-* \
  "$target"/usr/share/icons \
  "$target"/usr/share/licenses \
  "$target"/usr/share/xsessions \
  "$target"/usr/share/zoneinfo \
  "$target"/etc/groff \
  "$target"/usr/bin/rpm \
  "$target"/usr/bin/tini-static \
  "$target"/usr/local

# Write the base filesystem to stdout. The command changes to $target, so .
# refers to that directory
tar --numeric-owner -c -f "$output_file" -C "$target" .
