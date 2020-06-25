#!/usr/bin/env bash
#
# Create a basic filesystem that can be used to create a Docker images that
# don't require a full distro.
#
# Originally from:
# 
# https://github.com/moby/moby/blob/master/contrib/mkimage-yum.sh

declare -r BUSYBOX_VERSION="1.31.0"
declare -r TINI_VERSION="0.19.0"

BUSYBOX_PATH=""
TINI_BIN=""
case "$(arch)" in
    aarch64)
        BUSYBOX_PATH="${BUSYBOX_VERSION}-defconfig-multiarch-musl/busybox-armv8l"
        TINI_BIN='tini-arm64'
        ;;
    x86_64)
        BUSYBOX_PATH="${BUSYBOX_VERSION}-i686-uclibc/busybox"
        TINI_BIN='tini-amd64'
        ;;
    *)
        echo >&2
        echo >&2 "Unsupported architecture $(arch)"
        echo >&2
        exit 1
        ;;
esac

set -e

# Start off with an up-to-date system
yum update --setopt=tsflags=nodocs -y

# Create a temporary directory into which we will install files
target="$1"
mkdir $target

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

# Install a minimal set of dependencies, as well as some utilties (zip, pigz)
yum --installroot="$target" --releasever=/ --setopt=tsflags=nodocs \
  --setopt=group_package_types=mandatory -y  \
  --skip-broken \
  install \
    basesystem \
    bash \
    glibc \
    libstdc++ \
    pigz \
    zip \
    zlib

# The tini GitHub page gives instructions for verifying the binary using
# gpg, but the keyservers are slow to return the key and this can fail the
# build. Instead, we check the binary against the published checksum.
curl --retry 8 -S -L -O https://github.com/krallin/tini/releases/download/v0.19.0/${TINI_BIN}
curl --retry 8 -S -L -O https://github.com/krallin/tini/releases/download/v0.19.0/${TINI_BIN}.sha256sum
sha256sum -c ${TINI_BIN}.sha256sum
rm ${TINI_BIN}.sha256sum
mv ${TINI_BIN} "$target"/bin/tini
chmod +x "$target"/bin/tini

# Use busybox instead of installing more RPMs, which can pull in all kinds of
# stuff we don't want. There's no RPM for busybox available for CentOS.
curl --retry 10 -L -o "$target"/bin/busybox "https://busybox.net/downloads/binaries/$BUSYBOX_PATH"
chmod +x "$target"/bin/busybox

set +x
# Add links for all the utilities (except sh, as we have bash, and some others)
for path in $( "$target"/bin/busybox --list-full | grep -v bin/sh | grep -v telnet ); do
  ln "$target"/bin/busybox "$target"/$path
done
set -x

# Curl needs files under here. More importantly, we change Elasticsearch's
# bundled JDK to use /etc/pki/ca-trust/extracted/java/cacerts instead of
# the bundled cacerts.
mkdir -p "$target"/etc && cp -a /etc/pki "$target"/etc/

yum --installroot="$target" -y clean all

rm -rf \
  "$target"/etc/X11 \
  "$target"/etc/centos-release* \
  "$target"/etc/csh* \
  "$target"/etc/groff \
  "$target"/etc/profile* \
  "$target"/etc/skel* \
  "$target"/etc/yum* \
  "$target"/sbin/sln \
  "$target"/usr/bin/rpm \
  "$target"/{usr,var}/games \
  "$target"/usr/lib/dracut \
  "$target"/usr/lib/systemd \
  "$target"/usr/lib/udev \
  "$target"/usr/lib64/X11 \
  "$target"/usr/local \
  "$target"/usr/share/awk \
  "$target"/usr/share/centos-release \
  "$target"/usr/share/cracklib \
  "$target"/usr/share/desktop-directories \
  "$target"/usr/share/gcc-* \
  "$target"/usr/share/i18n \
  "$target"/usr/share/icons \
  "$target"/usr/share/licenses \
  "$target"/usr/share/xsessions \
  "$target"/usr/share/zoneinfo \
  "$target"/usr/share/{awk,man,doc,info,games,gdb,ghostscript,gnome,groff,icons,pixmaps,sounds,backgrounds,themes,X11} \
  "$target"/usr/{{lib,share}/locale,{lib,lib64}/gconv,bin/localedef,sbin/build-locale-archive} \
  "$target"/var/cache/yum \
  "$target"/var/lib/rpm \
  "$target"/var/lib/yum \
  "$target"/var/log/yum.log

# ldconfig
rm -rf "$target"/etc/ld.so.cache "$target"/var/cache/ldconfig
mkdir -p --mode=0755 "$target"/var/cache/ldconfig
