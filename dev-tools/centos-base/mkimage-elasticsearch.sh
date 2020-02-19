#!/usr/bin/env bash
#
# Create a very basic Docker image for running Elasticsearch, using files from CentOS
#
# Originally from:
# 
# https://github.com/moby/moby/blob/master/contrib/mkimage-yum.sh

set -e

usage() {
    cat <<EOOPTS
$(basename $0) [OPTIONS] <name>
OPTIONS:
  -y <yumconf>     The path to the yum config to install packages from. The
                   default is /etc/yum.conf for Centos/RHEL and /etc/dnf/dnf.conf for Fedora
  -t <tag>         Specify Tag information.
                   default is reffered at /etc/{redhat,system}-release
EOOPTS
    exit 1
}

# option defaults
yum_config=/etc/yum.conf
if [ -f /etc/dnf/dnf.conf ] && command -v dnf &> /dev/null; then
  yum_config=/etc/dnf/dnf.conf
  alias yum=dnf
fi
version=
while getopts ":y:t:h" opt; do
    case $opt in
        y)
            yum_config=$OPTARG
            ;;
        h)
            usage
            ;;
        t)
            version="$OPTARG"
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            usage
            ;;
    esac
done
shift $((OPTIND - 1))
name=$1

if [[ -z $name ]]; then
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

# Install files. We attempt to install a headless Java distro, and exclude a
# number of unnecessary dependencies. In so doing, we also filter out Java itself,
# but since Elasticsearch ships its own JDK, with its own libs, that isn't a problem
# and in fact is what we want.
#
# Note that I haven't yet verified that these dependencies are, in fact, unnecessary.
#
# We also include some utilities that we ship with the image.
#
#    * `pigz` is used for compressing large heaps dumps, and is considerably faster than `gzip` for this task.
#    * `tini` is a tiny but valid init for containers. This is used to cleanly control how ES and any child processes are shut down.
#
yum -c "$yum_config" --installroot="$target" --releasever=/ --setopt=tsflags=nodocs \
  --setopt=group_package_types=mandatory -y  \
  -x copy-jdk-configs -x cups-libs -x javapackages-tools -x alsa-lib -x freetype -x libjpeg -x libjpeg-turbo  \
  --skip-broken \
  install \
    java-latest-openjdk-headless \
    bash zip pigz \
    https://github.com/krallin/tini/releases/download/v0.18.0/tini_0.18.0-amd64.rpm

# Use busybox instead of installing more RPMs, which can pull in all kinds of
# stuff we don't want. Unforunately, there's no RPM for busybox available for CentOS.
curl -o "$target"/bin/busybox https://busybox.net/downloads/binaries/1.31.0-i686-uclibc/busybox
chmod +x "$target"/bin/busybox

set +x
# Add links for all the utilities (except sh, as we have bash)
for path in $( "$target"/bin/busybox --list-full | grep -v bin/sh ); do
  ln "$target"/bin/busybox "$target"/$path
done
set -x

# This comes from ca-certificates, but this is the only file we want
CA_CERTS=/etc/pki/ca-trust/extracted/java/cacerts
mkdir -p $(dirname "$target"/$CA_CERTS)
cp $CA_CERTS "$target"/$CA_CERTS

yum -c "$yum_config" --installroot="$target" -y clean all

# effectively: febootstrap-minimize --keep-zoneinfo --keep-rpmdb --keep-services "$target".
#  locales
rm -rf "$target"/usr/{{lib,share}/locale,{lib,lib64}/gconv,bin/localedef,sbin/build-locale-archive}
#  docs and man pages
rm -rf "$target"/usr/share/{awk,man,doc,info,gnome/help,groff}
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
  "$target"/etc/profile* \
  "$target"/etc/skel* \
  "$target"/usr/share/awk \
  "$target"/usr/lib/systemd \
  "$target"/usr/lib/dracut \
  "$target"/var/log/yum.log \
  "$target"/var/lib/yum \
  "$target"/var/lib/rpm \
  "$target"/usr/lib/udev \
  "$target"/etc/groff \
  "$target"/usr/bin/rpm \
  "$target"/usr/bin/tini-static

if [ -z "$version" ]; then
    for file in "$target"/etc/{redhat,system}-release; do
        if [ -r "$file" ]; then
            version="$(sed 's/^[^0-9\]*\([0-9.]\+\).*$/\1/' "$file")"
            break
        fi
    done
fi

if [ -z "$version" ]; then
    echo >&2 "warning: cannot autodetect OS version, using '$name' as tag"
    version=$name
fi

# Import the base filesystem into Docker
tar --numeric-owner -c -C "$target" . | docker import - $name:$version

# Remove the temp dir
rm -rf "$target"
