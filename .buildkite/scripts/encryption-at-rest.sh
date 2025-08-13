#!/bin/bash

set -euo pipefail

#!/bin/bash
# Configure a dm-crypt volume backed by a file
set -e
dd if=/dev/zero of=dm-crypt.img bs=1 count=0 seek=80GB
dd if=/dev/urandom of=key.secret bs=2k count=1
LOOP=$(losetup -f)
sudo losetup $LOOP dm-crypt.img
sudo cryptsetup luksFormat -q --key-file key.secret "$LOOP"
sudo cryptsetup open --key-file key.secret "$LOOP" secret --verbose
sudo mkfs.ext2 /dev/mapper/secret
sudo mkdir /mnt/secret
# Change /mnt/secret with care (at least a test uses this path to detect when encryption at rest is used)
sudo mount /dev/mapper/secret /mnt/secret
sudo chown -R buildkite-agent /mnt/secret
cp -r "$WORKSPACE" /mnt/secret
cd /mnt/secret/$(basename "$WORKSPACE")
touch .output.log
rm -Rf "$WORKSPACE"
ln -s "$PWD" "$WORKSPACE"

.ci/scripts/run-gradle.sh -Dbwc.checkout.align=true functionalTests
