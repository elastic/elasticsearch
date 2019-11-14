#!/bin/bash
set +x
license_path=/var/lib/jenkins/.license
mkdir -p $license_path
vault-read-as-role $VAULT_ROLE_ID secret/release/license 'pubkey' | base64 --decode > $license_path/license.key
