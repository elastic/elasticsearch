#!/bin/bash

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.

# Checks that X-Pack files are correctly installed
verify_xpack_installation() {
    local name="x-pack"
    local user="$ESPLUGIN_COMMAND_USER"
    local group="$ESPLUGIN_COMMAND_USER"

    # Verify binary files
    # nocommit: already verified by "main" package verification
    #assert_file "$ESHOME/bin" d $user $group 755
    local binaryFiles=(
        'elasticsearch-certgen'
        'elasticsearch-certgen.bat'
        'elasticsearch-certutil'
        'elasticsearch-certutil.bat'
        'elasticsearch-croneval'
        'elasticsearch-croneval.bat'
        'elasticsearch-migrate'
        'elasticsearch-migrate.bat'
        'elasticsearch-saml-metadata'
        'elasticsearch-saml-metadata.bat'	
        'elasticsearch-setup-passwords'
        'elasticsearch-setup-passwords.bat'
        'elasticsearch-sql-cli'
        'elasticsearch-sql-cli.bat'
        "elasticsearch-sql-cli-$(cat version).jar" # This jar is executable so we pitch it in bin so folks will find it
        'elasticsearch-syskeygen'
        'elasticsearch-syskeygen.bat'
        'elasticsearch-users'
        'elasticsearch-users.bat'
        'x-pack-env'
        'x-pack-env.bat'
        'x-pack-security-env'
        'x-pack-security-env.bat'
        'x-pack-watcher-env'
        'x-pack-watcher-env.bat'
    )

    local binaryFilesCount=5 # start with oss distro number
    for binaryFile in ${binaryFiles[@]}; do
        echo "checking for bin file ${binaryFile}"
        assert_file "$ESHOME/bin/${binaryFile}" f $user $group 755
        binaryFilesCount=$(( binaryFilesCount + 1 ))
    done
    ls "$ESHOME/bin/"
    # nocommit: decide whether to check the files added by the distribution, not part of xpack...
    #assert_number_of_files "$ESHOME/bin/" $binaryFilesCount

    # Verify config files
    # nocommit: already verified by "main" package verification
    #assert_file "$ESCONFIG" d $user elasticsearch 755
    local configFiles=(
        'users'
        'users_roles'
        'roles.yml'
        'role_mapping.yml'
        'log4j2.properties'
    )

    local configFilesCount=2 # start with ES files, excluding log4j2
    for configFile in ${configFiles[@]}; do
        assert_file "$ESCONFIG/${configFile}" f $user elasticsearch 660
        configFilesCount=$(( configFilesCount + 1 ))
    done
    # nocommit: decide whether to check the files added by the distribution, not part of xpack...
    #assert_number_of_files "$ESCONFIG/" $configFilesCount
}

assert_number_of_files() {
    local directory=$1
    local expected=$2

    local count=$(ls "$directory" | wc -l)
    [ "$count" -eq "$expected" ] || {
        echo "Expected $expected files in $directory but found: $count"
        false
    }
}

generate_trial_license() {
    sudo -E -u $ESPLUGIN_COMMAND_USER sh <<"NODE_SETTINGS"
cat >> $ESCONFIG/elasticsearch.yml <<- EOF
xpack.license.self_generated.type: trial
xpack.security.enabled: true
EOF
NODE_SETTINGS
}

wait_for_xpack() {
    local host=${1:-localhost}
    local port=${2:-9200}

    local waitFor
    for i in {1..60}; do
        waitFor=$(echo "GET / HTTP/1.0" > /dev/tcp/$host/$port; echo $?)
        [ "$waitFor" -eq "0" ] && break || sleep 1;
    done

    [ "$waitFor" -eq "0" ]
}
