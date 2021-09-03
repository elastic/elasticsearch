#!/bin/bash

#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0; you may not use this file except in compliance with the Elastic License
# 2.0.
#

set -e

TSA_URL=http://timestamp.digicert.com

TAB_SDK_REPO=https://github.com/tableau/connector-plugin-sdk
TAB_SDK_TAG="tdvt-2.1.9"

MY_NAME="Packager for Elastic's Tableau connector to Elasticsearch"
MY_FILE=$(basename $0)
MY_WORKSPACE=$(realpath ${PACKAGE_WORKSPACE:-build})
MY_OUT_DIR=$MY_WORKSPACE/distributions
MY_TOP_DIR=$(dirname $(realpath $0))
SRC_DIR=connector

ES_VER_FILE="server/src/main/java/org/elasticsearch/Version.java"
ES_VER_REL_PATH="$MY_TOP_DIR/../../../../../$ES_VER_FILE"

TACO_CLASS=$(xmllint \
    --xpath '//connector-plugin/@class' $MY_TOP_DIR/$SRC_DIR/manifest.xml \
    | awk -F\" '{print $2}')

# SDK generated TACO name
OUT_TACO=$TACO_CLASS.taco

function log()
{
    if [ -t 1 ]; then
        echo -e "$*"
    else
        logger ${MY_FILE##*/} ": " "$*"
    fi
}

function die()
{
    log "ERROR: $*"
    exit 1
}

function package() {
    if [ ! -d $MY_WORKSPACE ]; then
        mkdir -p $MY_WORKSPACE
        touch $MY_WORKSPACE/.workspace
    fi

    pushd . 1>/dev/null
    trap 'popd 1>/dev/null' EXIT
    cd $MY_WORKSPACE

    rm -rfv $MY_WORKSPACE/$SRC_DIR
    cp -rv $MY_TOP_DIR/$SRC_DIR $MY_WORKSPACE

    # patch plugin-version tag in manifest file, filtering out any -SNAPSHOT
    echo \
    -e "cd //connector-plugin/@plugin-version\nset ${TACO_VERSION%-*}\nsave" |\
        xmllint --shell $MY_WORKSPACE/$SRC_DIR/manifest.xml

    # check out TDVT SDK
    SDK_DIR=${TAB_SDK_REPO##*/}
    if [ -d $SDK_DIR ]; then
        cd $SDK_DIR
        git checkout $TAB_SDK_TAG
        cd ..
    else
        git -c advice.detachedHead=false clone --depth 1 \
            --branch $TAB_SDK_TAG $TAB_SDK_REPO
    fi

    # install environment
    cd $SDK_DIR/connector-packager
    python3 -m venv .venv
    source .venv/bin/activate
    python3 setup.py install

    # finally, create the connector
    python -m connector_packager.package $MY_WORKSPACE/$SRC_DIR
    mkdir -p $MY_OUT_DIR
    cp -f packaged-connector/$OUT_TACO $MY_OUT_DIR/$ES_TACO

    log "TACO packaged under: $MY_OUT_DIR/$ES_TACO"
}

function sha() {
    cd $MY_OUT_DIR
    sha512sum $ES_TACO > $ES_TACO.sha512
    echo $(cat $ES_TACO.sha512)
}

# Vars:
#   set: TACO_VERSION
function read_es_version() {
    VER_REGEX="CURRENT = V_[1-9]\{1,2\}_[0-9]\{1,2\}_[0-9]\{1,2\}"
    TACO_VERSION=$(grep -o "$VER_REGEX" $ES_VER_REL_PATH | \
        sed -e 's/V_//' -e 's/CURRENT = //' -e 's/_/./g')
    if [ -z $TACO_VERSION ]; then
        die "failed to read version in source file $ES_VER_REL_PATH"
    fi
}

# Vars:
#   read: CMD_ASM, CMD_SIGN
#   set:  ES_TACO, TACO_VERSION, SIGN_PARAMS
function read_cmd_params() {
    while [ $# -gt 0 ]; do
        key=${1%=*}
        val=${1#*=}
        case $key in
            version)
                if [ ! -z $TACO_VERSION ]; then
                    die "parameter 'version' already set to: $TACO_VERSION"
                fi
                TACO_VERSION=$val
                ;;
            qualifier)
                if [ ! -z $VER_QUALIFIER ]; then
                    die "parameter 'qualifier' already set to: $VER_QUALIFIER"
                fi
                VER_QUALIFIER=$val
                ;;
            keystore)
                if [ ! -z $SIGN_KEYSTORE ]; then
                    die "parameter 'keystore' already set to: $SIGN_KEYSTORE"
                fi
                SIGN_KEYSTORE=$(realpath $val)
                ;;
            alias)
                if [ ! -z $SIGN_ALIAS ]; then
                    die "parameter 'alias' already set to: $SIGN_ALIAS"
                fi
                SIGN_ALIAS=$val
                ;;
            storepassfile)
                if [ ! -z $SIGN_STOREPASSFILE ]; then
                    die "parameter 'storepassfile' already set."
                fi
                SIGN_STOREPASSFILE=$val
                ;;
            keypassfile)
                if [ ! -z $SIGN_KEYPASSFILE ]; then
                    die "parameter 'keypassfile' already set."
                fi
                SIGN_KEYPASSFILE=$val
                ;;
            onepass)
                if [ ! -z $SIGN_ONEPASS ]; then
                    die "parameter 'onepass' already set."
                fi
                SIGN_ONEPASS=$val
                ;;
            *)
                die "Unknown parameter: $key"
                ;;
        esac
        shift
    done

    if [ $CMD_ASM -gt 0 ]; then
        if [ -z $TACO_VERSION ]; then
            if [ ! -f $ES_VER_REL_PATH ]; then
                die "parameter 'version' is required for assembling."
            else
                read_es_version
            fi
        fi
        ES_TACO=$TACO_CLASS-$TACO_VERSION$VER_QUALIFIER.taco
    fi

    if [ $CMD_SIGN -gt 0 ]; then
        if [ -z $SIGN_KEYSTORE ]; then
            die "parameter 'keystore' is mandatory for signing."
        fi

        SIGN_PARAMS="$SIGN_ALIAS" # note: could be empty
        SIGN_PARAMS="$SIGN_PARAMS -keystore $SIGN_KEYSTORE"
        SIGN_PARAMS="$SIGN_PARAMS -tsa $TSA_URL"

        if [ ! -z $SIGN_ONEPASS ]; then
            if [ ! -z $SIGN_STOREPASSFILE ] || [ ! -z $SIGN_KEYPASSFILE ]; then
                die "parameter 'onepass' cannot be used together with " \
                    "'storepassfile' or 'keypassfile'."
            fi
            SIGN_PARAMS="$SIGN_PARAMS -storepass $SIGN_ONEPASS"
            SIGN_PARAMS="$SIGN_PARAMS -keypass $SIGN_ONEPASS"
        else
            if [ ! -z $SIGN_STOREPASSFILE ]; then
                pass=$(cat $SIGN_STOREPASSFILE)
                SIGN_PARAMS="$SIGN_PARAMS -storepass $pass"
            fi
            if [ ! -z $SIGN_KEYPASSFILE ]; then
                pass=$(cat $SIGN_KEYPASSFILE)
                SIGN_PARAMS="$SIGN_PARAMS -keypass $pass"
            fi
        fi
    fi
}

function sign() {
    for taco in $(ls -t $MY_OUT_DIR/*.taco 2>/dev/null); do
        jarsigner $taco $SIGN_PARAMS
        jarsigner -verify -verbose -certs $taco

        log "TACO signed under: $taco"
        break
    done

    if [ -z $taco ]; then
        die "No connector to sign found under: $MY_OUT_DIR/" \
            "\nCall 'assemble' first."
    fi

}

function clean() {
    if [ -f $MY_WORKSPACE/.workspace ]; then
        rm -rf $MY_WORKSPACE
    else
        die "Not a workspace: $MY_WORKSPACE"
    fi
}

function usage() {
    log $MY_NAME
    log
    log "Usage: $MY_FILE <command>"
    log
    log "Commands:"
    log "  asm [version parameters]   : assemble the TACO file"
    log "  sign <signing parameters>  : sign the TACO file"
    log "  pack <ver and sign params> : assemble and sign"
    log "  clean                      : remove the workspace"
    log
    log "Params take the form key=value with following keys:"
    log "  version       : version of the TACO to produce;"
    log "  qualifier     : qualifier to attach to the version;"
    log "  keystore      : path to keystore to use; mandatory;"
    log "  storepassfile : keystore password file; optional;"
    log "  keypassfile   : private key password file; optional;"
    log "  onepass       : password for both the keystore and the "
    log "                  private key; optional;"
    log "  alias         : alias for the keystore entry; optional."
    log
    log "All building is done under workspace: $MY_WORKSPACE"
    log "Can be changed with PACKAGE_WORKSPACE environment variable."
    log
    log "Example:"
    log "  ./$MY_FILE asm version=7.10.1 qualifier=-SNAPSHOT"
    log "  ./$MY_FILE sign keystore=keystore_file alias=alias_name"\
            "storepassfile=password_file"
    log "  ./$MY_FILE pack version=7.10.2 keystore=keystore_file"\
            "onepass=password"
    log
}

if [ $# -lt 1 ]; then
    usage
    die "missing command"
fi

CMD_ASM=0
CMD_SIGN=0

case $1 in
    asm|assemble)
        CMD_ASM=1
        shift
        read_cmd_params $*
        package
        sha
        ;;
    sign)
        CMD_SIGN=1
        shift
        read_cmd_params $*
        sign
        ;;
    pack|package)
        CMD_ASM=1
        CMD_SIGN=1
        shift
        read_cmd_params $*
        package
        sign
        sha
        ;;
    clean)
        clean
        ;;
    *)
        usage
        ;;
esac
