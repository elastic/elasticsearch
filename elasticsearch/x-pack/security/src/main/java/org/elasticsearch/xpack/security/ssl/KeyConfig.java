/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ssl;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.nio.file.Path;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;

abstract class KeyConfig extends TrustConfig {

    KeyConfig(boolean includeSystem) {
        super(includeSystem);
    }

    static final KeyConfig NONE = new KeyConfig(false) {
        @Override
        X509ExtendedKeyManager createKeyManager(@Nullable Environment environment) {
            return null;
        }

        @Override
        X509ExtendedTrustManager nonSystemTrustManager(@Nullable Environment environment) {
            return null;
        }

        @Override
        void validate() {
        }

        @Override
        List<Path> filesToMonitor(@Nullable Environment environment) {
            return Collections.emptyList();
        }

        @Override
        public String toString() {
            return "NONE";
        }
    };

    abstract X509ExtendedKeyManager createKeyManager(@Nullable Environment environment);
}
