/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

abstract class KeyConfig extends TrustConfig {

    static final KeyConfig NONE = new KeyConfig() {
        @Override
        X509ExtendedKeyManager createKeyManager(@Nullable Environment environment) {
            return null;
        }

        @Override
        X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
            return null;
        }

        @Override
        Collection<CertificateInfo> certificates(Environment environment) {
            return Collections.emptyList();
        }

        @Override
        List<Path> filesToMonitor(@Nullable Environment environment) {
            return Collections.emptyList();
        }

        @Override
        public String toString() {
            return "NONE";
        }

        @Override
        public boolean equals(Object o) {
            return o == this;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }

        @Override
        List<PrivateKey> privateKeys(@Nullable Environment environment) {
            return Collections.emptyList();
        }
    };

    abstract X509ExtendedKeyManager createKeyManager(@Nullable Environment environment);

    abstract List<PrivateKey> privateKeys(@Nullable Environment environment);

}
