/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.ssl;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link SslTrustConfig} that trusts all certificates. Used when {@link SslVerificationMode#isCertificateVerificationEnabled()} is
 * {@code false}.
 * This class cannot be used on FIPS-140 JVM as it has its own trust manager implementation.
 */
final class TrustEverythingConfig implements SslTrustConfig {

    static final TrustEverythingConfig TRUST_EVERYTHING = new TrustEverythingConfig();

    private TrustEverythingConfig() {
        // single instances
    }

    /**
     * The {@link X509ExtendedTrustManager} that will trust all certificates.
     * All methods are implemented as a no-op and do not throw exceptions regardless of the certificate presented.
     */
    private static final X509ExtendedTrustManager TRUST_MANAGER = new X509ExtendedTrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) {
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) {
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    };

    @Override
    public Collection<Path> getDependentFiles() {
        return Collections.emptyList();
    }

    @Override
    public X509ExtendedTrustManager createTrustManager() {
        return TRUST_MANAGER;
    }

    @Override
    public String toString() {
        return "trust everything";
    }
}
