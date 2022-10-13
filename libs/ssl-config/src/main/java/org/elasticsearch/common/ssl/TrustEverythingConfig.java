/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import java.net.Socket;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * A {@link SslTrustConfig} that trusts all certificates. Used when {@link SslVerificationMode#isCertificateVerificationEnabled()} is
 * {@code false}.
 * This class cannot be used on FIPS-140 JVM as it has its own trust manager implementation.
 */
public final class TrustEverythingConfig implements SslTrustConfig {

    public static final TrustEverythingConfig TRUST_EVERYTHING = new TrustEverythingConfig();

    private TrustEverythingConfig() {
        // single instances
    }

    /**
     * The {@link X509ExtendedTrustManager} that will trust all certificates.
     * All methods are implemented as a no-op and do not throw exceptions regardless of the certificate presented.
     */
    private static final X509ExtendedTrustManager TRUST_MANAGER = new X509ExtendedTrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) {}

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) {}

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) {}

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) {}

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) {}

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) {}

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    };

    @Override
    public Collection<Path> getDependentFiles() {
        return List.of();
    }

    @Override
    public Collection<? extends StoredCertificate> getConfiguredCertificates() {
        return List.of();
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
