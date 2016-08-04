/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

abstract class TrustConfig {

    protected final boolean includeSystem;

    TrustConfig(boolean includeSystem) {
        this.includeSystem = includeSystem;
    }

    final X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
        X509ExtendedTrustManager trustManager = nonSystemTrustManager(environment);
        if (includeSystem) {
            trustManager = mergeWithSystem(trustManager);
        } else if (trustManager == null) {
            return null;
        }
        return trustManager;
    }

    abstract X509ExtendedTrustManager nonSystemTrustManager(@Nullable Environment environment);

    abstract void validate();

    abstract List<Path> filesToMonitor(@Nullable Environment environment);

    public abstract String toString();

    private X509ExtendedTrustManager mergeWithSystem(X509ExtendedTrustManager nonSystemTrustManager) {
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((KeyStore) null);
            TrustManager[] systemTrustManagers = tmf.getTrustManagers();
            X509ExtendedTrustManager system = findFirstX509ExtendedTrustManager(systemTrustManagers);
            if (nonSystemTrustManager == null) {
                return system;
            }

            return new CombiningX509TrustManager(nonSystemTrustManager, system);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize a trust managers", e);
        }
    }

    private static X509ExtendedTrustManager findFirstX509ExtendedTrustManager(TrustManager[] trustManagers) {
        X509ExtendedTrustManager x509TrustManager = null;
        for (TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509ExtendedTrustManager) {
                // first one wins like in the JDK
                x509TrustManager = (X509ExtendedTrustManager) trustManager;
                break;
            }
        }
        if (x509TrustManager == null) {
            throw new IllegalArgumentException("did not find a X509ExtendedTrustManager");
        }
        return x509TrustManager;
    }

    private static class CombiningX509TrustManager extends X509ExtendedTrustManager {

        private final X509ExtendedTrustManager first;
        private final X509ExtendedTrustManager second;

        private final X509Certificate[] acceptedIssuers;

        CombiningX509TrustManager(X509ExtendedTrustManager first, X509ExtendedTrustManager second) {
            this.first = first;
            this.second = second;
            X509Certificate[] firstIssuers = first.getAcceptedIssuers();
            X509Certificate[] secondIssuers = second.getAcceptedIssuers();
            this.acceptedIssuers = new X509Certificate[firstIssuers.length + secondIssuers.length];
            System.arraycopy(firstIssuers, 0, acceptedIssuers, 0, firstIssuers.length);
            System.arraycopy(secondIssuers, 0, acceptedIssuers, firstIssuers.length, secondIssuers.length);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
            try {
                first.checkClientTrusted(x509Certificates, s, socket);
            } catch (CertificateException e) {
                second.checkClientTrusted(x509Certificates, s, socket);
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
            try {
                first.checkServerTrusted(x509Certificates, s, socket);
            } catch (CertificateException e) {
                second.checkServerTrusted(x509Certificates, s, socket);
            }
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
            try {
                first.checkClientTrusted(x509Certificates, s, sslEngine);
            } catch (CertificateException e) {
                second.checkClientTrusted(x509Certificates, s, sslEngine);
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
            try {
                first.checkServerTrusted(x509Certificates, s, sslEngine);
            } catch (CertificateException e) {
                second.checkServerTrusted(x509Certificates, s, sslEngine);
            }
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            try {
                first.checkClientTrusted(x509Certificates, s);
            } catch (CertificateException e) {
                second.checkClientTrusted(x509Certificates, s);
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            try {
                first.checkServerTrusted(x509Certificates, s);
            } catch (CertificateException e) {
                second.checkServerTrusted(x509Certificates, s);
            }
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return acceptedIssuers;
        }
    }
}
