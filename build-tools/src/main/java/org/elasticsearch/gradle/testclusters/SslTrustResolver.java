/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.testclusters;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

class SslTrustResolver {
    private Set<File> certificateAuthorities;
    private File trustStoreFile;
    private String trustStorePassword;
    private File serverCertificate;
    private File serverKeyStoreFile;
    private String serverKeyStorePassword;

    public void setCertificateAuthorities(File... certificateAuthorities) {
        this.certificateAuthorities = new HashSet<>(Arrays.asList(certificateAuthorities));
    }

    public void setTrustStoreFile(File trustStoreFile) {
        this.trustStoreFile = trustStoreFile;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public void setServerCertificate(File serverCertificate) {
        this.serverCertificate = serverCertificate;
    }

    public void setServerKeystoreFile(File keyStoreFile) {
        this.serverKeyStoreFile = keyStoreFile;
    }

    public void setServerKeystorePassword(String keyStorePassword) {
        this.serverKeyStorePassword = keyStorePassword;
    }

    public SSLContext getSslContext() throws GeneralSecurityException, IOException {
        final TrustManager[] trustManagers = buildTrustManagers();
        if (trustManagers != null) {
            return createSslContext(trustManagers);
        } else {
            return null;
        }
    }

    TrustManager[] buildTrustManagers() throws GeneralSecurityException, IOException {
        var configurationCount = Stream.of(
            this.certificateAuthorities,
            this.trustStoreFile,
            this.serverCertificate,
            this.serverKeyStoreFile
        ).filter(Objects::nonNull).count();
        if (configurationCount == 0) {
            return null;
        } else if (configurationCount > 1) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "Cannot specify more than one trust method (CA=%s, trustStore=%s, serverCert=%s, serverKeyStore=%s)",
                    certificateAuthorities,
                    trustStoreFile,
                    serverCertificate,
                    serverKeyStoreFile
                )
            );
        }
        if (this.certificateAuthorities != null) {
            return getTrustManagers(buildTrustStoreFromCA(certificateAuthorities));
        } else if (this.trustStoreFile != null) {
            return getTrustManagers(readKeyStoreFromFile(trustStoreFile, trustStorePassword));
        } else if (this.serverCertificate != null) {
            return buildTrustManagerFromLeafCertificates(head(readCertificates(serverCertificate)));
        } else if (this.serverKeyStoreFile != null) {
            return buildTrustManagerFromLeafCertificates(readCertificatesFromKeystore(serverKeyStoreFile, serverKeyStorePassword));
        } else {
            // Cannot get here unless the code gets out of sync with the 'configurationCount == 0' check above
            throw new IllegalStateException("Expected to configure trust, but all configuration values are null");
        }
    }

    private SSLContext createSslContext(TrustManager[] trustManagers) throws GeneralSecurityException {
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(new KeyManager[0], trustManagers, new SecureRandom());
        return sslContext;
    }

    private TrustManager[] getTrustManagers(KeyStore trustStore) throws GeneralSecurityException {
        checkForTrustEntry(trustStore);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        return tmf.getTrustManagers();
    }

    private void checkForTrustEntry(KeyStore trustStore) throws KeyStoreException {
        Enumeration<String> enumeration = trustStore.aliases();
        while (enumeration.hasMoreElements()) {
            if (trustStore.isCertificateEntry(enumeration.nextElement())) {
                // found trusted cert entry
                return;
            }
        }
        throw new IllegalStateException("Trust-store does not contain any trusted certificate entries");
    }

    private static KeyStore buildTrustStoreFromCA(Set<File> files) throws GeneralSecurityException, IOException {
        final KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null, null);
        int counter = 0;
        for (File ca : files) {
            for (Certificate certificate : readCertificates(ca)) {
                store.setCertificateEntry("cert-" + counter, certificate);
                counter++;
            }
        }
        return store;
    }

    private static TrustManager[] buildTrustManagerFromLeafCertificates(Collection<? extends Certificate> certificates) {
        final Set<X509Certificate> trusted = certificates.stream()
            .filter(X509Certificate.class::isInstance)
            .map(X509Certificate.class::cast)
            .collect(Collectors.toUnmodifiableSet());

        var trustManager = new X509TrustManager() {
            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                final X509Certificate leaf = chain[0];
                if (trusted.contains(leaf) == false) {
                    throw new CertificateException("Untrusted leaf certificate: " + leaf.getSubjectX500Principal());
                }
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                // This doesn't apply when trusting leaf certs, and is only really needed for server trust managers anyways
                return new X509Certificate[0];
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                throw new CertificateException("This trust manager is for client use only and cannot trust other clients");
            }

        };
        return new TrustManager[] { trustManager };
    }

    private static Collection<Certificate> readCertificatesFromKeystore(File file, String password) throws GeneralSecurityException,
        IOException {
        var keyStore = readKeyStoreFromFile(file, password);
        final Set<Certificate> certificates = new HashSet<>(keyStore.size());
        var enumeration = keyStore.aliases();
        while (enumeration.hasMoreElements()) {
            var alias = enumeration.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                certificates.add(keyStore.getCertificate(alias));
            }
        }
        return certificates;
    }

    private static KeyStore readKeyStoreFromFile(File file, String password) throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance(file.getName().endsWith(".jks") ? "JKS" : "PKCS12");
        try (InputStream input = new FileInputStream(file)) {
            keyStore.load(input, password == null ? null : password.toCharArray());
        }
        return keyStore;
    }

    private static Collection<? extends Certificate> readCertificates(File pemFile) throws GeneralSecurityException, IOException {
        final CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        try (InputStream input = new FileInputStream(pemFile)) {
            return certFactory.generateCertificates(input);
        }
    }

    private Collection<? extends Certificate> head(Collection<? extends Certificate> certificates) {
        if (certificates.isEmpty()) {
            return certificates;
        } else {
            return List.of(certificates.iterator().next());
        }
    }
}
