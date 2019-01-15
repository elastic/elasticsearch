/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * This class represents a trust configuration that corresponds to the default trusted certificates of the JDK
 */
class DefaultJDKTrustConfig extends TrustConfig {

    private SecureString trustStorePassword;

    /**
     * @param trustStorePassword the password for the default jdk truststore defined either as a system property or in the Elasticsearch
     *                           configuration. It applies only when PKCS#11 tokens are user, is null otherwise
     */
    DefaultJDKTrustConfig(@Nullable SecureString trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    @Override
    X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
        try {
            return CertParsingUtils.trustManager(getSystemTrustStore(), TrustManagerFactory.getDefaultAlgorithm());
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize a TrustManagerFactory", e);
        }
    }

    @Override
    /**
     * We don't return the list of JDK certificates here, because they are not managed by Elasticsearch, and the purpose
     * of this method is to obtain information about certificate (files/stores) that X-Pack directly manages.
     */
    Collection<CertificateInfo> certificates(Environment environment) throws GeneralSecurityException, IOException {
        return Collections.emptyList();
    }

    @Override
    List<Path> filesToMonitor(@Nullable Environment environment) {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return "JDK trusted certs";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultJDKTrustConfig that = (DefaultJDKTrustConfig) o;
        return Objects.equals(trustStorePassword, that.trustStorePassword);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trustStorePassword);
    }

    /**
     * Merges the default trust configuration with the provided {@link TrustConfig}
     * @param trustConfig the trust configuration to merge with
     * @param trustStorePassword the password for the default jdk truststore. It applies only to PKCS#11 tokens
     * @return a {@link TrustConfig} that represents a combination of both trust configurations
     */
    static TrustConfig merge(TrustConfig trustConfig, SecureString trustStorePassword) {
        if (trustConfig == null) {
            return new DefaultJDKTrustConfig(trustStorePassword);
        } else {
            return new CombiningTrustConfig(Arrays.asList(new DefaultJDKTrustConfig(trustStorePassword), trustConfig));
        }
    }

    /**
     * When a PKCS#11 token is used as the system default keystore/truststore, we need to pass the keystore
     * password when loading, even for reading certificates only ( as opposed to i.e. JKS keystores where
     * we only need to pass the password for reading Private Key entries ).
     *
     * @return the KeyStore used as truststore for PKCS#11 initialized with the password, null otherwise
     */
    private KeyStore getSystemTrustStore() throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        if (System.getProperty("javax.net.ssl.trustStoreType", "").equalsIgnoreCase("PKCS11")
            && trustStorePassword != null) {
            KeyStore keyStore = KeyStore.getInstance("PKCS11");
            keyStore.load(null, trustStorePassword.getChars());
            return keyStore;
        }
        return null;
    }
}
