/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.support.SecuredString;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;

/**
 * A key configuration that is backed by a {@link KeyStore}
 */
class StoreKeyConfig extends KeyConfig {

    final String keyStorePath;
    final String keyStorePassword;
    final String keyStoreAlgorithm;
    final String keyPassword;
    final String trustStoreAlgorithm;

    /**
     * Creates a new configuration that can be used to load key and trust material from a {@link KeyStore}
     * @param keyStorePath the path to the keystore file
     * @param keyStorePassword the password for the keystore
     * @param keyPassword the password for the private key in the keystore
     * @param keyStoreAlgorithm the algorithm for the keystore
     * @param trustStoreAlgorithm the algorithm to use when loading as a truststore
     */
    StoreKeyConfig(String keyStorePath, String keyStorePassword, String keyPassword, String keyStoreAlgorithm,
                   String trustStoreAlgorithm) {
        this.keyStorePath = Objects.requireNonNull(keyStorePath, "keystore path must be specified");
        this.keyStorePassword = Objects.requireNonNull(keyStorePassword, "keystore password must be specified");
        this.keyPassword = keyPassword;
        this.keyStoreAlgorithm = keyStoreAlgorithm;
        this.trustStoreAlgorithm = trustStoreAlgorithm;
    }

    @Override
    X509ExtendedKeyManager createKeyManager(@Nullable Environment environment) {
        try {
            KeyStore ks = getKeyStore(environment);
            checkKeyStore(ks);
            try (SecuredString keyPasswordSecuredString = new SecuredString(keyPassword.toCharArray())) {
                return CertUtils.keyManager(ks, keyPasswordSecuredString.internalChars(), keyStoreAlgorithm);
            }
        } catch (IOException | CertificateException | NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
            throw new ElasticsearchException("failed to initialize a KeyManagerFactory", e);
        }
    }

    @Override
    X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
        try {
            return CertUtils.trustManager(keyStorePath, keyStorePassword, trustStoreAlgorithm, environment);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize a TrustManagerFactory", e);
        }
    }

    @Override
    List<Path> filesToMonitor(@Nullable Environment environment) {
        return Collections.singletonList(CertUtils.resolvePath(keyStorePath, environment));
    }

    @Override
    List<PrivateKey> privateKeys(@Nullable Environment environment) {
        try {
            KeyStore keyStore = getKeyStore(environment);
            try (SecuredString keyPasswordSecuredString = new SecuredString(keyPassword.toCharArray())) {
                List<PrivateKey> privateKeys = new ArrayList<>();
                for (Enumeration<String> e = keyStore.aliases(); e.hasMoreElements(); ) {
                    final String alias = e.nextElement();
                    if (keyStore.isKeyEntry(alias)) {
                        Key key = keyStore.getKey(alias, keyPasswordSecuredString.internalChars());
                        if (key instanceof PrivateKey) {
                            privateKeys.add((PrivateKey) key);
                        }
                    }
                }
                return privateKeys;
            }
        } catch (Exception e) {
            throw new ElasticsearchException("failed to list keys", e);
        }
    }

    private KeyStore getKeyStore(@Nullable Environment environment)
                                throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        try (InputStream in = Files.newInputStream(CertUtils.resolvePath(keyStorePath, environment))) {
            // TODO remove reliance on JKS since we can use PKCS12 stores in JDK8+...
            KeyStore ks = KeyStore.getInstance("jks");
            if (keyStorePassword == null) {
                throw new IllegalArgumentException("keystore password may not be null");
            }
            try (SecuredString keyStorePasswordSecuredString  = new SecuredString(keyStorePassword.toCharArray())) {
                ks.load(in, keyStorePasswordSecuredString.internalChars());
            }
            return ks;
        }
    }

    private void checkKeyStore(KeyStore keyStore) throws KeyStoreException {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                return;
            }
        }
        throw new IllegalArgumentException("the keystore [" + keyStorePath + "] does not contain a private key entry");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StoreKeyConfig that = (StoreKeyConfig) o;

        if (keyStorePath != null ? !keyStorePath.equals(that.keyStorePath) : that.keyStorePath != null) return false;
        if (keyStorePassword != null ? !keyStorePassword.equals(that.keyStorePassword) : that.keyStorePassword != null)
            return false;
        if (keyStoreAlgorithm != null ? !keyStoreAlgorithm.equals(that.keyStoreAlgorithm) : that.keyStoreAlgorithm != null)
            return false;
        if (keyPassword != null ? !keyPassword.equals(that.keyPassword) : that.keyPassword != null) return false;
        return trustStoreAlgorithm != null ? trustStoreAlgorithm.equals(that.trustStoreAlgorithm) : that.trustStoreAlgorithm == null;
    }

    @Override
    public int hashCode() {
        int result = keyStorePath != null ? keyStorePath.hashCode() : 0;
        result = 31 * result + (keyStorePassword != null ? keyStorePassword.hashCode() : 0);
        result = 31 * result + (keyStoreAlgorithm != null ? keyStoreAlgorithm.hashCode() : 0);
        result = 31 * result + (keyPassword != null ? keyPassword.hashCode() : 0);
        result = 31 * result + (trustStoreAlgorithm != null ? trustStoreAlgorithm.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "keyStorePath=[" + keyStorePath +
                "], keyStoreAlgorithm=[" + keyStoreAlgorithm +
                "], trustStoreAlgorithm=[" + trustStoreAlgorithm +
                "]";
    }
}
