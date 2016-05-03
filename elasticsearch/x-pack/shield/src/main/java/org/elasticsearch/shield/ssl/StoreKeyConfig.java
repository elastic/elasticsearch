/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Collections;
import java.util.List;

class StoreKeyConfig extends KeyConfig {

    final String keyStorePath;
    final String keyStorePassword;
    final String keyStoreAlgorithm;
    final String keyPassword;
    final String trustStoreAlgorithm;

    StoreKeyConfig(boolean includeSystem, boolean reloadEnabled, String keyStorePath, String keyStorePassword, String keyPassword,
                   String keyStoreAlgorithm, String trustStoreAlgorithm) {
        super(includeSystem, reloadEnabled);
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.keyPassword = keyPassword;
        this.keyStoreAlgorithm = keyStoreAlgorithm;
        this.trustStoreAlgorithm = trustStoreAlgorithm;
    }

    @Override
    X509ExtendedKeyManager[] loadKeyManagers(@Nullable Environment environment) {
        try (InputStream in = Files.newInputStream(CertUtils.resolvePath(keyStorePath, environment))) {
            // TODO remove reliance on JKS since we can PKCS12 stores...
            KeyStore ks = KeyStore.getInstance("jks");
            assert keyStorePassword != null;
            ks.load(in, keyStorePassword.toCharArray());
            return CertUtils.keyManagers(ks, keyPassword.toCharArray(), keyStoreAlgorithm);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize a KeyManagerFactory", e);
        }
    }

    @Override
    X509ExtendedTrustManager[] nonSystemTrustManagers(@Nullable Environment environment) {
        try (InputStream in = Files.newInputStream(CertUtils.resolvePath(keyStorePath, environment))) {
            // TODO remove reliance on JKS since we can PKCS12 stores...
            KeyStore ks = KeyStore.getInstance("jks");
            assert keyStorePassword != null;
            ks.load(in, keyStorePassword.toCharArray());

            return CertUtils.trustManagers(ks, trustStoreAlgorithm);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize a TrustManagerFactory", e);
        }
    }

    @Override
    void validate() {
        if (keyStorePath == null) {
            throw new IllegalArgumentException("keystore path must be specified");
        } else if (keyStorePassword == null) {
            throw new IllegalArgumentException("no keystore password configured");
        }
    }

    @Override
    List<Path> filesToMonitor(@Nullable Environment environment) {
        return Collections.singletonList(CertUtils.resolvePath(keyStorePath, environment));
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
