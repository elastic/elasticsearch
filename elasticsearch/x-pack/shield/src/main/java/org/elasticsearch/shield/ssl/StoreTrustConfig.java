/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;

import javax.net.ssl.X509ExtendedTrustManager;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Collections;
import java.util.List;

class StoreTrustConfig extends TrustConfig {

    final String trustStorePath;
    final String trustStorePassword;
    final String trustStoreAlgorithm;

    StoreTrustConfig(boolean includeSystem, boolean reloadEnabled, String trustStorePath, String trustStorePassword,
                     String trustStoreAlgorithm) {
        super(includeSystem, reloadEnabled);
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.trustStoreAlgorithm = trustStoreAlgorithm;
    }

    @Override
    X509ExtendedTrustManager[] nonSystemTrustManagers(@Nullable Environment environment) {
        if (trustStorePath == null) {
            return null;
        }
        try (InputStream in = Files.newInputStream(CertUtils.resolvePath(trustStorePath, environment))) {
            // TODO remove reliance on JKS since we can PKCS12 stores...
            KeyStore trustStore = KeyStore.getInstance("jks");
            assert trustStorePassword != null;
            trustStore.load(in, trustStorePassword.toCharArray());
            return CertUtils.trustManagers(trustStore, trustStoreAlgorithm);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize a TrustManagerFactory", e);
        }
    }

    @Override
    void validate() {
        if (trustStorePath != null) {
            if (trustStorePassword == null) {
                throw new IllegalArgumentException("no truststore password configured");
            }
        }
    }

    @Override
    List<Path> filesToMonitor(@Nullable Environment environment) {
        return Collections.singletonList(CertUtils.resolvePath(trustStorePath, environment));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StoreTrustConfig that = (StoreTrustConfig) o;

        if (trustStorePath != null ? !trustStorePath.equals(that.trustStorePath) : that.trustStorePath != null) return false;
        if (trustStorePassword != null ? !trustStorePassword.equals(that.trustStorePassword) : that.trustStorePassword != null)
            return false;
        return trustStoreAlgorithm != null ? trustStoreAlgorithm.equals(that.trustStoreAlgorithm) : that.trustStoreAlgorithm ==
                null;

    }

    @Override
    public int hashCode() {
        int result = trustStorePath != null ? trustStorePath.hashCode() : 0;
        result = 31 * result + (trustStorePassword != null ? trustStorePassword.hashCode() : 0);
        result = 31 * result + (trustStoreAlgorithm != null ? trustStoreAlgorithm.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "trustStorePath=[" + trustStorePath +
                "], trustStoreAlgorithm=[" + trustStoreAlgorithm +
                "]";
    }
}
