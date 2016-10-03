/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;

/**
 * This class represents a trust configuration that corresponds to the default trusted certificates of the JDK
 */
class DefaultJDKTrustConfig extends TrustConfig {

    static final DefaultJDKTrustConfig INSTANCE = new DefaultJDKTrustConfig();

    private DefaultJDKTrustConfig() {
    }

    @Override
    X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
        try {
            return CertUtils.trustManager(null, TrustManagerFactory.getDefaultAlgorithm());
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize a TrustManagerFactory", e);
        }
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
        return o == this;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    /**
     * Merges the default trust configuration with the provided {@link TrustConfig}
     * @param trustConfig the trust configuration to merge with
     * @return a {@link TrustConfig} that represents a combination of both trust configurations
     */
    static TrustConfig merge(TrustConfig trustConfig) {
        return new CombiningTrustConfig(trustConfig);
    }

    /**
     * A trust configuration that is a combination of a trust configuration with the default JDK trust configuration. This trust
     * configuration returns a trust manager verifies certificates against both the default JDK trusted configurations and the specific
     * {@link TrustConfig} provided.
     */
    static class CombiningTrustConfig extends TrustConfig {

        private final TrustConfig trustConfig;

        private CombiningTrustConfig(TrustConfig trustConfig) {
            this.trustConfig = trustConfig;
        }

        @Override
        X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
            X509ExtendedTrustManager trustManager = trustConfig.createTrustManager(environment);
            X509ExtendedTrustManager defaultTrustManager = INSTANCE.createTrustManager(environment);
            if (trustManager == null) {
                return defaultTrustManager;
            }

            X509Certificate[] firstIssuers = trustManager.getAcceptedIssuers();
            X509Certificate[] secondIssuers = defaultTrustManager.getAcceptedIssuers();
            X509Certificate[] acceptedIssuers = new X509Certificate[firstIssuers.length + secondIssuers.length];
            System.arraycopy(firstIssuers, 0, acceptedIssuers, 0, firstIssuers.length);
            System.arraycopy(secondIssuers, 0, acceptedIssuers, firstIssuers.length, secondIssuers.length);
            try {
                return CertUtils.trustManager(acceptedIssuers);
            } catch (Exception e) {
                throw new ElasticsearchException("failed to create trust manager", e);
            }
        }

        @Override
        List<Path> filesToMonitor(@Nullable Environment environment) {
            return trustConfig.filesToMonitor(environment);
        }

        @Override
        public String toString() {
            return "Combining Trust Config{first=[" + trustConfig.toString() + "], second=[" + INSTANCE.toString() + "]}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CombiningTrustConfig)) {
                return false;
            }

            CombiningTrustConfig that = (CombiningTrustConfig) o;
            return trustConfig.equals(that.trustConfig);
        }

        @Override
        public int hashCode() {
            return trustConfig.hashCode();
        }
    }
}
