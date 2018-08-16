/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import javax.net.ssl.X509ExtendedTrustManager;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The configuration of trust material for SSL usage
 */
abstract class TrustConfig {

    /**
     * Creates a {@link X509ExtendedTrustManager} based on the provided configuration
     * @param environment the environment to resolve files against or null in the case of running in a transport client
     */
    abstract X509ExtendedTrustManager createTrustManager(@Nullable Environment environment);

    abstract Collection<CertificateInfo> certificates(@Nullable Environment environment) throws GeneralSecurityException, IOException;

    /**
     * Returns a list of files that should be monitored for changes
     * @param environment the environment to resolve files against or null in the case of running in a transport client
     */
    abstract List<Path> filesToMonitor(@Nullable Environment environment);

    /**
     * {@inheritDoc}. Declared as abstract to force implementors to provide a custom implementation
     */
    public abstract String toString();

    /**
     * {@inheritDoc}. Declared as abstract to force implementors to provide a custom implementation
     */
    public abstract boolean equals(Object o);

    /**
     * {@inheritDoc}. Declared as abstract to force implementors to provide a custom implementation
     */
    public abstract int hashCode();

    /**
     * A trust configuration that is a combination of a trust configuration with the default JDK trust configuration. This trust
     * configuration returns a trust manager verifies certificates against both the default JDK trusted configurations and the specific
     * {@link TrustConfig} provided.
     */
    static class CombiningTrustConfig extends TrustConfig {

        private final List<TrustConfig> trustConfigs;

        CombiningTrustConfig(List<TrustConfig> trustConfig) {
            this.trustConfigs = Collections.unmodifiableList(trustConfig);
        }

        @Override
        X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
            Optional<TrustConfig> matchAll = trustConfigs.stream().filter(TrustAllConfig.INSTANCE::equals).findAny();
            if (matchAll.isPresent()) {
                return matchAll.get().createTrustManager(environment);
            }

            try {
                return CertParsingUtils.trustManager(trustConfigs.stream()
                        .flatMap((tc) -> Arrays.stream(tc.createTrustManager(environment).getAcceptedIssuers()))
                        .collect(Collectors.toList())
                        .toArray(new X509Certificate[0]));
            } catch (Exception e) {
                throw new ElasticsearchException("failed to create trust manager", e);
            }
        }

        @Override
        Collection<CertificateInfo> certificates(Environment environment) throws GeneralSecurityException, IOException {
            List<CertificateInfo> certificates = new ArrayList<>();
            for (TrustConfig tc : trustConfigs) {
                certificates.addAll(tc.certificates(environment));
            }
            return certificates;
        }

        @Override
        List<Path> filesToMonitor(@Nullable Environment environment) {
            return trustConfigs.stream().flatMap((tc) -> tc.filesToMonitor(environment).stream()).collect(Collectors.toList());
        }

        @Override
        public String toString() {
            return "Combining Trust Config{" + trustConfigs.stream().map(TrustConfig::toString).collect(Collectors.joining(", ")) + "}";
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
            return trustConfigs.equals(that.trustConfigs);
        }

        @Override
        public int hashCode() {
            return trustConfigs.hashCode();
        }
    }
}
