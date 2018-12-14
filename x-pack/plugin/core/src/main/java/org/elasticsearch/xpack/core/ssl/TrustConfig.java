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

import javax.net.ssl.X509ExtendedTrustManager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
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
     * Loads and returns the appropriate {@link KeyStore} for the given configuration. The KeyStore can be backed by a file
     * in any format that the Security Provider might support, or a cryptographic software or hardware token in the case
     * of a PKCS#11 Provider.
     *
     * @param environment   the environment to resolve files against or null in the case of running in a transport client
     * @param storePath     the path to the {@link KeyStore} to load, or null if a PKCS11 token is configured as the keystore/truststore
     *                      of the JVM
     * @param storeType     the type of the {@link KeyStore}
     * @param storePassword the password to be used for decrypting the {@link KeyStore}
     * @return the loaded KeyStore to be used as a keystore or a truststore
     * @throws KeyStoreException        if an instance of the specified type cannot be loaded
     * @throws CertificateException     if any of the certificates in the keystore could not be loaded
     * @throws NoSuchAlgorithmException if the algorithm used to check the integrity of the keystore cannot be found
     * @throws IOException              if there is an I/O issue with the KeyStore data or the password is incorrect
     */
    KeyStore getStore(@Nullable Environment environment, @Nullable String storePath, String storeType, SecureString storePassword)
        throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        if (null != storePath) {
            try (InputStream in = Files.newInputStream(CertParsingUtils.resolvePath(storePath, environment))) {
                KeyStore ks = KeyStore.getInstance(storeType);
                ks.load(in, storePassword.getChars());
                return ks;
            }
        } else if (storeType.equalsIgnoreCase("pkcs11")) {
            KeyStore ks = KeyStore.getInstance(storeType);
            ks.load(null, storePassword.getChars());
            return ks;
        }
        throw new IllegalArgumentException("keystore.path or truststore.path can only be empty when using a PKCS#11 token");
    }

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
