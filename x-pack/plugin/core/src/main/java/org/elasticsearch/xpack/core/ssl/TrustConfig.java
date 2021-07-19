/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessControlException;
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
     * @param environment the environment to resolve files against
     */
    abstract X509ExtendedTrustManager createTrustManager(Environment environment);

    abstract Collection<CertificateInfo> certificates(Environment environment) throws GeneralSecurityException, IOException;

    /**
     * Returns a list of files that should be monitored for changes
     * @param environment the environment to resolve files against
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
     * @deprecated Use {@link #getStore(Path, String, SecureString)} instead
     */
    @Deprecated
    KeyStore getStore(Environment environment, @Nullable String storePath, String storeType, SecureString storePassword)
        throws GeneralSecurityException, IOException {
        return getStore(CertParsingUtils.resolvePath(storePath, environment), storeType, storePassword);
    }

    /**
     * Loads and returns the appropriate {@link KeyStore} for the given configuration. The KeyStore can be backed by a file
     * in any format that the Security Provider might support.
     *
     * @param storePath     the path to the {@link KeyStore} to load
     * @param storeType     the type of the {@link KeyStore}
     * @param storePassword the password to be used for decrypting the {@link KeyStore}
     * @return the loaded KeyStore to be used as a keystore or a truststore
     * @throws KeyStoreException        if an instance of the specified type cannot be loaded
     * @throws CertificateException     if any of the certificates in the keystore could not be loaded
     * @throws NoSuchAlgorithmException if the algorithm used to check the integrity of the keystore cannot be found
     * @throws IOException              if there is an I/O issue with the KeyStore data or the password is incorrect
     */
    KeyStore getStore(Path storePath, String storeType, SecureString storePassword) throws IOException, GeneralSecurityException {
        if (storeType.equalsIgnoreCase("pkcs11")) {
            throw new IllegalArgumentException("PKCS#11 keystores are no longer supported by Elasticsearch");
        }
        if (storePath == null) {
            throw new IllegalArgumentException("keystore.path or truststore.path cannot be empty");
        }
        try (InputStream in = Files.newInputStream(storePath)) {
            KeyStore ks = KeyStore.getInstance(storeType);
            ks.load(in, storePassword.getChars());
            return ks;
        }
    }

    /**
     * generate a new exception caused by a missing file, that is required for this trust config
     */
    protected ElasticsearchException missingTrustConfigFile(IOException cause, String fileType, Path path) {
        return new ElasticsearchException(
            "failed to initialize SSL TrustManager - " + fileType + " file [{}] does not exist", cause, path.toAbsolutePath());
    }

    /**
     * generate a new exception caused by an unreadable file (i.e. file-system access denied), that is required for this trust config
     */
    protected ElasticsearchException unreadableTrustConfigFile(AccessDeniedException cause, String fileType, Path path) {
        return new ElasticsearchException(
            "failed to initialize SSL TrustManager - not permitted to read " + fileType + " file [{}]", cause, path.toAbsolutePath());
    }

    /**
     * generate a new exception caused by a blocked file (i.e. security-manager access denied), that is required for this trust config
     * @param paths A list of possible files. Depending on the context, it may not be possible to know exactly which file caused the
     *              exception, so this method accepts multiple paths.
     */
    protected ElasticsearchException blockedTrustConfigFile(AccessControlException cause, Environment environment,
                                                            String fileType, List<Path> paths) {
        if (paths.size() == 1) {
            return new ElasticsearchException(
                "failed to initialize SSL TrustManager - access to read {} file [{}] is blocked;" +
                    " SSL resources should be placed in the [{}] directory",
                cause, fileType, paths.get(0).toAbsolutePath(), environment.configFile());
        } else {
            final String pathString = paths.stream().map(Path::toAbsolutePath).map(Path::toString).collect(Collectors.joining(", "));
            return new ElasticsearchException(
                "failed to initialize SSL TrustManager - access to read one or more of the {} files [{}] is blocked;" +
                    " SSL resources should be placed in the [{}] directory",
                cause, fileType, pathString, environment.configFile());
        }
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
                    .toArray(X509Certificate[]::new));
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
        List<Path> filesToMonitor(Environment environment) {
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
            if ((o instanceof CombiningTrustConfig) == false) {
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
