/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;

import javax.net.ssl.X509ExtendedTrustManager;

/**
 * Trust configuration that is backed by a {@link java.security.KeyStore}
 */
class StoreTrustConfig extends TrustConfig {

    private static final String TRUSTSTORE_FILE = "truststore";

    final String trustStorePath;
    final String trustStoreType;
    final SecureString trustStorePassword;
    final String trustStoreAlgorithm;

    /**
     * Create a new configuration based on the provided parameters
     *
     * @param trustStorePath      the path to the truststore
     * @param trustStorePassword  the password for the truststore
     * @param trustStoreAlgorithm the algorithm to use for reading the truststore
     */
    StoreTrustConfig(String trustStorePath, String trustStoreType, SecureString trustStorePassword, String trustStoreAlgorithm) {
        this.trustStorePath = trustStorePath;
        this.trustStoreType = trustStoreType;
        // since we support reloading the truststore, we must store the passphrase in memory for the life of the node, so we
        // clone the password and never close it during our uses below
        this.trustStorePassword = Objects.requireNonNull(trustStorePassword, "truststore password must be specified").clone();
        this.trustStoreAlgorithm = trustStoreAlgorithm;
    }

    @Override
    X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
        final Path storePath = resolveTrustStorePath(environment);
        try {
            KeyStore trustStore = getStore(storePath, trustStoreType, trustStorePassword);
            return CertParsingUtils.trustManager(trustStore, trustStoreAlgorithm);
        } catch (FileNotFoundException | NoSuchFileException e) {
            throw missingTrustConfigFile(e, TRUSTSTORE_FILE, storePath);
        } catch (AccessDeniedException e) {
            throw unreadableTrustConfigFile(e, TRUSTSTORE_FILE, storePath);
        } catch (AccessControlException e) {
            throw blockedTrustConfigFile(e, environment, TRUSTSTORE_FILE, Collections.singletonList(storePath));
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize SSL TrustManager", e);
        }
    }

    @Override
    Collection<CertificateInfo> certificates(Environment environment) throws GeneralSecurityException, IOException {
        final KeyStore trustStore = getStore(environment, trustStorePath, trustStoreType, trustStorePassword);
        final List<CertificateInfo> certificates = new ArrayList<>();
        final Enumeration<String> aliases = trustStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            final Certificate certificate = trustStore.getCertificate(alias);
            if (certificate instanceof X509Certificate) {
                final boolean hasKey = trustStore.isKeyEntry(alias);
                certificates.add(new CertificateInfo(trustStorePath, trustStoreType, alias, hasKey, (X509Certificate) certificate));
            }
        }
        return certificates;
    }

    protected Path resolveTrustStorePath(Environment environment) {
        return CertParsingUtils.resolvePath(trustStorePath, environment);
    }

    protected KeyStore getStore(Path path) throws GeneralSecurityException, IOException {
        return getStore(path, trustStoreType, trustStorePassword);
    }

    @Override
    List<Path> filesToMonitor(@Nullable Environment environment) {
        if (trustStorePath == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(CertParsingUtils.resolvePath(trustStorePath, environment));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StoreTrustConfig that = (StoreTrustConfig) o;
        return Objects.equals(trustStorePath, that.trustStorePath)
            && Objects.equals(trustStoreType, that.trustStoreType)
            && Objects.equals(trustStorePassword, that.trustStorePassword)
            && Objects.equals(trustStoreAlgorithm, that.trustStoreAlgorithm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trustStorePath, trustStoreType, trustStorePassword, trustStoreAlgorithm);
    }

    @Override
    public String toString() {
        return "trustStorePath=[" + trustStorePath + "], trustStoreAlgorithm=[" + trustStoreAlgorithm + "]";
    }
}
