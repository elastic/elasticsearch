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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

abstract class KeyConfig extends TrustConfig {

    static final KeyConfig NONE = new KeyConfig() {
        @Override
        X509ExtendedKeyManager createKeyManager(@Nullable Environment environment) {
            try {
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                keyStore.load(null, null);
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, null);
                return (X509ExtendedKeyManager) keyManagerFactory.getKeyManagers()[0];
            } catch (IOException | CertificateException | NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
                throw new ElasticsearchException("failed to initialize SSL KeyManager", e);
            }
        }

        @Override
        X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
            return null;
        }

        @Override
        Collection<CertificateInfo> certificates(Environment environment) {
            return Collections.emptyList();
        }

        @Override
        List<Path> filesToMonitor(@Nullable Environment environment) {
            return Collections.emptyList();
        }

        @Override
        public String toString() {
            return "NONE";
        }

        @Override
        public boolean equals(Object o) {
            return o == this;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }

        @Override
        List<PrivateKey> privateKeys(@Nullable Environment environment) {
            return Collections.emptyList();
        }
    };

    abstract X509ExtendedKeyManager createKeyManager(@Nullable Environment environment);

    /**
     * generate a new exception caused by a missing file, that is required for this key config
     */
    static ElasticsearchException missingKeyConfigFile(IOException cause, String fileType, Path path) {
        return new ElasticsearchException(
            "failed to initialize SSL KeyManager - " + fileType + " file [{}] does not exist", cause, path.toAbsolutePath());
    }

    /**
     * generate a new exception caused by an unreadable file (i.e. file-system access denied), that is required for this key config
     */
    static ElasticsearchException unreadableKeyConfigFile(AccessDeniedException cause, String fileType, Path path) {
        return new ElasticsearchException(
            "failed to initialize SSL KeyManager - not permitted to read " + fileType + " file [{}]", cause, path.toAbsolutePath());
    }

    /**
     * generate a new exception caused by a blocked file (i.e. security-manager access denied), that is required for this key config
     */
    static ElasticsearchException blockedKeyConfigFile(AccessControlException cause, Environment environment, String fileType, Path path) {
        return new ElasticsearchException(
            "failed to initialize SSL KeyManager - access to read {} file [{}] is blocked;" +
                " SSL resources should be placed in the [{}] directory", cause, fileType, path, environment.configFile());
    }

    abstract List<PrivateKey> privateKeys(@Nullable Environment environment);

}
