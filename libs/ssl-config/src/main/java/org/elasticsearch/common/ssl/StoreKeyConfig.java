/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * A {@link SslKeyConfig} that builds a Key Manager from a keystore file.
 */
public class StoreKeyConfig extends SslKeystoreConfig {
    private final String keystorePath;
    private final String type;

    /**
     * @param path          The path to the keystore file
     * @param storePassword The password for the keystore
     * @param type          The {@link KeyStore#getType() type} of the keystore (typically "PKCS12" or "jks").
     *                      See {@link KeyStoreUtil#inferKeyStoreType}.
     * @param keyPassword   The password for the key(s) within the keystore
     *                      (see {@link javax.net.ssl.KeyManagerFactory#init(KeyStore, char[])}).
     * @param algorithm     The algorithm to use for the Key Manager (see {@link KeyManagerFactory#getAlgorithm()}).
     * @param configBasePath The base path for configuration files (used for error handling)
     */
    public StoreKeyConfig(String path, char[] storePassword, String type, char[] keyPassword, String algorithm, Path configBasePath) {
        super(storePassword, keyPassword, algorithm, configBasePath);
        this.keystorePath = Objects.requireNonNull(path, "Keystore path cannot be null");
        this.type = Objects.requireNonNull(type, "Keystore type cannot be null");
    }

    @Override
    public SslTrustConfig asTrustConfig() {
        final String trustStoreAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        return new StoreTrustConfig(keystorePath, getKeystorePassword(), type, trustStoreAlgorithm, false, getConfigBasePath());
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return List.of(resolvePath());
    }

    @Override
    public String getKeystorePath() {
        return keystorePath;
    }

    @Override
    public String getKeystoreType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreKeyConfig that = (StoreKeyConfig) o;
        return super.equals(that) &&
            keystorePath.equals(that.keystorePath)
            && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(keystorePath, type);
        result = 31 * result + super.hashCode();
        return result;
    }

}
