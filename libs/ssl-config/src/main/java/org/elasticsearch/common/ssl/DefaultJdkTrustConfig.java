/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.core.Nullable;

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

/**
 * This class represents a trust configuration that corresponds to the default trusted CAs of the JDK
 */
public final class DefaultJdkTrustConfig implements SslTrustConfig {

    public static final DefaultJdkTrustConfig DEFAULT_INSTANCE = new DefaultJdkTrustConfig();

    private final BiFunction<String, String, String> systemProperties;
    private final char[] trustStorePassword;

    /**
     * Create a trust config that uses System properties to determine the TrustStore type, and the relevant password.
     */
    DefaultJdkTrustConfig() {
        this(System::getProperty);
    }

    /**
     * Create a trust config that uses supplied {@link BiFunction} to determine the TrustStore type, and the relevant password.
     */
    DefaultJdkTrustConfig(BiFunction<String, String, String> systemProperties) {
        this(systemProperties, isPkcs11Truststore(systemProperties) ? getSystemTrustStorePassword(systemProperties) : null);
    }

    /**
     * @param trustStorePassword the password for the truststore. It applies only when PKCS#11 tokens are used, is null otherwise
     */
    DefaultJdkTrustConfig(BiFunction<String, String, String> systemProperties, @Nullable char[] trustStorePassword) {
        this.systemProperties = systemProperties;
        this.trustStorePassword = trustStorePassword;
    }

    @Override
    public boolean isSystemDefault() {
        return true;
    }

    @Override
    public X509ExtendedTrustManager createTrustManager() {
        try {
            return KeyStoreUtil.createTrustManager(getSystemTrustStore(), TrustManagerFactory.getDefaultAlgorithm());
        } catch (GeneralSecurityException e) {
            throw new SslConfigException("failed to initialize a TrustManager for the system keystore", e);
        }
    }

    /**
     * When a PKCS#11 token is used as the system default keystore/truststore, we need to pass the keystore
     * password when loading, even for reading certificates only ( as opposed to i.e. JKS keystores where
     * we only need to pass the password for reading Private Key entries ).
     *
     * @return the KeyStore used as truststore for PKCS#11 initialized with the password, null otherwise
     */
    private KeyStore getSystemTrustStore() {
        if (isPkcs11Truststore(systemProperties) && trustStorePassword != null) {
            try {
                KeyStore keyStore = KeyStore.getInstance("PKCS11");
                keyStore.load(null, trustStorePassword);
                return keyStore;
            } catch (GeneralSecurityException | IOException e) {
                throw new SslConfigException("failed to load the system PKCS#11 truststore", e);
            }
        }
        return null;
    }

    private static boolean isPkcs11Truststore(BiFunction<String, String, String> systemProperties) {
        return systemProperties.apply("javax.net.ssl.trustStoreType", "").equalsIgnoreCase("PKCS11");
    }

    private static char[] getSystemTrustStorePassword(BiFunction<String, String, String> systemProperties) {
        return systemProperties.apply("javax.net.ssl.trustStorePassword", "").toCharArray();
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return List.of();
    }

    @Override
    public Collection<? extends StoredCertificate> getConfiguredCertificates() {
        return List.of();
    }

    @Override
    public String toString() {
        return "JDK-trusted-certs";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DefaultJdkTrustConfig that = (DefaultJdkTrustConfig) o;
        return Arrays.equals(this.trustStorePassword, that.trustStorePassword);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(trustStorePassword);
    }
}
