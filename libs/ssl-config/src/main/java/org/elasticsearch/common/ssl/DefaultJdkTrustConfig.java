/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.common.Nullable;

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.BiFunction;

/**
 * This class represents a trust configuration that corresponds to the default trusted CAs of the JDK
 */
final class DefaultJdkTrustConfig implements SslTrustConfig {

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
        return Collections.emptyList();
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
