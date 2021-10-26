/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class for handling the standard setting keys for use in SSL configuration.
 *
 * @see SslConfiguration
 * @see SslConfigurationLoader
 */
public class SslConfigurationKeys {
    /**
     * The SSL/TLS protocols (i.e. versions) that should be used
     */
    public static final String PROTOCOLS = "supported_protocols";

    /**
     * The SSL/TLS cipher suites that should be used
     */
    public static final String CIPHERS = "cipher_suites";

    /**
     * Whether certificate and/or hostname verification should be used
     */
    public static final String VERIFICATION_MODE = "verification_mode";

    /**
     * When operating as a server, whether to request/require client certificates
     */
    public static final String CLIENT_AUTH = "client_authentication";

    // Trust
    /**
     * A list of paths to PEM formatted certificates that should be trusted as CAs
     */
    public static final String CERTIFICATE_AUTHORITIES = "certificate_authorities";
    /**
     * The path to a KeyStore file (in a format supported by this JRE) that should be used as a trust-store
     */
    public static final String TRUSTSTORE_PATH = "truststore.path";
    /**
     * The password for the file configured in {@link #TRUSTSTORE_PATH}, as a secure setting.
     */
    public static final String TRUSTSTORE_SECURE_PASSWORD = "truststore.secure_password";
    /**
     * The password for the file configured in {@link #TRUSTSTORE_PATH}, as a non-secure setting.
     * The use of this setting {@link #isDeprecated(String) is deprecated}.
     */
    public static final String TRUSTSTORE_LEGACY_PASSWORD = "truststore.password";
    /**
     * The {@link KeyStore#getType() keystore type} for the file configured in {@link #TRUSTSTORE_PATH}.
     */
    public static final String TRUSTSTORE_TYPE = "truststore.type";
    /**
     * The {@link TrustManagerFactory#getAlgorithm() trust management algorithm} to use when configuring trust
     * with a {@link #TRUSTSTORE_PATH truststore}.
     */
    public static final String TRUSTSTORE_ALGORITHM = "truststore.algorithm";

    // Key Management
    // -- Keystore
    /**
     * The path to a KeyStore file (in a format supported by this JRE) that should be used for key management
     */
    public static final String KEYSTORE_PATH = "keystore.path";
    /**
     * The password for the file configured in {@link #KEYSTORE_PATH}, as a secure setting.
     */
    public static final String KEYSTORE_SECURE_PASSWORD = "keystore.secure_password";
    /**
     * The password for the file configured in {@link #KEYSTORE_PATH}, as a non-secure setting.
     * The use of this setting {@link #isDeprecated(String) is deprecated}.
     */
    public static final String KEYSTORE_LEGACY_PASSWORD = "keystore.password";
    /**
     * The password for the key within the {@link #KEYSTORE_PATH configured keystore}, as a secure setting.
     * If no key password is specified, it will default to the keystore password.
     */
    public static final String KEYSTORE_SECURE_KEY_PASSWORD = "keystore.secure_key_password";
    /**
     * The password for the key within the {@link #KEYSTORE_PATH configured keystore}, as a non-secure setting.
     * The use of this setting {@link #isDeprecated(String) is deprecated}.
     * If no key password is specified, it will default to the keystore password.
     */
    public static final String KEYSTORE_LEGACY_KEY_PASSWORD = "keystore.key_password";
    /**
     * The {@link KeyStore#getType() keystore type} for the file configured in {@link #KEYSTORE_PATH}.
     */
    public static final String KEYSTORE_TYPE = "keystore.type";
    /**
     * The {@link javax.net.ssl.KeyManagerFactory#getAlgorithm() key management algorithm} to use when
     * connstructing a Key manager from a {@link #KEYSTORE_PATH keystore}.
     */
    public static final String KEYSTORE_ALGORITHM = "keystore.algorithm";
    // -- PEM
    /**
     * The path to a PEM formatted file that contains the certificate to be used as part of key management
     */
    public static final String CERTIFICATE = "certificate";
    /**
     * The path to a PEM formatted file that contains the private key for the configured {@link #CERTIFICATE}.
     */
    public static final String KEY = "key";
    /**
     * The password to read the configured {@link #KEY}, as a secure setting.
     * This (or the {@link #KEY_LEGACY_PASSPHRASE legacy fallback}) is required if the key file is encrypted.
     */
    public static final String KEY_SECURE_PASSPHRASE = "secure_key_passphrase";
    /**
     * The password to read the configured {@link #KEY}, as a non-secure setting.
     * The use of this setting {@link #isDeprecated(String) is deprecated}.
     */
    public static final String KEY_LEGACY_PASSPHRASE = "key_passphrase";

    private static final Set<String> DEPRECATED_KEYS = new HashSet<>(
        Arrays.asList(TRUSTSTORE_LEGACY_PASSWORD, KEYSTORE_LEGACY_PASSWORD, KEYSTORE_LEGACY_KEY_PASSWORD, KEY_LEGACY_PASSPHRASE)
    );

    private SslConfigurationKeys() {
        throw new IllegalStateException("Utility class should not be instantiated");
    }

    /**
     * The list of keys that are used to load a non-secure, non-list setting
     */
    public static List<String> getStringKeys() {
        return Arrays.asList(
            VERIFICATION_MODE, CLIENT_AUTH,
            TRUSTSTORE_PATH, TRUSTSTORE_LEGACY_PASSWORD, TRUSTSTORE_TYPE, TRUSTSTORE_TYPE,
            KEYSTORE_PATH, KEYSTORE_LEGACY_PASSWORD, KEYSTORE_LEGACY_KEY_PASSWORD, KEYSTORE_TYPE, KEYSTORE_ALGORITHM,
            CERTIFICATE, KEY, KEY_LEGACY_PASSPHRASE
        );
    }

    /**
     * The list of keys that are used to load a non-secure, list setting
     */
    public static List<String> getListKeys() {
        return Arrays.asList(PROTOCOLS, CIPHERS, CERTIFICATE_AUTHORITIES);
    }

    /**
     * The list of keys that are used to load a secure setting (such as a password) that would typically be stored in the elasticsearch
     * keystore.
     */
    public static List<String> getSecureStringKeys() {
        return Arrays.asList(TRUSTSTORE_SECURE_PASSWORD, KEYSTORE_SECURE_PASSWORD, KEYSTORE_SECURE_KEY_PASSWORD, KEY_SECURE_PASSPHRASE);
    }

    /**
     * @return {@code true} if the provided key is a deprecated setting
     */
    public static boolean isDeprecated(String key) {
        return DEPRECATED_KEYS.contains(key);
    }

}
