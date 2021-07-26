/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.jdk.JavaVersion;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.ssl.KeyStoreUtil.inferKeyStoreType;
import static org.elasticsearch.common.ssl.SslConfiguration.ORDERED_PROTOCOL_ALGORITHM_MAP;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.CERTIFICATE;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.CERTIFICATE_AUTHORITIES;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.CIPHERS;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.CLIENT_AUTH;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.KEY;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.KEYSTORE_ALGORITHM;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.KEYSTORE_LEGACY_KEY_PASSWORD;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.KEYSTORE_LEGACY_PASSWORD;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.KEYSTORE_PATH;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.KEYSTORE_SECURE_KEY_PASSWORD;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.KEYSTORE_SECURE_PASSWORD;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.KEYSTORE_TYPE;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.KEY_LEGACY_PASSPHRASE;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.KEY_SECURE_PASSPHRASE;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.PROTOCOLS;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.TRUSTSTORE_ALGORITHM;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.TRUSTSTORE_LEGACY_PASSWORD;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.TRUSTSTORE_PATH;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.TRUSTSTORE_SECURE_PASSWORD;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.TRUSTSTORE_TYPE;
import static org.elasticsearch.common.ssl.SslConfigurationKeys.VERIFICATION_MODE;

/**
 * Loads {@link SslConfiguration} from settings.
 * This class handles the logic of interpreting the various "ssl.*" configuration settings and their interactions
 * (as well as being aware of dependencies and conflicts between different settings).
 * The constructed {@code SslConfiguration} has sensible defaults for any settings that are not explicitly configured,
 * and these defaults can be overridden through the various {@code setDefaultXyz} methods.
 * It is {@code abstract} because this library has minimal dependencies, so the extraction of the setting values from
 * the underlying setting source must be handled by the code that makes use of this class.
 *
 * @see SslConfiguration
 * @see SslConfigurationKeys
 */
public abstract class SslConfigurationLoader {

    static final List<String> DEFAULT_PROTOCOLS = Collections.unmodifiableList(
        ORDERED_PROTOCOL_ALGORITHM_MAP.containsKey("TLSv1.3") ?
            Arrays.asList("TLSv1.3", "TLSv1.2", "TLSv1.1") : Arrays.asList("TLSv1.2", "TLSv1.1"));

    private static final List<String> JDK11_CIPHERS = List.of(
        // TLSv1.3 cipher has PFS, AEAD, hardware support
        "TLS_AES_256_GCM_SHA384",
        "TLS_AES_128_GCM_SHA256",

        // PFS, AEAD, hardware support
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",

        // PFS, AEAD, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",

        // PFS, hardware support
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",

        // PFS, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",

        // PFS, hardware support
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",

        // PFS, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",

        // AEAD, hardware support
        "TLS_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_RSA_WITH_AES_128_GCM_SHA256",

        // hardware support
        "TLS_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_RSA_WITH_AES_128_CBC_SHA256",

        // hardware support
        "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_RSA_WITH_AES_128_CBC_SHA"
    );

    private static final List<String> JDK12_CIPHERS = List.of(
        // TLSv1.3 cipher has PFS, AEAD, hardware support
        "TLS_AES_256_GCM_SHA384",
        "TLS_AES_128_GCM_SHA256",

        // TLSv1.3 cipher has PFS, AEAD
        "TLS_CHACHA20_POLY1305_SHA256",

        // PFS, AEAD, hardware support
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",

        // PFS, AEAD, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",

        // PFS, AEAD
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",

        // PFS, hardware support
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",

        // PFS, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",

        // PFS, hardware support
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",

        // PFS, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",

        // AEAD, hardware support
        "TLS_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_RSA_WITH_AES_128_GCM_SHA256",

        // hardware support
        "TLS_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_RSA_WITH_AES_128_CBC_SHA256",

        // hardware support
        "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_RSA_WITH_AES_128_CBC_SHA"
    );

    static final List<String> DEFAULT_CIPHERS =
        JavaVersion.current().compareTo(JavaVersion.parse("12")) > -1 ? JDK12_CIPHERS : JDK11_CIPHERS;
    private static final char[] EMPTY_PASSWORD = new char[0];

    private final String settingPrefix;

    private SslTrustConfig defaultTrustConfig;
    private SslKeyConfig defaultKeyConfig;
    private SslVerificationMode defaultVerificationMode;
    private SslClientAuthenticationMode defaultClientAuth;
    private List<String> defaultCiphers;
    private List<String> defaultProtocols;

    /**
     * Construct a new loader with the "standard" default values.
     *
     * @param settingPrefix The prefix to apply to all settings that are loaded. It may be the empty string, otherwise it
     *                      must end in a "." (period). For example, if the prefix is {@code "reindex.ssl."} then the keys that are
     *                      passed to methods like {@link #getSettingAsString(String)} will be in the form
     *                      {@code "reindex.ssl.verification_mode"}, and those same keys will be reported in error messages (via
     *                      {@link SslConfigException}).
     */
    public SslConfigurationLoader(String settingPrefix) {
        this.settingPrefix = settingPrefix == null ? "" : settingPrefix;
        if (this.settingPrefix.isEmpty() == false && this.settingPrefix.endsWith(".") == false) {
            throw new IllegalArgumentException("Setting prefix [" + settingPrefix + "] must be blank or end in '.'");
        }
        this.defaultTrustConfig = new DefaultJdkTrustConfig();
        this.defaultKeyConfig = EmptyKeyConfig.INSTANCE;
        this.defaultVerificationMode = SslVerificationMode.FULL;
        this.defaultClientAuth = SslClientAuthenticationMode.OPTIONAL;
        this.defaultProtocols = DEFAULT_PROTOCOLS;
        this.defaultCiphers = DEFAULT_CIPHERS;
    }

    /**
     * Change the default trust config.
     * The initial trust config is {@link DefaultJdkTrustConfig}, which trusts the JDK's default CA certs
     */
    public void setDefaultTrustConfig(SslTrustConfig defaultTrustConfig) {
        this.defaultTrustConfig = defaultTrustConfig;
    }

    /**
     * Change the default key config.
     * The initial key config is {@link EmptyKeyConfig}, which does not provide any keys
     */
    public void setDefaultKeyConfig(SslKeyConfig defaultKeyConfig) {
        this.defaultKeyConfig = defaultKeyConfig;
    }

    /**
     * Change the default verification mode.
     * The initial verification mode is {@link SslVerificationMode#FULL}.
     */
    public void setDefaultVerificationMode(SslVerificationMode defaultVerificationMode) {
        this.defaultVerificationMode = defaultVerificationMode;
    }

    /**
     * Change the default client authentication mode.
     * The initial client auth mode is {@link SslClientAuthenticationMode#OPTIONAL}.
     */
    public void setDefaultClientAuth(SslClientAuthenticationMode defaultClientAuth) {
        this.defaultClientAuth = defaultClientAuth;
    }

    /**
     * Change the default supported ciphers.
     */
    public void setDefaultCiphers(List<String> defaultCiphers) {
        this.defaultCiphers = defaultCiphers;
    }

    /**
     * Change the default SSL/TLS protocol list.
     * The initial protocol list is defined by {@link #DEFAULT_PROTOCOLS}
     */
    public void setDefaultProtocols(List<String> defaultProtocols) {
        this.defaultProtocols = defaultProtocols;
    }

    /**
     * Clients of this class should implement this method to determine whether there are any settings for a given prefix.
     * This is used to populate {@link SslConfiguration#isExplicitlyConfigured()}.
     */
    protected abstract boolean hasSettings(String prefix);

    /**
     * Clients of this class should implement this method to load a fully-qualified key from the preferred settings source.
     * This method will be called for basic string settings (see {@link SslConfigurationKeys#getStringKeys()}).
     * <p>
     * The setting should be returned as a string, and this class will convert it to the relevant type.
     *
     * @throws Exception If a {@link RuntimeException} is thrown, it will be rethrown unwrapped. All checked exceptions are wrapped in
     *                   {@link SslConfigException} before being rethrown.
     */
    protected abstract String getSettingAsString(String key) throws Exception;

    /**
     * Clients of this class should implement this method to load a fully-qualified key from the preferred secure settings source.
     * This method will be called for any setting keys that are marked as being
     * {@link SslConfigurationKeys#getSecureStringKeys() secure} settings.
     *
     * @throws Exception If a {@link RuntimeException} is thrown, it will be rethrown unwrapped. All checked exceptions are wrapped in
     *                   {@link SslConfigException} before being rethrown.
     */
    protected abstract char[] getSecureSetting(String key) throws Exception;

    /**
     * Clients of this class should implement this method to load a fully-qualified key from the preferred settings source.
     * This method will be called for list settings (see {@link SslConfigurationKeys#getListKeys()}).
     * <p>
     * The setting should be returned as a list of strings, and this class will convert the values to the relevant type.
     *
     * @throws Exception If a {@link RuntimeException} is thrown, it will be rethrown unwrapped. All checked exceptions are wrapped in
     *                   {@link SslConfigException} before being rethrown.
     */
    protected abstract List<String> getSettingAsList(String key) throws Exception;

    /**
     * Resolve all necessary configuration settings, and load a {@link SslConfiguration}.
     *
     * @param basePath The base path to use for any settings that represent file paths. Typically points to the Elasticsearch
     *                 configuration directory.
     * @throws SslConfigException For any problems with the configuration, or with loading the required SSL classes.
     */
    public SslConfiguration load(Path basePath) {
        Objects.requireNonNull(basePath, "Base Path cannot be null");
        final List<String> protocols = resolveListSetting(PROTOCOLS, Function.identity(), defaultProtocols);
        final List<String> ciphers = resolveListSetting(CIPHERS, Function.identity(), defaultCiphers);
        final SslVerificationMode verificationMode = resolveSetting(VERIFICATION_MODE, SslVerificationMode::parse, defaultVerificationMode);
        final SslClientAuthenticationMode clientAuth = resolveSetting(CLIENT_AUTH, SslClientAuthenticationMode::parse, defaultClientAuth);

        final SslKeyConfig keyConfig = buildKeyConfig(basePath);
        final SslTrustConfig trustConfig = buildTrustConfig(basePath, verificationMode, keyConfig);

        if (protocols == null || protocols.isEmpty()) {
            throw new SslConfigException("no protocols configured in [" + settingPrefix + PROTOCOLS + "]");
        }
        if (ciphers == null || ciphers.isEmpty()) {
            throw new SslConfigException("no cipher suites configured in [" + settingPrefix + CIPHERS + "]");
        }
        final boolean isExplicitlyConfigured = hasSettings(settingPrefix);
        return new SslConfiguration(isExplicitlyConfigured, trustConfig, keyConfig, verificationMode, clientAuth, ciphers, protocols);
    }

    protected SslTrustConfig buildTrustConfig(Path basePath, SslVerificationMode verificationMode, SslKeyConfig keyConfig) {
        final List<String> certificateAuthorities = resolveListSetting(CERTIFICATE_AUTHORITIES, Function.identity(), null);
        final String trustStorePath = resolveSetting(TRUSTSTORE_PATH, Function.identity(), null);

        if (certificateAuthorities != null && trustStorePath != null) {
            throw new SslConfigException("cannot specify both [" + settingPrefix + CERTIFICATE_AUTHORITIES + "] and [" +
                settingPrefix + TRUSTSTORE_PATH + "]");
        }
        if (verificationMode.isCertificateVerificationEnabled() == false) {
            return TrustEverythingConfig.TRUST_EVERYTHING;
        }
        if (certificateAuthorities != null) {
            return new PemTrustConfig(certificateAuthorities, basePath);
        }
        if (trustStorePath != null) {
            final char[] password = resolvePasswordSetting(TRUSTSTORE_SECURE_PASSWORD, TRUSTSTORE_LEGACY_PASSWORD);
            final String storeType = resolveSetting(TRUSTSTORE_TYPE, Function.identity(), inferKeyStoreType(trustStorePath));
            final String algorithm = resolveSetting(TRUSTSTORE_ALGORITHM, Function.identity(), TrustManagerFactory.getDefaultAlgorithm());
            return new StoreTrustConfig(trustStorePath, password, storeType, algorithm, true, basePath);
        }
        return buildDefaultTrustConfig(defaultTrustConfig, keyConfig);
    }

    protected SslTrustConfig buildDefaultTrustConfig(SslTrustConfig defaultTrustConfig, SslKeyConfig keyConfig) {
        final SslTrustConfig trust = keyConfig.asTrustConfig();
        if (trust == null) {
            return defaultTrustConfig;
        } else {
            return new CompositeTrustConfig(List.of(defaultTrustConfig, trust));
        }
    }

    public SslKeyConfig buildKeyConfig(Path basePath) {
        final String certificatePath = stringSetting(CERTIFICATE);
        final String keyPath = stringSetting(KEY);
        final String keyStorePath = stringSetting(KEYSTORE_PATH);

        if (certificatePath != null && keyStorePath != null) {
            throw new SslConfigException("cannot specify both [" + settingPrefix + CERTIFICATE + "] and [" +
                settingPrefix + KEYSTORE_PATH + "]");
        }

        if (certificatePath != null || keyPath != null) {
            if (keyPath == null) {
                throw new SslConfigException("cannot specify [" + settingPrefix + CERTIFICATE + "] without also setting [" +
                    settingPrefix + KEY + "]");
            }
            if (certificatePath == null) {
                throw new SslConfigException("cannot specify [" + settingPrefix + KEYSTORE_PATH + "] without also setting [" +
                    settingPrefix + CERTIFICATE + "]");
            }
            final char[] password = resolvePasswordSetting(KEY_SECURE_PASSPHRASE, KEY_LEGACY_PASSPHRASE);
            return new PemKeyConfig(certificatePath, keyPath, password, basePath);
        }

        if (keyStorePath != null) {
            final char[] storePassword = resolvePasswordSetting(KEYSTORE_SECURE_PASSWORD, KEYSTORE_LEGACY_PASSWORD);
            char[] keyPassword = resolvePasswordSetting(KEYSTORE_SECURE_KEY_PASSWORD, KEYSTORE_LEGACY_KEY_PASSWORD);
            if (keyPassword.length == 0) {
                keyPassword = storePassword;
            }
            final String storeType = resolveSetting(KEYSTORE_TYPE, Function.identity(), inferKeyStoreType(keyStorePath));
            final String algorithm = resolveSetting(KEYSTORE_ALGORITHM, Function.identity(), KeyManagerFactory.getDefaultAlgorithm());
            return new StoreKeyConfig(keyStorePath, storePassword, storeType, keyPassword, algorithm, basePath);
        }

        return defaultKeyConfig;
    }

    protected Path resolvePath(String settingKey, Path basePath) {
        return resolveSetting(settingKey, basePath::resolve, null);
    }

    private String expandSettingKey(String key) {
        return settingPrefix + key;
    }

    private char[] resolvePasswordSetting(String secureSettingKey, String legacySettingKey) {
        final char[] securePassword = resolveSecureSetting(secureSettingKey, null);
        final String legacyPassword = stringSetting(legacySettingKey);
        if (securePassword == null) {
            if (legacyPassword == null) {
                return EMPTY_PASSWORD;
            } else {
                return legacyPassword.toCharArray();
            }
        } else {
            if (legacyPassword != null) {
                throw new SslConfigException("cannot specify both [" + settingPrefix + secureSettingKey + "] and ["
                    + settingPrefix + legacySettingKey + "]");
            } else {
                return securePassword;
            }
        }
    }

    private String stringSetting(String key) {
        return resolveSetting(key, Function.identity(), null);
    }

    private <V> V resolveSetting(String key, Function<String, V> parser, V defaultValue) {
        try {
            String setting = getSettingAsString(expandSettingKey(key));
            if (setting == null || setting.isEmpty()) {
                return defaultValue;
            }
            return parser.apply(setting);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new SslConfigException("cannot retrieve setting [" + settingPrefix + key + "]", e);
        }
    }

    private char[] resolveSecureSetting(String key, char[] defaultValue) {
        try {
            char[] setting = getSecureSetting(expandSettingKey(key));
            if (setting == null || setting.length == 0) {
                return defaultValue;
            }
            return setting;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new SslConfigException("cannot retrieve secure setting [" + settingPrefix + key + "]", e);
        }

    }

    private <V> List<V> resolveListSetting(String key, Function<String, V> parser, List<V> defaultValue) {
        try {
            final List<String> list = getSettingAsList(expandSettingKey(key));
            if (list == null || list.isEmpty()) {
                return defaultValue;
            }
            return list.stream().map(parser).collect(Collectors.toList());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new SslConfigException("cannot retrieve setting [" + settingPrefix + key + "]", e);
        }
    }
}
