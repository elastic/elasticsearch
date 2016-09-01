/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.XPackSettings;

/**
 * Represents the configuration for an SSLContext
 */
class SSLConfiguration {

    // These settings are never registered, but they exist so that we can parse the values defined under grouped settings. Also, some are
    // implemented as optional settings, which provides a declarative manner for fallback as we typically fallback to values from a
    // different configuration
    private static final Setting<List<String>> CIPHERS_SETTING = Setting.listSetting("cipher_suites", Collections.emptyList(), s -> s);
    private static final Setting<List<String>> SUPPORTED_PROTOCOLS_SETTING =
            Setting.listSetting("supported_protocols", Collections.emptyList(), s -> s);
    private static final Setting<Optional<String>> KEYSTORE_PATH_SETTING =
            new Setting<>("keystore.path", (String) null, Optional::ofNullable);
    private static final Setting<Optional<String>> KEYSTORE_PASSWORD_SETTING =
            new Setting<>("keystore.password", (String) null, Optional::ofNullable);
    private static final Setting<String> KEYSTORE_ALGORITHM_SETTING = new Setting<>("keystore.algorithm",
            s -> System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm()), Function.identity());
    private static final Setting<Optional<String>> KEYSTORE_KEY_PASSWORD_SETTING =
            new Setting<>("keystore.key_password", KEYSTORE_PASSWORD_SETTING, Optional::ofNullable);
    private static final Setting<Optional<String>> TRUSTSTORE_PATH_SETTING =
            new Setting<>("truststore.path", (String) null, Optional::ofNullable);
    private static final Setting<Optional<String>> TRUSTSTORE_PASSWORD_SETTING =
            new Setting<>("truststore.password", (String) null, Optional::ofNullable);
    private static final Setting<String> TRUSTSTORE_ALGORITHM_SETTING = new Setting<>("truststore.algorithm",
            s -> System.getProperty("ssl.TrustManagerFactory.algorithm",
                    TrustManagerFactory.getDefaultAlgorithm()), Function.identity());
    private static final Setting<Optional<String>> KEY_PATH_SETTING =
            new Setting<>("key", (String) null, Optional::ofNullable);
    private static final Setting<Optional<String>> KEY_PASSWORD_SETTING =
            new Setting<>("key_passphrase", (String) null, Optional::ofNullable);
    private static final Setting<Optional<String>> CERT_SETTING =
            new Setting<>("certificate", (String) null, Optional::ofNullable);
    private static final Setting<List<String>> CA_PATHS_SETTING =
            Setting.listSetting("certificate_authorities", Collections.emptyList(), s -> s);
    private static final Setting<Optional<SSLClientAuth>> CLIENT_AUTH_SETTING =
            new Setting<>("client_authentication", (String) null, s -> {
                if (s == null) {
                    return Optional.ofNullable(null);
                } else {
                    return Optional.of(SSLClientAuth.parse(s));
                }
            });
    private static final Setting<Optional<VerificationMode>> VERIFICATION_MODE_SETTING = new Setting<>("verification_mode", (String) null,
            s -> {
                if (s == null) {
                    return Optional.ofNullable(null);
                } else {
                    return Optional.of(VerificationMode.parse(s));
                }
            });

    private final KeyConfig keyConfig;
    private final TrustConfig trustConfig;
    private final List<String> ciphers;
    private final List<String> supportedProtocols;
    private final SSLClientAuth sslClientAuth;
    private final VerificationMode verificationMode;

    /**
     * Creates a new SSLConfiguration from the given settings. There is no fallback configuration when invoking this constructor so
     * un-configured aspects will take on their default values.
     * @param settings the SSL specific settings; only the settings under a *.ssl. prefix
     */
    SSLConfiguration(Settings settings) {
        this.keyConfig = createKeyConfig(settings, null);
        this.trustConfig = createTrustConfig(settings, keyConfig, null);
        this.ciphers = getListOrDefault(CIPHERS_SETTING, settings, XPackSettings.DEFAULT_CIPHERS);
        this.supportedProtocols = getListOrDefault(SUPPORTED_PROTOCOLS_SETTING, settings, XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS);
        this.sslClientAuth = CLIENT_AUTH_SETTING.get(settings).orElse(XPackSettings.CLIENT_AUTH_DEFAULT);
        this.verificationMode = VERIFICATION_MODE_SETTING.get(settings).orElse(XPackSettings.VERIFICATION_MODE_DEFAULT);
    }

    /**
     * Creates a new SSLConfiguration from the given settings and global/default SSLConfiguration. If the settings do not contain a value
     * for a given aspect, the value from the global configuration will be used.
     * @param settings the SSL specific settings; only the settings under a *.ssl. prefix
     * @param globalSSLConfiguration the default configuration that is used as a fallback
     */
    SSLConfiguration(Settings settings, SSLConfiguration globalSSLConfiguration) {
        Objects.requireNonNull(globalSSLConfiguration);
        this.keyConfig = createKeyConfig(settings, globalSSLConfiguration);
        this.trustConfig = createTrustConfig(settings, keyConfig, globalSSLConfiguration);
        this.ciphers = getListOrDefault(CIPHERS_SETTING, settings, globalSSLConfiguration.cipherSuites());
        this.supportedProtocols = getListOrDefault(SUPPORTED_PROTOCOLS_SETTING, settings, globalSSLConfiguration.supportedProtocols());
        this.sslClientAuth = CLIENT_AUTH_SETTING.get(settings).orElse(globalSSLConfiguration.sslClientAuth());
        this.verificationMode = VERIFICATION_MODE_SETTING.get(settings).orElse(globalSSLConfiguration.verificationMode());
    }

    /**
     * The configuration for the key, if any, that will be used as part of this ssl configuration
     */
    KeyConfig keyConfig() {
        return keyConfig;
    }

    /**
     * The configuration of trust material that will be used as part of this ssl configuration
     */
    TrustConfig trustConfig() {
        return trustConfig;
    }

    /**
     * The cipher suites that will be used for this ssl configuration
     */
    List<String> cipherSuites() {
        return ciphers;
    }

    /**
     * The protocols that are supported by this configuration
     */
    List<String> supportedProtocols() {
        return supportedProtocols;
    }

    /**
     * The verification mode for this configuration; this mode controls certificate and hostname verification
     */
    VerificationMode verificationMode() {
        return verificationMode;
    }

    /**
     * The client auth configuration
     */
    SSLClientAuth sslClientAuth() {
        return sslClientAuth;
    }

    /**
     * Provides the list of paths to files that back this configuration
     */
    List<Path> filesToMonitor(@Nullable Environment environment) {
        if (keyConfig() == trustConfig()) {
            return keyConfig().filesToMonitor(environment);
        }
        List<Path> paths = new ArrayList<>(keyConfig().filesToMonitor(environment));
        paths.addAll(trustConfig().filesToMonitor(environment));
        return paths;
    }

    @Override
    public String toString() {
        return "SSLConfiguration{" +
                "keyConfig=[" + keyConfig +
                "], trustConfig=" + trustConfig +
                "], cipherSuites=[" + ciphers +
                "], supportedProtocols=[" + supportedProtocols +
                "], sslClientAuth=[" + sslClientAuth +
                "], verificationMode=[" + verificationMode +
                "]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SSLConfiguration)) return false;

        SSLConfiguration that = (SSLConfiguration) o;

        if (this.keyConfig() != null ? !this.keyConfig().equals(that.keyConfig()) : that.keyConfig() != null) {
            return false;
        }
        if (this.trustConfig() != null ? !this.trustConfig().equals(that.trustConfig()) : that.trustConfig() != null) {
            return false;
        }
        if (this.cipherSuites() != null ? !this.cipherSuites().equals(that.cipherSuites()) : that.cipherSuites() != null) {
            return false;
        }
        if (!this.supportedProtocols().equals(that.supportedProtocols())) {
            return false;
        }
        if (this.verificationMode() != that.verificationMode()) {
            return false;
        }
        if (this.sslClientAuth() != that.sslClientAuth()) {
            return false;
        }
        return this.supportedProtocols() != null ?
                this.supportedProtocols().equals(that.supportedProtocols()) : that.supportedProtocols() == null;
    }

    @Override
    public int hashCode() {
        int result = this.keyConfig() != null ? this.keyConfig().hashCode() : 0;
        result = 31 * result + (this.trustConfig() != null ? this.trustConfig().hashCode() : 0);
        result = 31 * result + (this.cipherSuites() != null ? this.cipherSuites().hashCode() : 0);
        result = 31 * result + (this.supportedProtocols() != null ? this.supportedProtocols().hashCode() : 0);
        result = 31 * result + this.verificationMode().hashCode();
        result = 31 * result + this.sslClientAuth().hashCode();
        return result;
    }

    private static KeyConfig createKeyConfig(Settings settings, SSLConfiguration global) {
        String keyStorePath = KEYSTORE_PATH_SETTING.get(settings).orElse(null);
        String keyPath = KEY_PATH_SETTING.get(settings).orElse(null);
        if (keyPath != null && keyStorePath != null) {
            throw new IllegalArgumentException("you cannot specify a keystore and key file");
        } else if (keyStorePath == null && keyPath == null) {
            if (global != null) {
                return global.keyConfig();
            } else if (System.getProperty("javax.net.ssl.keyStore") != null) {
                return new StoreKeyConfig(System.getProperty("javax.net.ssl.keyStore"),
                        System.getProperty("javax.net.ssl.keyStorePassword", ""), System.getProperty("javax.net.ssl.keyStorePassword", ""),
                        System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm()),
                        System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm()));
            }
            return KeyConfig.NONE;
        }

        if (keyPath != null) {
            String keyPassword = KEY_PASSWORD_SETTING.get(settings).orElse(null);
            String certPath = CERT_SETTING.get(settings).orElse(null);
            if (certPath == null) {
                throw new IllegalArgumentException("you must specify the certificates to use with the key");
            }
            return new PEMKeyConfig(keyPath, keyPassword, certPath);
        } else {
            String keyStorePassword = KEYSTORE_PASSWORD_SETTING.get(settings).orElse(null);
            String keyStoreAlgorithm = KEYSTORE_ALGORITHM_SETTING.get(settings);
            String keyStoreKeyPassword = KEYSTORE_KEY_PASSWORD_SETTING.get(settings).orElse(keyStorePassword);
            String trustStoreAlgorithm = TRUSTSTORE_ALGORITHM_SETTING.get(settings);
            return new StoreKeyConfig(keyStorePath, keyStorePassword, keyStoreKeyPassword, keyStoreAlgorithm, trustStoreAlgorithm);
        }
    }

    private static TrustConfig createTrustConfig(Settings settings, KeyConfig keyConfig, SSLConfiguration global) {
        String trustStorePath = TRUSTSTORE_PATH_SETTING.get(settings).orElse(null);
        List<String> caPaths = getListOrNull(CA_PATHS_SETTING, settings);
        if (trustStorePath != null && caPaths != null) {
            throw new IllegalArgumentException("you cannot specify a truststore and ca files");
        }

        VerificationMode verificationMode = VERIFICATION_MODE_SETTING.get(settings).orElseGet(() -> {
            if (global != null) {
                return global.verificationMode();
            }
            return XPackSettings.VERIFICATION_MODE_DEFAULT;
        });
        if (verificationMode.isCertificateVerificationEnabled() == false) {
            return TrustAllConfig.INSTANCE;
        } else if (caPaths != null) {
            return new PEMTrustConfig(caPaths);
        } else if (trustStorePath != null) {
            String trustStorePassword = TRUSTSTORE_PASSWORD_SETTING.get(settings).orElse(null);
            String trustStoreAlgorithm = TRUSTSTORE_ALGORITHM_SETTING.get(settings);
            return new StoreTrustConfig(trustStorePath, trustStorePassword, trustStoreAlgorithm);
        } else if (global == null && System.getProperty("javax.net.ssl.trustStore") != null) {
            return new StoreTrustConfig(System.getProperty("javax.net.ssl.trustStore"),
                    System.getProperty("javax.net.ssl.trustStorePassword", ""),
                    System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm()));
        } else if (global != null && keyConfig == global.keyConfig()) {
            return global.trustConfig();
        } else if (keyConfig != KeyConfig.NONE) {
            return DefaultJDKTrustConfig.merge(keyConfig);
        } else {
            return DefaultJDKTrustConfig.INSTANCE;
        }
    }

    private static List<String> getListOrNull(Setting<List<String>> listSetting, Settings settings) {
        return getListOrDefault(listSetting, settings, null);
    }

    private static List<String> getListOrDefault(Setting<List<String>> listSetting, Settings settings, List<String> defaultValue) {
        if (listSetting.exists(settings)) {
            return listSetting.get(settings);
        }
        return defaultValue;
    }
}
