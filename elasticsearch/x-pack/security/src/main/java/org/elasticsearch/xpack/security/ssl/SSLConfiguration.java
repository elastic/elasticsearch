/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ssl;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;

import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.support.OptionalSettings.createInt;
import static org.elasticsearch.xpack.security.support.OptionalSettings.createString;
import static org.elasticsearch.xpack.security.support.OptionalSettings.createTimeValue;

/**
 * Class that contains all configuration related to SSL use within x-pack
 */
public abstract class SSLConfiguration {

    public abstract KeyConfig keyConfig();

    public abstract TrustConfig trustConfig();

    public abstract String protocol();

    public abstract int sessionCacheSize();

    public abstract TimeValue sessionCacheTimeout();

    public abstract List<String> ciphers();

    public abstract List<String> supportedProtocols();

    /**
     * Provides the list of paths to files that back this configuration
     */
    public List<Path> filesToMonitor(@Nullable Environment environment) {
        if (keyConfig() == trustConfig()) {
            return keyConfig().filesToMonitor(environment);
        }
        List<Path> paths = new ArrayList<>(keyConfig().filesToMonitor(environment));
        paths.addAll(trustConfig().filesToMonitor(environment));
        return paths;
    }

    /**
     * Reloads the portion of this configuration that makes use of the modified file
     */
    public void reload(Path file, @Nullable Environment environment) {
        if (keyConfig() == trustConfig()) {
            keyConfig().reload(environment);
            return;
        }

        for (Path path : keyConfig().filesToMonitor(environment)) {
            if (file.equals(path)) {
                keyConfig().reload(environment);
                break;
            }
        }

        for (Path path : trustConfig().filesToMonitor(environment)) {
            if (file.equals(path)) {
                trustConfig().reload(environment);
                break;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SSLConfiguration)) return false;

        SSLConfiguration that = (SSLConfiguration) o;

        if (this.sessionCacheSize() != that.sessionCacheSize()) {
            return false;
        }
        if (this.keyConfig() != null ? !this.keyConfig().equals(that.keyConfig()) : that.keyConfig() != null) {
            return false;
        }
        if (this.trustConfig() != null ? !this.trustConfig().equals(that.trustConfig()) : that.trustConfig() != null) {
            return false;
        }
        if (this.protocol() != null ? !this.protocol().equals(that.protocol()) : that.protocol() != null) {
            return false;
        }
        if (this.sessionCacheTimeout() != null ?
                !this.sessionCacheTimeout().equals(that.sessionCacheTimeout()) : that.sessionCacheTimeout() != null) {
            return false;
        }
        if (this.ciphers() != null ? !this.ciphers().equals(that.ciphers()) : that.ciphers() != null) {
            return false;
        }
        return this.supportedProtocols() != null ?
                this.supportedProtocols().equals(that.supportedProtocols()) : that.supportedProtocols() == null;
    }

    @Override
    public int hashCode() {
        int result = this.keyConfig() != null ? this.keyConfig().hashCode() : 0;
        result = 31 * result + (this.trustConfig() != null ? this.trustConfig().hashCode() : 0);
        result = 31 * result + (this.protocol() != null ? this.protocol().hashCode() : 0);
        result = 31 * result + this.sessionCacheSize();
        result = 31 * result + (this.sessionCacheTimeout() != null ? this.sessionCacheTimeout().hashCode() : 0);
        result = 31 * result + (this.ciphers() != null ? this.ciphers().hashCode() : 0);
        result = 31 * result + (this.supportedProtocols() != null ? this.supportedProtocols().hashCode() : 0);
        return result;
    }

    public static class Global extends SSLConfiguration {

        public static final List<String> DEFAULT_SUPPORTED_PROTOCOLS = Arrays.asList("TLSv1", "TLSv1.1", "TLSv1.2");
        public static final List<String> DEFAULT_CIPHERS =
                Arrays.asList("TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
        public static final TimeValue DEFAULT_SESSION_CACHE_TIMEOUT = TimeValue.timeValueHours(24);
        public static final int DEFAULT_SESSION_CACHE_SIZE = 1000;
        public static final String DEFAULT_PROTOCOL = "TLSv1.2";

        // common settings
        static final Setting<List<String>> CIPHERS_SETTING = Setting.listSetting(globalKey(Custom.CIPHERS_SETTING), DEFAULT_CIPHERS,
                Function.identity(), Property.NodeScope, Property.Filtered);
        static final Setting<List<String>> SUPPORTED_PROTOCOLS_SETTING = Setting.listSetting(globalKey(Custom.SUPPORTED_PROTOCOLS_SETTING),
                DEFAULT_SUPPORTED_PROTOCOLS, Function.identity(), Property.NodeScope, Property.Filtered);
        static final Setting<String> PROTOCOL_SETTING = new Setting<>(globalKey(Custom.PROTOCOL_SETTING), DEFAULT_PROTOCOL,
                Function.identity(), Property.NodeScope, Property.Filtered);
        static final Setting<Integer> SESSION_CACHE_SIZE_SETTING = Setting.intSetting(globalKey(Custom.CACHE_SIZE_SETTING),
                DEFAULT_SESSION_CACHE_SIZE, Property.NodeScope, Property.Filtered);
        static final Setting<TimeValue> SESSION_CACHE_TIMEOUT_SETTING = Setting.timeSetting(globalKey(Custom.CACHE_TIMEOUT_SETTING),
                DEFAULT_SESSION_CACHE_TIMEOUT, Property.NodeScope, Property.Filtered);
        static final Setting<Boolean> RELOAD_ENABLED_SETTING =
                Setting.boolSetting(globalKey(Custom.RELOAD_ENABLED_SETTING), true, Property.NodeScope, Property.Filtered);

        // keystore settings
        static final Setting<Optional<String>> KEYSTORE_PATH_SETTING = createString(globalKey(Custom.KEYSTORE_PATH_SETTING),
                s -> System.getProperty("javax.net.ssl.keyStore"), Property.NodeScope, Property.Filtered);
        static final Setting<Optional<String>> KEYSTORE_PASSWORD_SETTING = createString(globalKey(Custom.KEYSTORE_PASSWORD_SETTING),
                        s -> System.getProperty("javax.net.ssl.keyStorePassword"), Property.NodeScope, Property.Filtered);
        static final Setting<String> KEYSTORE_ALGORITHM_SETTING = new Setting<>(globalKey(Custom.KEYSTORE_ALGORITHM_SETTING),
                        s -> System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm()),
                        Function.identity(), Property.NodeScope, Property.Filtered);
        static final Setting<Optional<String>> KEYSTORE_KEY_PASSWORD_SETTING =
                createString(globalKey(Custom.KEYSTORE_KEY_PASSWORD_SETTING), KEYSTORE_PASSWORD_SETTING,
                        Property.NodeScope, Property.Filtered);

        // truststore settings
        static final Setting<Optional<String>> TRUSTSTORE_PATH_SETTING = createString(globalKey(Custom.TRUSTSTORE_PATH_SETTING),
                s -> System.getProperty("javax.net.ssl.trustStore"), Property.NodeScope, Property.Filtered);
        static final Setting<Optional<String>> TRUSTSTORE_PASSWORD_SETTING = createString(globalKey(Custom.TRUSTSTORE_PASSWORD_SETTING),
                s -> System.getProperty("javax.net.ssl.trustStorePassword"), Property.NodeScope, Property.Filtered);
        static final Setting<String> TRUSTSTORE_ALGORITHM_SETTING = new Setting<>(globalKey(Custom.TRUSTSTORE_ALGORITHM_SETTING),
                        s -> System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm()),
                        Function.identity(), Property.NodeScope, Property.Filtered);

        // PEM key and cert settings
        static final Setting<Optional<String>> KEY_PATH_SETTING = createString(globalKey(Custom.KEY_PATH_SETTING),
                Property.NodeScope, Property.Filtered);
        static final Setting<Optional<String>> KEY_PASSWORD_SETTING = createString(globalKey(Custom.KEY_PASSWORD_SETTING),
                Property.NodeScope, Property.Filtered);
        static final Setting<List<String>> CERT_SETTING = Setting.listSetting(globalKey(Custom.CERT_SETTING), Collections.emptyList(),
                s -> s, Property.NodeScope, Property.Filtered);

        // PEM trusted certs
        static final Setting<List<String>> CA_PATHS_SETTING = Setting.listSetting(globalKey(Custom.CA_PATHS_SETTING),
                Collections.emptyList(), s -> s, Property.NodeScope, Property.Filtered);

        // Default system trusted certs
        static final Setting<Boolean> INCLUDE_JDK_CERTS_SETTING = Setting.boolSetting(globalKey(Custom.INCLUDE_JDK_CERTS_SETTING), true,
                Property.NodeScope, Property.Filtered);

        public static void addSettings(List<Setting<?>> settings) {
            settings.add(Global.CIPHERS_SETTING);
            settings.add(Global.SUPPORTED_PROTOCOLS_SETTING);
            settings.add(Global.KEYSTORE_PATH_SETTING);
            settings.add(Global.KEYSTORE_PASSWORD_SETTING);
            settings.add(Global.KEYSTORE_ALGORITHM_SETTING);
            settings.add(Global.KEYSTORE_KEY_PASSWORD_SETTING);
            settings.add(Global.KEY_PATH_SETTING);
            settings.add(Global.KEY_PASSWORD_SETTING);
            settings.add(Global.CERT_SETTING);
            settings.add(Global.TRUSTSTORE_PATH_SETTING);
            settings.add(Global.TRUSTSTORE_PASSWORD_SETTING);
            settings.add(Global.TRUSTSTORE_ALGORITHM_SETTING);
            settings.add(Global.PROTOCOL_SETTING);
            settings.add(Global.SESSION_CACHE_SIZE_SETTING);
            settings.add(Global.SESSION_CACHE_TIMEOUT_SETTING);
            settings.add(Global.CA_PATHS_SETTING);
            settings.add(Global.INCLUDE_JDK_CERTS_SETTING);
            settings.add(Global.RELOAD_ENABLED_SETTING);
        }

        private final KeyConfig keyConfig;
        private final TrustConfig trustConfig;
        private final String sslProtocol;
        private final int sessionCacheSize;
        private final TimeValue sessionCacheTimeout;
        private final List<String> ciphers;
        private final List<String> supportedProtocols;

        /**
         * This constructor should be used with the global settings of the service
         *
         * @param settings the global settings to build the SSL configuration from
         */
        public Global(Settings settings) {
            this.keyConfig = createGlobalKeyConfig(settings);
            this.trustConfig = createGlobalTrustConfig(settings, keyConfig);
            this.sslProtocol = PROTOCOL_SETTING.get(settings);
            this.sessionCacheSize = SESSION_CACHE_SIZE_SETTING.get(settings);
            this.sessionCacheTimeout = SESSION_CACHE_TIMEOUT_SETTING.get(settings);
            this.ciphers = CIPHERS_SETTING.get(settings);
            this.supportedProtocols = SUPPORTED_PROTOCOLS_SETTING.get(settings);
        }

        @Override
        public KeyConfig keyConfig() {
            return keyConfig;
        }

        @Override
        public TrustConfig trustConfig() {
            return trustConfig;
        }

        @Override
        public String protocol() {
            return sslProtocol;
        }

        @Override
        public int sessionCacheSize() {
            return sessionCacheSize;
        }

        @Override
        public TimeValue sessionCacheTimeout() {
            return sessionCacheTimeout;
        }

        @Override
        public List<String> ciphers() {
            return ciphers;
        }

        @Override
        public List<String> supportedProtocols() {
            return supportedProtocols;
        }

        @Override
        public String toString() {
            return "SSLConfiguration{" +
                    ", keyConfig=[" + keyConfig +
                    "], trustConfig=" + trustConfig +
                    "], sslProtocol=['" + sslProtocol + '\'' +
                    "], sessionCacheSize=[" + sessionCacheSize +
                    "], sessionCacheTimeout=[" + sessionCacheTimeout +
                    "], ciphers=[" + ciphers +
                    "], supportedProtocols=[" + supportedProtocols +
                    "]}";
        }

        private static String globalKey(Setting setting) {
            return setting("ssl." + setting.getKey());
        }

        static KeyConfig createGlobalKeyConfig(Settings settings) {
            String keyStorePath = KEYSTORE_PATH_SETTING.get(settings).orElse(null);
            String keyPath = KEY_PATH_SETTING.get(settings).orElse(null);
            if (keyPath != null && keyStorePath != null) {
                throw new IllegalArgumentException("you cannot specify a keystore and key file");
            } else if (keyStorePath == null && keyPath == null) {
                return KeyConfig.NONE;
            }

            boolean includeSystem = INCLUDE_JDK_CERTS_SETTING.get(settings);
            boolean reloadEnabled = RELOAD_ENABLED_SETTING.get(settings);
            if (keyPath != null) {
                String keyPassword = KEY_PASSWORD_SETTING.get(settings).orElse(null);
                List<String> certPaths = getListOrNull(CERT_SETTING, settings);
                if (certPaths == null) {
                    throw new IllegalArgumentException("you must specify the certificates to use with the key");
                }
                return new PEMKeyConfig(includeSystem, keyPath, keyPassword, certPaths);
            } else {
                assert keyStorePath != null;
                String keyStorePassword = KEYSTORE_PASSWORD_SETTING.get(settings).orElse(null);
                String keyStoreAlgorithm = KEYSTORE_ALGORITHM_SETTING.get(settings);
                String keyStoreKeyPassword = KEYSTORE_KEY_PASSWORD_SETTING.get(settings).orElse(keyStorePassword);
                String trustStoreAlgorithm = TRUSTSTORE_ALGORITHM_SETTING.get(settings);
                return new StoreKeyConfig(includeSystem, keyStorePath, keyStorePassword, keyStoreKeyPassword,
                        keyStoreAlgorithm, trustStoreAlgorithm);
            }
        }

        static TrustConfig createGlobalTrustConfig(Settings settings, KeyConfig keyInfo) {
            String trustStorePath = TRUSTSTORE_PATH_SETTING.get(settings).orElse(null);
            List<String> caPaths = getListOrNull(CA_PATHS_SETTING, settings);
            boolean includeSystem = INCLUDE_JDK_CERTS_SETTING.get(settings);
            if (trustStorePath != null && caPaths != null) {
                throw new IllegalArgumentException("you cannot specify a truststore and ca files");
            } else if (caPaths != null) {
                return new PEMTrustConfig(includeSystem, caPaths);
            } else if (trustStorePath != null) {
                String trustStorePassword = TRUSTSTORE_PASSWORD_SETTING.get(settings).orElse(null);
                String trustStoreAlgorithm = TRUSTSTORE_ALGORITHM_SETTING.get(settings);
                return new StoreTrustConfig(includeSystem, trustStorePath, trustStorePassword, trustStoreAlgorithm);
            } else if (keyInfo != KeyConfig.NONE) {
                return keyInfo;
            } else {
                return new StoreTrustConfig(includeSystem, null, null, null);
            }
        }
    }

    public static class Custom extends SSLConfiguration {

        static final Setting<Optional<String>> PROTOCOL_SETTING = createString("protocol");
        static final Setting<Optional<Integer>> CACHE_SIZE_SETTING = createInt("session.cache_size");
        static final Setting<Optional<TimeValue>> CACHE_TIMEOUT_SETTING = createTimeValue("session.cache_timeout");
        static final Setting<List<String>> CIPHERS_SETTING = Setting.listSetting("ciphers", Collections.emptyList(), s -> s);
        static final Setting<List<String>> SUPPORTED_PROTOCOLS_SETTING =
                Setting.listSetting("supported_protocols", Collections.emptyList(), s -> s);

        static final Setting<Optional<String>> KEYSTORE_PATH_SETTING = createString("keystore.path");
        static final Setting<Optional<String>> KEYSTORE_PASSWORD_SETTING = createString("keystore.password");
        static final Setting<String> KEYSTORE_ALGORITHM_SETTING = new Setting<>("keystore.algorithm",
                s -> System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm()), Function.identity());
        static final Setting<Optional<String>> KEYSTORE_KEY_PASSWORD_FALLBACK = createString("keystore.password");
        static final Setting<Optional<String>> KEYSTORE_KEY_PASSWORD_SETTING =
                createString("keystore.key_password", KEYSTORE_KEY_PASSWORD_FALLBACK);


        static final Setting<Optional<String>> TRUSTSTORE_PATH_SETTING = createString("truststore.path");
        static final Setting<Optional<String>> TRUSTSTORE_PASSWORD_SETTING = createString("truststore.password");
        static final Setting<String> TRUSTSTORE_ALGORITHM_SETTING = new Setting<>("truststore.algorithm",
                s -> System.getProperty("ssl.TrustManagerFactory.algorithm",
                        TrustManagerFactory.getDefaultAlgorithm()), Function.identity());

        static final Setting<Optional<String>> KEY_PATH_SETTING = createString("key.path");
        static final Setting<Optional<String>> KEY_PASSWORD_SETTING = createString("key.password");
        static final Setting<List<String>> CERT_SETTING = Setting.listSetting("cert", Collections.emptyList(), s -> s);

        static final Setting<List<String>> CA_PATHS_SETTING = Setting.listSetting("ca", Collections.emptyList(), s -> s);
        static final Setting<Boolean> INCLUDE_JDK_CERTS_SETTING = Setting.boolSetting("trust_cacerts", true);
        static final Setting<Boolean> RELOAD_ENABLED_SETTING = Setting.boolSetting("reload.enabled", true);

        private final KeyConfig keyConfig;
        private final TrustConfig trustConfig;
        private final String sslProtocol;
        private final int sessionCacheSize;
        private final TimeValue sessionCacheTimeout;
        private final List<String> ciphers;
        private final List<String> supportedProtocols;

        /**
         * The settings passed in should be the group settings under ssl, like xpack.security.ssl
         *
         * @param settings the profile settings to get the SSL configuration for
         * @param defaultConfig   the default SSL configuration
         */
        public Custom(Settings settings, SSLConfiguration defaultConfig) {
            Objects.requireNonNull(settings);
            this.keyConfig = createKeyConfig(settings, defaultConfig);
            this.trustConfig = createTrustConfig(settings, keyConfig, defaultConfig);
            this.sslProtocol = PROTOCOL_SETTING.get(settings).orElse(defaultConfig.protocol());
            this.sessionCacheSize = CACHE_SIZE_SETTING.get(settings).orElse(defaultConfig.sessionCacheSize());
            this.sessionCacheTimeout = CACHE_TIMEOUT_SETTING.get(settings).orElse(defaultConfig.sessionCacheTimeout());
            this.ciphers = getListOrDefault(CIPHERS_SETTING, settings, defaultConfig.ciphers());
            this.supportedProtocols = getListOrDefault(SUPPORTED_PROTOCOLS_SETTING, settings, defaultConfig.supportedProtocols());
        }

        @Override
        public KeyConfig keyConfig() {
            return keyConfig;
        }

        @Override
        public TrustConfig trustConfig() {
            return trustConfig;
        }

        @Override
        public String protocol() {
            return sslProtocol;
        }

        @Override
        public int sessionCacheSize() {
            return sessionCacheSize;
        }

        @Override
        public TimeValue sessionCacheTimeout() {
            return sessionCacheTimeout;
        }

        @Override
        public List<String> ciphers() {
            return ciphers;
        }

        @Override
        public List<String> supportedProtocols() {
            return supportedProtocols;
        }

        @Override
        public String toString() {
            return "SSLConfiguration{" +
                    ", keyConfig=[" + keyConfig +
                    "], trustConfig=" + trustConfig +
                    "], sslProtocol=['" + sslProtocol + '\'' +
                    "], sessionCacheSize=[" + sessionCacheSize +
                    "], sessionCacheTimeout=[" + sessionCacheTimeout +
                    "], ciphers=[" + ciphers +
                    "], supportedProtocols=[" + supportedProtocols +
                    '}';
        }

        static KeyConfig createKeyConfig(Settings settings, SSLConfiguration global) {
            String keyStorePath = KEYSTORE_PATH_SETTING.get(settings).orElse(null);
            String keyPath = KEY_PATH_SETTING.get(settings).orElse(null);
            if (keyPath != null && keyStorePath != null) {
                throw new IllegalArgumentException("you cannot specify a keystore and key file");
            } else if (keyStorePath == null && keyPath == null) {
                return global.keyConfig();
            }

            boolean includeSystem = INCLUDE_JDK_CERTS_SETTING.get(settings);
            boolean reloadEnabled = RELOAD_ENABLED_SETTING.get(settings);
            if (keyPath != null) {
                String keyPassword = KEY_PASSWORD_SETTING.get(settings).orElse(null);
                List<String> certPaths = getListOrNull(CERT_SETTING, settings);
                if (certPaths == null) {
                    throw new IllegalArgumentException("you must specify the certificates to use with the key");
                }
                return new PEMKeyConfig(includeSystem, keyPath, keyPassword, certPaths);
            } else {
                assert keyStorePath != null;
                String keyStorePassword = KEYSTORE_PASSWORD_SETTING.get(settings).orElse(null);
                String keyStoreAlgorithm = KEYSTORE_ALGORITHM_SETTING.get(settings);
                String keyStoreKeyPassword = KEYSTORE_KEY_PASSWORD_SETTING.get(settings).orElse(keyStorePassword);
                String trustStoreAlgorithm = TRUSTSTORE_ALGORITHM_SETTING.get(settings);
                return new StoreKeyConfig(includeSystem, keyStorePath, keyStorePassword, keyStoreKeyPassword,
                        keyStoreAlgorithm, trustStoreAlgorithm);
            }
        }

        static TrustConfig createTrustConfig(Settings settings, KeyConfig keyConfig, SSLConfiguration global) {
            String trustStorePath = TRUSTSTORE_PATH_SETTING.get(settings).orElse(null);
            List<String> caPaths = getListOrNull(CA_PATHS_SETTING, settings);
            if (trustStorePath != null && caPaths != null) {
                throw new IllegalArgumentException("you cannot specify a truststore and ca files");
            } else if (caPaths != null) {
                return new PEMTrustConfig(INCLUDE_JDK_CERTS_SETTING.get(settings), caPaths);
            } else if (trustStorePath != null) {
                String trustStorePassword = TRUSTSTORE_PASSWORD_SETTING.get(settings).orElse(null);
                String trustStoreAlgorithm = TRUSTSTORE_ALGORITHM_SETTING.get(settings);
                return new StoreTrustConfig(INCLUDE_JDK_CERTS_SETTING.get(settings),
                        trustStorePath, trustStorePassword, trustStoreAlgorithm);
            } else if (keyConfig == global.keyConfig()) {
                return global.trustConfig();
            } else {
                return keyConfig;
            }
        }
    }

    static List<String> getListOrNull(Setting<List<String>> listSetting, Settings settings) {
        return getListOrDefault(listSetting, settings, null);
    }

    static List<String> getListOrDefault(Setting<List<String>> listSetting, Settings settings, List<String> defaultValue) {
        if (listSetting.exists(settings)) {
            return listSetting.get(settings);
        }
        return defaultValue;
    }
}
