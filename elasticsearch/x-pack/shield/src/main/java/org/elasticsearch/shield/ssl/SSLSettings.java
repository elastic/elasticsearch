/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.TimeValue;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.elasticsearch.shield.Security.setting;
import static org.elasticsearch.shield.support.OptionalStringSetting.create;

/**
 * Class that contains all settings related to SSL
 */
public class SSLSettings {

    public interface Globals {
        List<String> DEFAULT_SUPPORTED_PROTOCOLS = Arrays.asList("TLSv1", "TLSv1.1", "TLSv1.2");
        List<String> DEFAULT_CIPHERS =
                Arrays.asList("TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
        TimeValue DEFAULT_SESSION_CACHE_TIMEOUT = TimeValue.timeValueHours(24);
        int DEFAULT_SESSION_CACHE_SIZE = 1000;
        String DEFAULT_PROTOCOL = "TLSv1.2";

        Setting<List<String>> CIPHERS_SETTING =
                Setting.listSetting(setting("ssl.ciphers"), DEFAULT_CIPHERS, Function.identity(), Property.NodeScope, Property.Filtered);
        Setting<List<String>> SUPPORTED_PROTOCOLS_SETTING =
                Setting.listSetting(setting("ssl.supported_protocols"), DEFAULT_SUPPORTED_PROTOCOLS,
                        Function.identity(), Property.NodeScope, Property.Filtered);
        Setting<Optional<String>> KEYSTORE_PATH_SETTING = create(setting("ssl.keystore.path"),
                s -> System.getProperty("javax.net.ssl.keyStore"), Property.NodeScope, Property.Filtered);
        Setting<Optional<String>> KEYSTORE_PASSWORD_SETTING = create(setting("ssl.keystore.password"),
                        s -> System.getProperty("javax.net.ssl.keyStorePassword"), Property.NodeScope, Property.Filtered);
        Setting<String> KEYSTORE_ALGORITHM_SETTING =
                new Setting<>(setting("ssl.keystore.algorithm"),
                        s -> System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm()),
                        Function.identity(), Property.NodeScope, Property.Filtered);
        Setting<Optional<String>> KEYSTORE_KEY_PASSWORD_SETTING = create(setting("ssl.keystore.key_password"), KEYSTORE_PASSWORD_SETTING,
                        Property.NodeScope, Property.Filtered);
        Setting<Optional<String>> TRUSTSTORE_PATH_SETTING = create(setting("ssl.truststore.path"),
                s -> System.getProperty("javax.net.ssl.trustStore"), Property.NodeScope, Property.Filtered);
        Setting<Optional<String>> TRUSTSTORE_PASSWORD_SETTING = create(setting("ssl.truststore.password"),
                s -> System.getProperty("javax.net.ssl.trustStorePassword"), Property.NodeScope, Property.Filtered);
        Setting<String> TRUSTSTORE_ALGORITHM_SETTING =
                new Setting<>(setting("ssl.truststore.algorithm"),
                        s -> System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm()),
                        Function.identity(), Property.NodeScope, Property.Filtered);
        Setting<String> PROTOCOL_SETTING =
                new Setting<>(setting("ssl.protocol"), DEFAULT_PROTOCOL, Function.identity(), Property.NodeScope, Property.Filtered);
        Setting<Integer> SESSION_CACHE_SIZE_SETTING =
                Setting.intSetting(setting("ssl.session.cache_size"), DEFAULT_SESSION_CACHE_SIZE, Property.NodeScope, Property.Filtered);
        Setting<TimeValue> SESSION_CACHE_TIMEOUT_SETTING =
                Setting.timeSetting(setting("ssl.session.cache_timeout"), DEFAULT_SESSION_CACHE_TIMEOUT,
                        Property.NodeScope, Property.Filtered);
    }

    private static final ESLogger logger = Loggers.getLogger(SSLSettings.class);

    static Setting<Optional<String>> KEYSTORE_PATH_SETTING = create("keystore.path", Globals.KEYSTORE_PATH_SETTING);
    static Setting<Optional<String>> KEYSTORE_PASSWORD_SETTING = create("keystore.password", Globals.KEYSTORE_PASSWORD_SETTING);
    static Setting<String> KEYSTORE_ALGORITHM_SETTING =
            new Setting<>("keystore.algorithm", Globals.KEYSTORE_ALGORITHM_SETTING, s -> s);

    //key password fallback should be keystore.key_password -> keystore.password -> global keystore.key_pasword -> global keystore.password
    static Setting<Optional<String>> KEY_PASSWORD_FALLBACK = create("keystore.password", Globals.KEYSTORE_KEY_PASSWORD_SETTING);
    static Setting<Optional<String>> KEY_PASSWORD_SETTING = create("keystore.key_password", KEY_PASSWORD_FALLBACK);

    static Setting<Optional<String>> TRUSTSTORE_PATH_SETTING = create("truststore.path", Globals.TRUSTSTORE_PATH_SETTING);
    static Setting<Optional<String>> TRUSTSTORE_PASSWORD_SETTING = create("truststore.password", Globals.TRUSTSTORE_PASSWORD_SETTING);
    static Setting<String> TRUSTSTORE_ALGORITHM_SETTING =
            new Setting<>("truststore.algorithm", Globals.TRUSTSTORE_ALGORITHM_SETTING, s -> s);
    static Setting<String> PROTOCOL_SETTING =
            new Setting<>("protocol", Globals.PROTOCOL_SETTING, s -> s);
    static Setting<Integer> CACHE_SIZE_SETTING =
            new Setting<>("session.cache_size", Globals.SESSION_CACHE_SIZE_SETTING, Integer::parseInt);
    static Setting<TimeValue> CACHE_TIMEOUT_SETTING =
            Setting.timeSetting("session.cache_timeout", Globals.SESSION_CACHE_TIMEOUT_SETTING);

    String keyStorePath;
    String keyStorePassword;
    String keyStoreAlgorithm;
    String keyPassword;
    String trustStorePath;
    String trustStorePassword;
    String trustStoreAlgorithm;
    String sslProtocol;
    int sessionCacheSize;
    TimeValue sessionCacheTimeout;

    SSLSettings(Settings settings, Settings sslServiceSettings) {
        keyStorePath = getStringOrNull(KEYSTORE_PATH_SETTING, settings, sslServiceSettings);
        keyStorePassword = getStringOrNull(KEYSTORE_PASSWORD_SETTING, settings, sslServiceSettings);
        keyStoreAlgorithm = KEYSTORE_ALGORITHM_SETTING.get(settings, sslServiceSettings);
        keyPassword = getStringOrNull(KEY_PASSWORD_SETTING, settings, sslServiceSettings);

        // Truststore settings
        trustStorePath = getStringOrNull(TRUSTSTORE_PATH_SETTING, settings, sslServiceSettings);
        trustStorePassword = getStringOrNull(TRUSTSTORE_PASSWORD_SETTING, settings, sslServiceSettings);
        trustStoreAlgorithm = TRUSTSTORE_ALGORITHM_SETTING.get(settings, sslServiceSettings);

        sslProtocol = PROTOCOL_SETTING.get(settings, sslServiceSettings);
        sessionCacheSize = CACHE_SIZE_SETTING.get(settings, sslServiceSettings);
        sessionCacheTimeout = CACHE_TIMEOUT_SETTING.get(settings, sslServiceSettings);

        if (trustStorePath == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("no truststore defined. using keystore [{}] as truststore", keyStorePath);
            }
            trustStorePath = keyStorePath;
            trustStorePassword = keyStorePassword;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SSLSettings that = (SSLSettings) o;

        if (keyStorePath != null ? !keyStorePath.equals(that.keyStorePath) : that.keyStorePath != null) {
            return false;
        }
        if (sslProtocol != null ? !sslProtocol.equals(that.sslProtocol) : that.sslProtocol != null) {
            return false;
        }
        if (trustStorePath != null ? !trustStorePath.equals(that.trustStorePath) : that.trustStorePath != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = keyStorePath != null ? keyStorePath.hashCode() : 0;
        result = 31 * result + (trustStorePath != null ? trustStorePath.hashCode() : 0);
        result = 31 * result + (sslProtocol != null ? sslProtocol.hashCode() : 0);
        return result;
    }

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(Globals.CIPHERS_SETTING);
        settingsModule.registerSetting(Globals.SUPPORTED_PROTOCOLS_SETTING);
        settingsModule.registerSetting(Globals.KEYSTORE_PATH_SETTING);
        settingsModule.registerSetting(Globals.KEYSTORE_PASSWORD_SETTING);
        settingsModule.registerSetting(Globals.KEYSTORE_ALGORITHM_SETTING);
        settingsModule.registerSetting(Globals.KEYSTORE_KEY_PASSWORD_SETTING);
        settingsModule.registerSetting(Globals.TRUSTSTORE_PATH_SETTING);
        settingsModule.registerSetting(Globals.TRUSTSTORE_PASSWORD_SETTING);
        settingsModule.registerSetting(Globals.TRUSTSTORE_ALGORITHM_SETTING);
        settingsModule.registerSetting(Globals.PROTOCOL_SETTING);
        settingsModule.registerSetting(Globals.SESSION_CACHE_SIZE_SETTING);
        settingsModule.registerSetting(Globals.SESSION_CACHE_TIMEOUT_SETTING);
    }

    private static String getStringOrNull(Setting<Optional<String>> setting, Settings settings, Settings fallbackSettings) {
        // for settings with complicated fallback we need to try to get it first, if not then try the fallback settings
        Optional<String> optional = setting.get(settings);
        return optional.orElse(setting.get(fallbackSettings).orElse(null));
    }
}
