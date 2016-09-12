/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.ssl.SSLClientAuth;
import org.elasticsearch.xpack.ssl.VerificationMode;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import static java.util.Collections.emptyList;

/**
 * A container for xpack setting constants.
 */
public class XPackSettings {
    /** All setting constants created in this class. */
    private static final List<Setting<?>> ALL_SETTINGS = new ArrayList<>();

    /** Setting for enabling or disabling security. Defaults to true. */
    public static final Setting<Boolean> SECURITY_ENABLED = enabledSetting(XPackPlugin.SECURITY, true);

    /** Setting for enabling or disabling monitoring. Defaults to true if not a tribe node. */
    public static final Setting<Boolean> MONITORING_ENABLED = enabledSetting(XPackPlugin.MONITORING,
        // By default, monitoring is disabled on tribe nodes
        s -> String.valueOf(XPackPlugin.isTribeNode(s) == false && XPackPlugin.isTribeClientNode(s) == false));

    /** Setting for enabling or disabling watcher. Defaults to true. */
    public static final Setting<Boolean> WATCHER_ENABLED = enabledSetting(XPackPlugin.WATCHER, true);

    /** Setting for enabling or disabling graph. Defaults to true. */
    public static final Setting<Boolean> GRAPH_ENABLED = enabledSetting(XPackPlugin.GRAPH, true);

    /** Setting for enabling or disabling auditing. Defaults to false. */
    public static final Setting<Boolean> AUDIT_ENABLED = enabledSetting(XPackPlugin.SECURITY + ".audit", false);

    /** Setting for enabling or disabling document/field level security. Defaults to true. */
    public static final Setting<Boolean> DLS_FLS_ENABLED = enabledSetting(XPackPlugin.SECURITY + ".dls_fls", true);

    /** Setting for enabling or disabling transport ssl. Defaults to false. */
    public static final Setting<Boolean> TRANSPORT_SSL_ENABLED = enabledSetting(XPackPlugin.SECURITY + ".transport.ssl", false);

    /** Setting for enabling or disabling http ssl. Defaults to false. */
    public static final Setting<Boolean> HTTP_SSL_ENABLED = enabledSetting(XPackPlugin.SECURITY + ".http.ssl", false);

    /** Setting for enabling or disabling the reserved realm. Defaults to true */
    public static final Setting<Boolean> RESERVED_REALM_ENABLED_SETTING =
            enabledSetting(XPackPlugin.SECURITY + ".authc.reserved_realm", true);

    /*
     * SSL settings. These are the settings that are specifically registered for SSL. Many are private as we do not explicitly use them
     * but instead parse based on a prefix (eg *.ssl.*)
     */
    public static final List<String> DEFAULT_CIPHERS =
            Arrays.asList("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
                    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA256",
                    "TLS_RSA_WITH_AES_128_CBC_SHA");
    public static final List<String> DEFAULT_SUPPORTED_PROTOCOLS = Arrays.asList("TLSv1.2", "TLSv1.1", "TLSv1");
    public static final SSLClientAuth CLIENT_AUTH_DEFAULT = SSLClientAuth.REQUIRED;
    public static final SSLClientAuth HTTP_CLIENT_AUTH_DEFAULT = SSLClientAuth.NONE;
    public static final VerificationMode VERIFICATION_MODE_DEFAULT = VerificationMode.FULL;

    // global settings that apply to everything!
    private static final Setting<List<String>> CIPHERS_SETTING = Setting.listSetting("xpack.ssl.cipher_suites", DEFAULT_CIPHERS,
            Function.identity(), Property.NodeScope, Property.Filtered);
    private static final Setting<List<String>> SUPPORTED_PROTOCOLS_SETTING = Setting.listSetting("xpack.ssl.supported_protocols",
            DEFAULT_SUPPORTED_PROTOCOLS, Function.identity(), Property.NodeScope, Property.Filtered);
    private static final Setting<SSLClientAuth> CLIENT_AUTH_SETTING = new Setting<>("xpack.ssl.client_authentication",
            CLIENT_AUTH_DEFAULT.name(), SSLClientAuth::parse, Property.NodeScope, Property.Filtered);
    private static final Setting<VerificationMode> VERIFICATION_MODE_SETTING = new Setting<>("xpack.ssl.verification_mode",
            VERIFICATION_MODE_DEFAULT.name(), VerificationMode::parse, Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> KEYSTORE_PATH_SETTING = new Setting<>("xpack.ssl.keystore.path",
            s -> System.getProperty("javax.net.ssl.keyStore"), Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> KEYSTORE_PASSWORD_SETTING = new Setting<>("xpack.ssl.keystore.password",
            s -> System.getProperty("javax.net.ssl.keyStorePassword"), Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<String> KEYSTORE_ALGORITHM_SETTING = new Setting<>("xpack.ssl.keystore.algorithm",
            s -> System.getProperty("ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm()),
            Function.identity(), Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> KEYSTORE_KEY_PASSWORD_SETTING =
            new Setting<>("xpack.ssl.keystore.key_password", KEYSTORE_PASSWORD_SETTING, Optional::ofNullable,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> TRUSTSTORE_PATH_SETTING = new Setting<>("xpack.ssl.truststore.path",
            s -> System.getProperty("javax.net.ssl.trustStore"), Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> TRUSTSTORE_PASSWORD_SETTING = new Setting<>("xpack.ssl.truststore.password",
            s -> System.getProperty("javax.net.ssl.trustStorePassword"), Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<String> TRUSTSTORE_ALGORITHM_SETTING = new Setting<>("xpack.ssl.truststore.algorithm",
            s -> System.getProperty("ssl.TrustManagerFactory.algorithm", TrustManagerFactory.getDefaultAlgorithm()),
            Function.identity(), Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> KEY_PATH_SETTING =
            new Setting<>("xpack.ssl.key", (String) null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> KEY_PASSWORD_SETTING =
            new Setting<>("xpack.ssl.key_passphrase", (String) null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> CERT_SETTING =
            new Setting<>("xpack.ssl.certificate", (String) null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<List<String>> CA_PATHS_SETTING = Setting.listSetting("xpack.ssl.certificate_authorities",
            Collections.emptyList(), s -> s, Property.NodeScope, Property.Filtered);

    // http specific settings
    private static final Setting<List<String>> HTTP_CIPHERS_SETTING = Setting.listSetting("xpack.security.http.ssl.cipher_suites",
            DEFAULT_CIPHERS, Function.identity(), Property.NodeScope, Property.Filtered);
    private static final Setting<List<String>> HTTP_SUPPORTED_PROTOCOLS_SETTING =
            Setting.listSetting("xpack.security.http.ssl.supported_protocols", emptyList(), Function.identity(),
                    Property.NodeScope, Property.Filtered);
    private static final Setting<SSLClientAuth> HTTP_CLIENT_AUTH_SETTING = new Setting<>("xpack.security.http.ssl.client_authentication",
            CLIENT_AUTH_DEFAULT.name(), SSLClientAuth::parse, Property.NodeScope, Property.Filtered);
    private static final Setting<VerificationMode> HTTP_VERIFICATION_MODE_SETTING =
            new Setting<>("xpack.security.http.ssl.verification_mode", VERIFICATION_MODE_DEFAULT.name(), VerificationMode::parse,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> HTTP_KEYSTORE_PATH_SETTING = new Setting<>("xpack.security.http.ssl.keystore.path",
            (String) null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> HTTP_KEYSTORE_PASSWORD_SETTING =
            new Setting<>("xpack.security.http.ssl.keystore.password", (String) null, Optional::ofNullable,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<String> HTTP_KEYSTORE_ALGORITHM_SETTING = new Setting<>("xpack.security.http.ssl.keystore.algorithm",
            "", Function.identity(), Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> HTTP_KEYSTORE_KEY_PASSWORD_SETTING =
            new Setting<>("xpack.security.http.ssl.keystore.key_password", HTTP_KEYSTORE_PASSWORD_SETTING, Optional::ofNullable,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> HTTP_TRUSTSTORE_PATH_SETTING = new Setting<>("xpack.security.http.ssl.truststore.path",
            (String) null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> HTTP_TRUSTSTORE_PASSWORD_SETTING =
            new Setting<>("xpack.security.http.ssl.truststore.password", (String) null, Optional::ofNullable,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<String> HTTP_TRUSTSTORE_ALGORITHM_SETTING = new Setting<>("xpack.security.http.ssl.truststore.algorithm",
            "", Function.identity(), Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> HTTP_KEY_PATH_SETTING =
            new Setting<>("xpack.security.http.ssl.key", (String) null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> HTTP_KEY_PASSWORD_SETTING = new Setting<>("xpack.security.http.ssl.key_passphrase",
            (String) null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> HTTP_CERT_SETTING = new Setting<>("xpack.security.http.ssl.certificate",
            (String) null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<List<String>> HTTP_CA_PATHS_SETTING =
            Setting.listSetting("xpack.security.http.ssl.certificate_authorities", emptyList(), s -> s,
                    Property.NodeScope, Property.Filtered);

    // transport specific settings
    private static final Setting<List<String>> TRANSPORT_CIPHERS_SETTING =
            Setting.listSetting("xpack.security.transport.ssl.cipher_suites", DEFAULT_CIPHERS, Function.identity(),
                    Property.NodeScope, Property.Filtered);
    private static final Setting<List<String>> TRANSPORT_SUPPORTED_PROTOCOLS_SETTING =
            Setting.listSetting("xpack.security.transport.ssl.supported_protocols", emptyList(), Function.identity(),
                    Property.NodeScope, Property.Filtered);
    private static final Setting<SSLClientAuth> TRANSPORT_CLIENT_AUTH_SETTING =
            new Setting<>("xpack.security.transport.ssl.client_authentication", CLIENT_AUTH_DEFAULT.name(), SSLClientAuth::parse,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<VerificationMode> TRANSPORT_VERIFICATION_MODE_SETTING =
            new Setting<>("xpack.security.transport.ssl.verification_mode", VERIFICATION_MODE_DEFAULT.name(), VerificationMode::parse,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> TRANSPORT_KEYSTORE_PATH_SETTING =
            new Setting<>("xpack.security.transport.ssl.keystore.path", (String) null, Optional::ofNullable,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> TRANSPORT_KEYSTORE_PASSWORD_SETTING =
            new Setting<>("xpack.security.transport.ssl.keystore.password", (String) null, Optional::ofNullable,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<String> TRANSPORT_KEYSTORE_ALGORITHM_SETTING =
            new Setting<>("xpack.security.transport.ssl.keystore.algorithm", "", Function.identity(), Property.NodeScope,
                    Property.Filtered);
    private static final Setting<Optional<String>> TRANSPORT_KEYSTORE_KEY_PASSWORD_SETTING =
            new Setting<>("xpack.security.transport.ssl.keystore.key_password", TRANSPORT_KEYSTORE_PASSWORD_SETTING, Optional::ofNullable,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> TRANSPORT_TRUSTSTORE_PATH_SETTING =
            new Setting<>("xpack.security.transport.ssl.truststore.path", (String) null, Optional::ofNullable,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> TRANSPORT_TRUSTSTORE_PASSWORD_SETTING =
            new Setting<>("xpack.security.transport.ssl.truststore.password", (String) null, Optional::ofNullable,
                    Property.NodeScope, Property.Filtered);
    private static final Setting<String> TRANSPORT_TRUSTSTORE_ALGORITHM_SETTING =
            new Setting<>("xpack.security.transport.ssl.truststore.algorithm", "", Function.identity(),
                    Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> TRANSPORT_KEY_PATH_SETTING =
            new Setting<>("xpack.security.transport.ssl.key", (String) null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<Optional<String>> TRANSPORT_KEY_PASSWORD_SETTING =
            new Setting<>("xpack.security.transport.ssl.key_passphrase", (String) null, Optional::ofNullable, Property.NodeScope,
                    Property.Filtered);
    private static final Setting<Optional<String>> TRANSPORT_CERT_SETTING = new Setting<>("xpack.security.transport.ssl.certificate",
            (String) null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
    private static final Setting<List<String>> TRANSPORT_CA_PATHS_SETTING =
            Setting.listSetting("xpack.security.transport.ssl.certificate_authorities", emptyList(), s -> s,
                    Property.NodeScope, Property.Filtered);
    /* End SSL settings */

    static {
        ALL_SETTINGS.add(CIPHERS_SETTING);
        ALL_SETTINGS.add(SUPPORTED_PROTOCOLS_SETTING);
        ALL_SETTINGS.add(KEYSTORE_PATH_SETTING);
        ALL_SETTINGS.add(KEYSTORE_PASSWORD_SETTING);
        ALL_SETTINGS.add(KEYSTORE_ALGORITHM_SETTING);
        ALL_SETTINGS.add(KEYSTORE_KEY_PASSWORD_SETTING);
        ALL_SETTINGS.add(KEY_PATH_SETTING);
        ALL_SETTINGS.add(KEY_PASSWORD_SETTING);
        ALL_SETTINGS.add(CERT_SETTING);
        ALL_SETTINGS.add(TRUSTSTORE_PATH_SETTING);
        ALL_SETTINGS.add(TRUSTSTORE_PASSWORD_SETTING);
        ALL_SETTINGS.add(TRUSTSTORE_ALGORITHM_SETTING);
        ALL_SETTINGS.add(CA_PATHS_SETTING);
        ALL_SETTINGS.add(VERIFICATION_MODE_SETTING);
        ALL_SETTINGS.add(CLIENT_AUTH_SETTING);
        ALL_SETTINGS.add(HTTP_CIPHERS_SETTING);
        ALL_SETTINGS.add(HTTP_SUPPORTED_PROTOCOLS_SETTING);
        ALL_SETTINGS.add(HTTP_KEYSTORE_PATH_SETTING);
        ALL_SETTINGS.add(HTTP_KEYSTORE_PASSWORD_SETTING);
        ALL_SETTINGS.add(HTTP_KEYSTORE_ALGORITHM_SETTING);
        ALL_SETTINGS.add(HTTP_KEYSTORE_KEY_PASSWORD_SETTING);
        ALL_SETTINGS.add(HTTP_KEY_PATH_SETTING);
        ALL_SETTINGS.add(HTTP_KEY_PASSWORD_SETTING);
        ALL_SETTINGS.add(HTTP_CERT_SETTING);
        ALL_SETTINGS.add(HTTP_TRUSTSTORE_PATH_SETTING);
        ALL_SETTINGS.add(HTTP_TRUSTSTORE_PASSWORD_SETTING);
        ALL_SETTINGS.add(HTTP_TRUSTSTORE_ALGORITHM_SETTING);
        ALL_SETTINGS.add(HTTP_CA_PATHS_SETTING);
        ALL_SETTINGS.add(HTTP_VERIFICATION_MODE_SETTING);
        ALL_SETTINGS.add(HTTP_CLIENT_AUTH_SETTING);
        ALL_SETTINGS.add(TRANSPORT_CIPHERS_SETTING);
        ALL_SETTINGS.add(TRANSPORT_SUPPORTED_PROTOCOLS_SETTING);
        ALL_SETTINGS.add(TRANSPORT_KEYSTORE_PATH_SETTING);
        ALL_SETTINGS.add(TRANSPORT_KEYSTORE_PASSWORD_SETTING);
        ALL_SETTINGS.add(TRANSPORT_KEYSTORE_ALGORITHM_SETTING);
        ALL_SETTINGS.add(TRANSPORT_KEYSTORE_KEY_PASSWORD_SETTING);
        ALL_SETTINGS.add(TRANSPORT_KEY_PATH_SETTING);
        ALL_SETTINGS.add(TRANSPORT_KEY_PASSWORD_SETTING);
        ALL_SETTINGS.add(TRANSPORT_CERT_SETTING);
        ALL_SETTINGS.add(TRANSPORT_TRUSTSTORE_PATH_SETTING);
        ALL_SETTINGS.add(TRANSPORT_TRUSTSTORE_PASSWORD_SETTING);
        ALL_SETTINGS.add(TRANSPORT_TRUSTSTORE_ALGORITHM_SETTING);
        ALL_SETTINGS.add(TRANSPORT_CA_PATHS_SETTING);
        ALL_SETTINGS.add(TRANSPORT_VERIFICATION_MODE_SETTING);
        ALL_SETTINGS.add(TRANSPORT_CLIENT_AUTH_SETTING);
    }

    /**
     * Create a Setting for the enabled state of features in xpack.
     *
     * The given feature by be enabled or disabled with:
     * {@code "xpack.<feature>.enabled": true | false}
     *
     * For backward compatibility with 1.x and 2.x, the following also works:
     * {@code "<feature>.enabled": true | false}
     *
     * @param featureName The name of the feature in xpack
     * @param defaultValue True if the feature should be enabled by defualt, false otherwise
     */
    private static Setting<Boolean> enabledSetting(String featureName, boolean defaultValue) {
        return enabledSetting(featureName, s -> String.valueOf(defaultValue));
    }

    /**
     * Create a setting for the enabled state of a feature, with a complex default value.
     * @param featureName The name of the feature in xpack
     * @param defaultValueFn A function to determine the default value based on the existing settings
     * @see #enabledSetting(String,boolean)
     */
    private static Setting<Boolean> enabledSetting(String featureName, Function<Settings,String> defaultValueFn) {
        String fallbackName = featureName + ".enabled";
        Setting<Boolean> fallback = Setting.boolSetting(fallbackName, defaultValueFn,
            Setting.Property.NodeScope, Setting.Property.Deprecated);
        String settingName = XPackPlugin.featureSettingPrefix(featureName) + ".enabled";
        Setting<Boolean> setting = Setting.boolSetting(settingName, fallback, Setting.Property.NodeScope);
        ALL_SETTINGS.add(setting);
        return setting;
    }

    /** Returns all settings created in {@link XPackSettings}. */
    static List<Setting<?>> getAllSettings() {
        return Collections.unmodifiableList(ALL_SETTINGS);
    }
}
