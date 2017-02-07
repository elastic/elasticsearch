/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.ssl.SSLClientAuth;
import org.elasticsearch.xpack.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.ssl.VerificationMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

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

    /**
     * Legacy setting for enabling or disabling transport ssl. Defaults to true. This is just here to make upgrading easier since the
     * user needs to set this setting in 5.x to upgrade
     */
    private static final Setting<Boolean> TRANSPORT_SSL_ENABLED =
            new Setting<>(XPackPlugin.featureSettingPrefix(XPackPlugin.SECURITY) + ".transport.ssl.enabled", (s) -> Boolean.toString(true),
                    (s) -> {
                        final boolean parsed = Booleans.parseBoolean(s);
                        if (parsed == false) {
                            throw new IllegalArgumentException("transport ssl cannot be disabled. Remove setting [" +
                                    XPackPlugin.featureSettingPrefix(XPackPlugin.SECURITY) + ".transport.ssl.enabled]");
                        }
                        return true;
                    }, Property.NodeScope, Property.Deprecated);

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
    public static final String GLOBAL_SSL_PREFIX = "xpack.ssl.";
    private static final SSLConfigurationSettings GLOBAL_SSL = SSLConfigurationSettings.withPrefix(GLOBAL_SSL_PREFIX);

    // http specific settings
    public static final String HTTP_SSL_PREFIX = Security.setting("http.ssl.");
    private static final SSLConfigurationSettings HTTP_SSL = SSLConfigurationSettings.withPrefix(HTTP_SSL_PREFIX);

    // transport specific settings
    public static final String TRANSPORT_SSL_PREFIX = Security.setting("transport.ssl.");
    private static final SSLConfigurationSettings TRANSPORT_SSL = SSLConfigurationSettings.withPrefix(TRANSPORT_SSL_PREFIX);

    /* End SSL settings */

    static {
        ALL_SETTINGS.addAll(GLOBAL_SSL.getAllSettings());
        ALL_SETTINGS.addAll(HTTP_SSL.getAllSettings());
        ALL_SETTINGS.addAll(TRANSPORT_SSL.getAllSettings());
        ALL_SETTINGS.add(TRANSPORT_SSL_ENABLED);
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
