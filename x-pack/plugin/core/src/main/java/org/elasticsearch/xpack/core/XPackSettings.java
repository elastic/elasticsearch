/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.VerificationMode;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.SecurityField.USER_SETTING;

/**
 * A container for xpack setting constants.
 */
public class XPackSettings {

    private XPackSettings() {
        throw new IllegalStateException("Utility class should not be instantiated");
    }


    /**
     * Setting for controlling whether or not CCR is enabled.
     */
    public static final Setting<Boolean> CCR_ENABLED_SETTING = Setting.boolSetting("xpack.ccr.enabled", true, Property.NodeScope);

    /** Setting for enabling or disabling security. Defaults to true. */
    public static final Setting<Boolean> SECURITY_ENABLED = Setting.boolSetting("xpack.security.enabled", true, Setting.Property.NodeScope);

    /** Setting for enabling or disabling monitoring. */
    public static final Setting<Boolean> MONITORING_ENABLED = Setting.boolSetting("xpack.monitoring.enabled", true,
            Setting.Property.NodeScope);

    /** Setting for enabling or disabling watcher. Defaults to true. */
    public static final Setting<Boolean> WATCHER_ENABLED = Setting.boolSetting("xpack.watcher.enabled", true, Setting.Property.NodeScope);

    /** Setting for enabling or disabling graph. Defaults to true. */
    public static final Setting<Boolean> GRAPH_ENABLED = Setting.boolSetting("xpack.graph.enabled", true, Setting.Property.NodeScope);

    /** Setting for enabling or disabling machine learning. Defaults to false. */
    public static final Setting<Boolean> MACHINE_LEARNING_ENABLED = Setting.boolSetting("xpack.ml.enabled", true,
            Setting.Property.NodeScope);

    /** Setting for enabling or disabling rollup. Defaults to true. */
    public static final Setting<Boolean> ROLLUP_ENABLED = Setting.boolSetting("xpack.rollup.enabled", true,
            Setting.Property.NodeScope);

    /** Setting for enabling or disabling auditing. Defaults to false. */
    public static final Setting<Boolean> AUDIT_ENABLED = Setting.boolSetting("xpack.security.audit.enabled", false,
            Setting.Property.NodeScope);

    /** Setting for enabling or disabling document/field level security. Defaults to true. */
    public static final Setting<Boolean> DLS_FLS_ENABLED = Setting.boolSetting("xpack.security.dls_fls.enabled", true,
            Setting.Property.NodeScope);

    /** Setting for enabling or disabling Logstash extensions. Defaults to true. */
    public static final Setting<Boolean> LOGSTASH_ENABLED = Setting.boolSetting("xpack.logstash.enabled", true,
            Setting.Property.NodeScope);

    /** Setting for enabling or disabling Beats extensions. Defaults to true. */
    public static final Setting<Boolean> BEATS_ENABLED = Setting.boolSetting("xpack.beats.enabled", true,
        Setting.Property.NodeScope);

    /**
     * Setting for enabling or disabling the index lifecycle extension. Defaults to true.
     */
    public static final Setting<Boolean> INDEX_LIFECYCLE_ENABLED = Setting.boolSetting("xpack.ilm.enabled", true,
        Setting.Property.NodeScope);

    /** Setting for enabling or disabling TLS. Defaults to false. */
    public static final Setting<Boolean> TRANSPORT_SSL_ENABLED = Setting.boolSetting("xpack.security.transport.ssl.enabled", false,
            Property.NodeScope);

    /** Setting for enabling or disabling http ssl. Defaults to false. */
    public static final Setting<Boolean> HTTP_SSL_ENABLED = Setting.boolSetting("xpack.security.http.ssl.enabled", false,
            Setting.Property.NodeScope);

    /** Setting for enabling or disabling the reserved realm. Defaults to true */
    public static final Setting<Boolean> RESERVED_REALM_ENABLED_SETTING = Setting.boolSetting("xpack.security.authc.reserved_realm.enabled",
            true, Setting.Property.NodeScope);

    /** Setting for enabling or disabling the token service. Defaults to true */
    public static final Setting<Boolean> TOKEN_SERVICE_ENABLED_SETTING = Setting.boolSetting("xpack.security.authc.token.enabled",
        XPackSettings.HTTP_SSL_ENABLED::getRaw, Setting.Property.NodeScope);

    /** Setting for enabling or disabling FIPS mode. Defaults to false */
    public static final Setting<Boolean> FIPS_MODE_ENABLED =
        Setting.boolSetting("xpack.security.fips_mode.enabled", false, Property.NodeScope);

    /** Setting for enabling or disabling sql. Defaults to true. */
    public static final Setting<Boolean> SQL_ENABLED = Setting.boolSetting("xpack.sql.enabled", true, Setting.Property.NodeScope);

    /*
     * SSL settings. These are the settings that are specifically registered for SSL. Many are private as we do not explicitly use them
     * but instead parse based on a prefix (eg *.ssl.*)
     */
    public static final List<String> DEFAULT_CIPHERS;

    static {
        List<String> ciphers = Arrays.asList("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
                "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA256",
                "TLS_RSA_WITH_AES_128_CBC_SHA");
        try {
            final boolean use256Bit = Cipher.getMaxAllowedKeyLength("AES") > 128;
            if (use256Bit) {
                List<String> strongerCiphers = new ArrayList<>(ciphers.size() * 2);
                strongerCiphers.addAll(Arrays.asList("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
                        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
                        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA256", "TLS_RSA_WITH_AES_256_CBC_SHA"));
                strongerCiphers.addAll(ciphers);
                ciphers = strongerCiphers;
            }
        } catch (NoSuchAlgorithmException e) {
            // ignore it here - there will be issues elsewhere and its not nice to throw in a static initializer
        }

        DEFAULT_CIPHERS = ciphers;
    }

    /*
     * Do not allow insecure hashing algorithms to be used for password hashing
     */
    public static final Setting<String> PASSWORD_HASHING_ALGORITHM = new Setting<>(
        "xpack.security.authc.password_hashing.algorithm", "bcrypt", Function.identity(), v -> {
        if (Hasher.getAvailableAlgoStoredHash().contains(v.toLowerCase(Locale.ROOT)) == false) {
            throw new IllegalArgumentException("Invalid algorithm: " + v + ". Valid values for password hashing are " +
                Hasher.getAvailableAlgoStoredHash().toString());
        } else if (v.regionMatches(true, 0, "pbkdf2", 0, "pbkdf2".length())) {
            try {
                SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalArgumentException(
                    "Support for PBKDF2WithHMACSHA512 must be available in order to use any of the " +
                        "PBKDF2 algorithms for the [xpack.security.authc.password_hashing.algorithm] setting.", e);
            }
        }
    }, Setting.Property.NodeScope);

    public static final List<String> DEFAULT_SUPPORTED_PROTOCOLS = Arrays.asList("TLSv1.2", "TLSv1.1", "TLSv1");
    public static final SSLClientAuth CLIENT_AUTH_DEFAULT = SSLClientAuth.REQUIRED;
    public static final SSLClientAuth HTTP_CLIENT_AUTH_DEFAULT = SSLClientAuth.NONE;
    public static final VerificationMode VERIFICATION_MODE_DEFAULT = VerificationMode.FULL;

    // global settings that apply to everything!
    public static final String GLOBAL_SSL_PREFIX = "xpack.ssl.";
    private static final SSLConfigurationSettings GLOBAL_SSL = SSLConfigurationSettings.withPrefix(GLOBAL_SSL_PREFIX);

    // http specific settings
    public static final String HTTP_SSL_PREFIX = SecurityField.setting("http.ssl.");
    private static final SSLConfigurationSettings HTTP_SSL = SSLConfigurationSettings.withPrefix(HTTP_SSL_PREFIX);

    // transport specific settings
    public static final String TRANSPORT_SSL_PREFIX = SecurityField.setting("transport.ssl.");
    private static final SSLConfigurationSettings TRANSPORT_SSL = SSLConfigurationSettings.withPrefix(TRANSPORT_SSL_PREFIX);

    /** Returns all settings created in {@link XPackSettings}. */
    public static List<Setting<?>> getAllSettings() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        settings.addAll(GLOBAL_SSL.getAllSettings());
        settings.addAll(HTTP_SSL.getAllSettings());
        settings.addAll(TRANSPORT_SSL.getAllSettings());
        settings.add(SECURITY_ENABLED);
        settings.add(MONITORING_ENABLED);
        settings.add(GRAPH_ENABLED);
        settings.add(MACHINE_LEARNING_ENABLED);
        settings.add(AUDIT_ENABLED);
        settings.add(WATCHER_ENABLED);
        settings.add(DLS_FLS_ENABLED);
        settings.add(LOGSTASH_ENABLED);
        settings.add(TRANSPORT_SSL_ENABLED);
        settings.add(HTTP_SSL_ENABLED);
        settings.add(RESERVED_REALM_ENABLED_SETTING);
        settings.add(TOKEN_SERVICE_ENABLED_SETTING);
        settings.add(SQL_ENABLED);
        settings.add(USER_SETTING);
        settings.add(ROLLUP_ENABLED);
        settings.add(PASSWORD_HASHING_ALGORITHM);
        settings.add(INDEX_LIFECYCLE_ENABLED);
        return Collections.unmodifiableList(settings);
    }

}
