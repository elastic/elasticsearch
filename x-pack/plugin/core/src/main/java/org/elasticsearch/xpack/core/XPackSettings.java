/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import javax.crypto.SecretKeyFactory;
import javax.net.ssl.SSLContext;
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
    private static final boolean IS_DARWIN_AARCH64;
    static {
        final String name = System.getProperty("os.name");
        final String arch = System.getProperty("os.arch");
        IS_DARWIN_AARCH64 = "aarch64".equals(arch) && name.startsWith("Mac OS X");
    }

    private XPackSettings() {
        throw new IllegalStateException("Utility class should not be instantiated");
    }

    /**
     * Setting for controlling whether or not CCR is enabled.
     */
    public static final Setting<Boolean> CCR_ENABLED_SETTING = Setting.boolSetting("xpack.ccr.enabled", true, Property.NodeScope);

    /** Setting for enabling or disabling security. Defaults to true. */
    public static final Setting<Boolean> SECURITY_ENABLED = Setting.boolSetting("xpack.security.enabled", true, Setting.Property.NodeScope);

    /** Setting for enabling or disabling watcher. Defaults to true. */
    public static final Setting<Boolean> WATCHER_ENABLED = Setting.boolSetting("xpack.watcher.enabled", true, Setting.Property.NodeScope);

    /** Setting for enabling or disabling graph. Defaults to true. */
    public static final Setting<Boolean> GRAPH_ENABLED = Setting.boolSetting("xpack.graph.enabled", true, Setting.Property.NodeScope);

    /** Setting for enabling or disabling machine learning. Defaults to true. */
    public static final Setting<Boolean> MACHINE_LEARNING_ENABLED = Setting.boolSetting(
        "xpack.ml.enabled",
        IS_DARWIN_AARCH64 == false,
        value -> {
            if (value && IS_DARWIN_AARCH64) {
                throw new IllegalArgumentException("[xpack.ml.enabled] can not be set to [true] on [Mac OS X/aarch64]");
            }
        },
        Setting.Property.NodeScope);

    /** Setting for enabling or disabling auditing. Defaults to false. */
    public static final Setting<Boolean> AUDIT_ENABLED = Setting.boolSetting("xpack.security.audit.enabled", false,
            Setting.Property.NodeScope);

    /** Setting for enabling or disabling document/field level security. Defaults to true. */
    public static final Setting<Boolean> DLS_FLS_ENABLED = Setting.boolSetting("xpack.security.dls_fls.enabled", true,
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

    /** Setting for enabling or disabling the token service. Defaults to the value of https being enabled */
    public static final Setting<Boolean> TOKEN_SERVICE_ENABLED_SETTING =
        Setting.boolSetting("xpack.security.authc.token.enabled", XPackSettings.HTTP_SSL_ENABLED, Setting.Property.NodeScope);

    /** Setting for enabling or disabling the api key service. Defaults to the value of https being enabled */
    public static final Setting<Boolean> API_KEY_SERVICE_ENABLED_SETTING =
        Setting.boolSetting("xpack.security.authc.api_key.enabled", XPackSettings.HTTP_SSL_ENABLED, Setting.Property.NodeScope);

    /** Setting for enabling or disabling FIPS mode. Defaults to false */
    public static final Setting<Boolean> FIPS_MODE_ENABLED =
        Setting.boolSetting("xpack.security.fips_mode.enabled", false, Property.NodeScope);

    /** Setting for enabling enrollment process; set-up by the es start-up script */
    public static final Setting<Boolean> ENROLLMENT_ENABLED =
        Setting.boolSetting("xpack.security.enrollment.enabled", false, Property.NodeScope);

    /*
     * SSL settings. These are the settings that are specifically registered for SSL. Many are private as we do not explicitly use them
     * but instead parse based on a prefix (eg *.ssl.*)
     */
    private static final List<String> JDK11_CIPHERS = List.of(
        "TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256", // TLSv1.3 cipher has PFS, AEAD, hardware support
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", // PFS, AEAD, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", // PFS, AEAD, hardware support
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",  "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256", // PFS, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", // PFS, hardware support
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA", // PFS, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", // PFS, hardware support
        "TLS_RSA_WITH_AES_256_GCM_SHA384", "TLS_RSA_WITH_AES_128_GCM_SHA256", // AEAD, hardware support
        "TLS_RSA_WITH_AES_256_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA256", // hardware support
        "TLS_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA"); // hardware support

    private static final List<String> JDK12_CIPHERS = List.of(
        "TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256", // TLSv1.3 cipher has PFS, AEAD, hardware support
        "TLS_CHACHA20_POLY1305_SHA256", // TLSv1.3 cipher has PFS, AEAD
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", // PFS, AEAD, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", // PFS, AEAD, hardware support
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256", "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256", // PFS, AEAD
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",  "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256", // PFS, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", // PFS, hardware support
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA", // PFS, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", // PFS, hardware support
        "TLS_RSA_WITH_AES_256_GCM_SHA384", "TLS_RSA_WITH_AES_128_GCM_SHA256", // AEAD, hardware support
        "TLS_RSA_WITH_AES_256_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA256", // hardware support
        "TLS_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA"); // hardware support

    public static final List<String> DEFAULT_CIPHERS =
        JavaVersion.current().compareTo(JavaVersion.parse("12")) > -1 ? JDK12_CIPHERS : JDK11_CIPHERS;

    /*
     * Do not allow insecure hashing algorithms to be used for password hashing
     */
    public static final Setting<String> PASSWORD_HASHING_ALGORITHM = new Setting<>(
        new Setting.SimpleKey("xpack.security.authc.password_hashing.algorithm"),
        (s) -> {
            if (XPackSettings.FIPS_MODE_ENABLED.get(s)) {
                return "PBKDF2";
            } else {
                return "BCRYPT";
            }
        },
        Function.identity(),
        v -> {
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
        }, Property.NodeScope);

    // TODO: This setting of hashing algorithm can share code with the one for password when pbkdf2_stretch is the default for both
    public static final Setting<String> SERVICE_TOKEN_HASHING_ALGORITHM = new Setting<>(
        new Setting.SimpleKey("xpack.security.authc.service_token_hashing.algorithm"),
        (s) -> "PBKDF2_STRETCH",
        Function.identity(),
        v -> {
            if (Hasher.getAvailableAlgoStoredHash().contains(v.toLowerCase(Locale.ROOT)) == false) {
                throw new IllegalArgumentException("Invalid algorithm: " + v + ". Valid values for password hashing are " +
                    Hasher.getAvailableAlgoStoredHash().toString());
            } else if (v.regionMatches(true, 0, "pbkdf2", 0, "pbkdf2".length())) {
                try {
                    SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalArgumentException(
                        "Support for PBKDF2WithHMACSHA512 must be available in order to use any of the " +
                            "PBKDF2 algorithms for the [xpack.security.authc.service_token_hashing.algorithm] setting.", e);
                }
            }
        }, Property.NodeScope);

    public static final List<String> DEFAULT_SUPPORTED_PROTOCOLS;

    static {
        boolean supportsTLSv13 = false;
        try {
            SSLContext.getInstance("TLSv1.3");
            supportsTLSv13 = true;
        } catch (NoSuchAlgorithmException e) {
            // BCJSSE in FIPS mode doesn't support TLSv1.3 yet.
            LogManager.getLogger(XPackSettings.class).debug("TLSv1.3 is not supported", e);
        }
        DEFAULT_SUPPORTED_PROTOCOLS = supportsTLSv13 ?
            Arrays.asList("TLSv1.3", "TLSv1.2", "TLSv1.1") : Arrays.asList("TLSv1.2", "TLSv1.1");
    }

    public static final SSLClientAuth CLIENT_AUTH_DEFAULT = SSLClientAuth.REQUIRED;
    public static final SSLClientAuth HTTP_CLIENT_AUTH_DEFAULT = SSLClientAuth.NONE;
    public static final SslVerificationMode VERIFICATION_MODE_DEFAULT = SslVerificationMode.FULL;

    // http specific settings
    public static final String HTTP_SSL_PREFIX = SecurityField.setting("http.ssl.");
    private static final SSLConfigurationSettings HTTP_SSL = SSLConfigurationSettings.withPrefix(HTTP_SSL_PREFIX, true);

    // transport specific settings
    public static final String TRANSPORT_SSL_PREFIX = SecurityField.setting("transport.ssl.");
    private static final SSLConfigurationSettings TRANSPORT_SSL = SSLConfigurationSettings.withPrefix(TRANSPORT_SSL_PREFIX, true);

    /** Returns all settings created in {@link XPackSettings}. */
    public static List<Setting<?>> getAllSettings() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        settings.addAll(HTTP_SSL.getEnabledSettings());
        settings.addAll(TRANSPORT_SSL.getEnabledSettings());
        settings.add(SECURITY_ENABLED);
        settings.add(GRAPH_ENABLED);
        settings.add(MACHINE_LEARNING_ENABLED);
        settings.add(AUDIT_ENABLED);
        settings.add(WATCHER_ENABLED);
        settings.add(DLS_FLS_ENABLED);
        settings.add(TRANSPORT_SSL_ENABLED);
        settings.add(HTTP_SSL_ENABLED);
        settings.add(RESERVED_REALM_ENABLED_SETTING);
        settings.add(TOKEN_SERVICE_ENABLED_SETTING);
        settings.add(API_KEY_SERVICE_ENABLED_SETTING);
        settings.add(USER_SETTING);
        settings.add(PASSWORD_HASHING_ALGORITHM);
        settings.add(ENROLLMENT_ENABLED);
        return Collections.unmodifiableList(settings);
    }

}
