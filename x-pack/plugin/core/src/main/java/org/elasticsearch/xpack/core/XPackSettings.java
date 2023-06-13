/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.core.Strings;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import javax.crypto.SecretKeyFactory;
import javax.net.ssl.SSLContext;

import static org.elasticsearch.xpack.core.security.SecurityField.USER_SETTING;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.DOMAIN_TO_REALM_ASSOC_SETTING;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.DOMAIN_UID_LITERAL_USERNAME_SETTING;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.DOMAIN_UID_SUFFIX_SETTING;

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
    public static final Setting<Boolean> SECURITY_ENABLED = Setting.boolSetting("xpack.security.enabled", true, new Setting.Validator<>() {
        @Override
        public void validate(Boolean value) {}

        @Override
        public void validate(Boolean value, Map<Setting<?>, Object> settings, boolean isPresent) {
            final boolean remoteClusterServerEnabled = (boolean) settings.get(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED);
            if (remoteClusterServerEnabled && false == value) {
                throw new IllegalArgumentException(
                    Strings.format("Security [%s] must be enabled to use the remote cluster server feature", SECURITY_ENABLED.getKey())
                );
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return List.<Setting<?>>of(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED).iterator();
        }
    }, Setting.Property.NodeScope);

    /** Setting for enabling or disabling watcher. Defaults to true. */
    public static final Setting<Boolean> WATCHER_ENABLED = Setting.boolSetting("xpack.watcher.enabled", true, Setting.Property.NodeScope);

    /** Setting for enabling or disabling graph. Defaults to true. */
    public static final Setting<Boolean> GRAPH_ENABLED = Setting.boolSetting("xpack.graph.enabled", true, Setting.Property.NodeScope);

    /** Setting for enabling or disabling machine learning. Defaults to true. */
    public static final Setting<Boolean> MACHINE_LEARNING_ENABLED = Setting.boolSetting(
        "xpack.ml.enabled",
        true,
        Setting.Property.NodeScope
    );

    /** Setting for enabling or disabling universal profiling. Defaults to true. */
    public static final Setting<Boolean> PROFILING_ENABLED = Setting.boolSetting(
        "xpack.profiling.enabled",
        true,
        Setting.Property.NodeScope
    );

    /** Setting for enabling or disabling enterprise search. Defaults to true. */
    public static final Setting<Boolean> ENTERPRISE_SEARCH_ENABLED = Setting.boolSetting(
        "xpack.ent_search.enabled",
        true,
        Setting.Property.NodeScope
    );

    /** Setting for enabling or disabling query rules. Defaults to false. */
    public static final Setting<Boolean> ENTERPRISE_SEARCH_QUERY_RULES_ENABLED = Setting.boolSetting(
        "xpack.ent_search.query_rules.enabled",
        true,
        Setting.Property.NodeScope
    );

    /** Setting for enabling or disabling auditing. Defaults to false. */
    public static final Setting<Boolean> AUDIT_ENABLED = Setting.boolSetting(
        "xpack.security.audit.enabled",
        false,
        Setting.Property.NodeScope
    );

    /** Setting for enabling or disabling document/field level security. Defaults to true. */
    public static final Setting<Boolean> DLS_FLS_ENABLED = Setting.boolSetting(
        "xpack.security.dls_fls.enabled",
        true,
        Setting.Property.NodeScope
    );

    /** Setting for enabling or disabling TLS. Defaults to false. */
    public static final Setting<Boolean> TRANSPORT_SSL_ENABLED = Setting.boolSetting(
        "xpack.security.transport.ssl.enabled",
        false,
        Property.NodeScope
    );

    /** Setting for enabling or disabling http ssl. Defaults to false. */
    public static final Setting<Boolean> HTTP_SSL_ENABLED = Setting.boolSetting(
        "xpack.security.http.ssl.enabled",
        false,
        Setting.Property.NodeScope
    );

    /** Setting for enabling or disabling the reserved realm. Defaults to true */
    public static final Setting<Boolean> RESERVED_REALM_ENABLED_SETTING = Setting.boolSetting(
        "xpack.security.authc.reserved_realm.enabled",
        true,
        Setting.Property.NodeScope
    );

    /** Setting for enabling or disabling the token service. Defaults to the value of https being enabled */
    public static final Setting<Boolean> TOKEN_SERVICE_ENABLED_SETTING = Setting.boolSetting(
        "xpack.security.authc.token.enabled",
        XPackSettings.HTTP_SSL_ENABLED,
        Setting.Property.NodeScope
    );

    /** Setting for enabling or disabling the api key service. Defaults to true */
    public static final Setting<Boolean> API_KEY_SERVICE_ENABLED_SETTING = Setting.boolSetting(
        "xpack.security.authc.api_key.enabled",
        true,
        Setting.Property.NodeScope
    );

    /** Setting for enabling or disabling FIPS mode. Defaults to false */
    public static final Setting<Boolean> FIPS_MODE_ENABLED = Setting.boolSetting(
        "xpack.security.fips_mode.enabled",
        false,
        Property.NodeScope
    );

    /**
     * Setting for enabling the enrollment process, ie the enroll APIs are enabled, and the initial cluster node generates and displays
     * enrollment tokens (for Kibana and sometimes for ES nodes) when starting up for the first time.
     * This is usually set by start-up scripts, which run before the node starts, which perform TLS and cluster formation specific
     * configuration (persisted in the node's config dir).
     * This can be toggled liberally by admins (it can be made a dynamic setting), in order to permit or not the enrollment of subsequent
     * nodes. Nevertheless, we assumes that when {@code ENROLLMENT_ENABLED} is {@code true} the node MUST have been configured by said
     * start-up scripts (eg we don't support enrollment with general TLS certificates).
     */
    public static final Setting<Boolean> ENROLLMENT_ENABLED = Setting.boolSetting(
        "xpack.security.enrollment.enabled",
        false,
        Property.NodeScope
    );

    /**
     * Setting for enabling or disabling the TLS auto-configuration as well as credentials auto-generation for nodes, before starting for
     * the first time, and in the absence of other conflicting configurations.
     */
    public static final Setting<Boolean> SECURITY_AUTOCONFIGURATION_ENABLED = Setting.boolSetting(
        "xpack.security.autoconfiguration.enabled",
        true,
        Property.NodeScope
    );

    private static final List<String> JDK12_CIPHERS = List.of(
        "TLS_AES_256_GCM_SHA384",
        "TLS_AES_128_GCM_SHA256", // TLSv1.3 cipher has PFS, AEAD, hardware support
        "TLS_CHACHA20_POLY1305_SHA256", // TLSv1.3 cipher has PFS, AEAD
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", // PFS, AEAD, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", // PFS, AEAD, hardware support
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256", // PFS, AEAD
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256", // PFS, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", // PFS, hardware support
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA", // PFS, hardware support
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", // PFS, hardware support
        "TLS_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_RSA_WITH_AES_128_GCM_SHA256", // AEAD, hardware support
        "TLS_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_RSA_WITH_AES_128_CBC_SHA256", // hardware support
        "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_RSA_WITH_AES_128_CBC_SHA"
    ); // hardware support

    public static final List<String> DEFAULT_CIPHERS = JDK12_CIPHERS;

    public static final Setting<String> PASSWORD_HASHING_ALGORITHM = defaultStoredHashAlgorithmSetting(
        "xpack.security.authc.password_hashing.algorithm",
        (s) -> {
            if (XPackSettings.FIPS_MODE_ENABLED.get(s)) {
                return Hasher.PBKDF2_STRETCH.name();
            } else {
                return Hasher.BCRYPT.name();
            }
        }
    );

    public static final Setting<String> SERVICE_TOKEN_HASHING_ALGORITHM = defaultStoredHashAlgorithmSetting(
        "xpack.security.authc.service_token_hashing.algorithm",
        (s) -> Hasher.PBKDF2_STRETCH.name()
    );

    /*
     * Do not allow insecure hashing algorithms to be used for password hashing
     */
    public static Setting<String> defaultStoredHashAlgorithmSetting(String key, Function<Settings, String> defaultHashingAlgorithm) {
        return new Setting<>(new Setting.SimpleKey(key), defaultHashingAlgorithm, Function.identity(), v -> {
            if (Hasher.getAvailableAlgoStoredHash().contains(v.toLowerCase(Locale.ROOT)) == false) {
                throw new IllegalArgumentException(
                    "Invalid algorithm: " + v + ". Valid values for password hashing are " + Hasher.getAvailableAlgoStoredHash().toString()
                );
            } else if (v.regionMatches(true, 0, "pbkdf2", 0, "pbkdf2".length())) {
                try {
                    SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
                } catch (NoSuchAlgorithmException e) {
                    throw new IllegalArgumentException(
                        "Support for PBKDF2WithHMACSHA512 must be available in order to use any of the PBKDF2 algorithms for the ["
                            + key
                            + "] setting.",
                        e
                    );
                }
            }
        }, Property.NodeScope);
    }

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
        DEFAULT_SUPPORTED_PROTOCOLS = supportsTLSv13 ? Arrays.asList("TLSv1.3", "TLSv1.2", "TLSv1.1") : Arrays.asList("TLSv1.2", "TLSv1.1");
    }

    public static final SslClientAuthenticationMode CLIENT_AUTH_DEFAULT = SslClientAuthenticationMode.REQUIRED;
    public static final SslClientAuthenticationMode HTTP_CLIENT_AUTH_DEFAULT = SslClientAuthenticationMode.NONE;
    public static final SslClientAuthenticationMode REMOTE_CLUSTER_CLIENT_AUTH_DEFAULT = SslClientAuthenticationMode.NONE;
    public static final SslVerificationMode VERIFICATION_MODE_DEFAULT = SslVerificationMode.FULL;

    // http specific settings
    public static final String HTTP_SSL_PREFIX = SecurityField.setting("http.ssl.");
    private static final SSLConfigurationSettings HTTP_SSL = SSLConfigurationSettings.withPrefix(HTTP_SSL_PREFIX, true);

    // transport specific settings
    public static final String TRANSPORT_SSL_PREFIX = SecurityField.setting("transport.ssl.");
    private static final SSLConfigurationSettings TRANSPORT_SSL = SSLConfigurationSettings.withPrefix(TRANSPORT_SSL_PREFIX, true);

    // remote cluster specific settings
    public static final String REMOTE_CLUSTER_SERVER_SSL_PREFIX = SecurityField.setting("remote_cluster_server.ssl.");
    public static final String REMOTE_CLUSTER_CLIENT_SSL_PREFIX = SecurityField.setting("remote_cluster_client.ssl.");

    private static final SSLConfigurationSettings REMOTE_CLUSTER_SERVER_SSL = SSLConfigurationSettings.withPrefix(
        REMOTE_CLUSTER_SERVER_SSL_PREFIX,
        false,
        SSLConfigurationSettings.IntendedUse.SERVER
    );

    private static final SSLConfigurationSettings REMOTE_CLUSTER_CLIENT_SSL = SSLConfigurationSettings.withPrefix(
        REMOTE_CLUSTER_CLIENT_SSL_PREFIX,
        false,
        SSLConfigurationSettings.IntendedUse.CLIENT
    );

    /** Setting for enabling or disabling remote cluster server TLS. Defaults to true. */
    public static final Setting<Boolean> REMOTE_CLUSTER_SERVER_SSL_ENABLED = Setting.boolSetting(
        REMOTE_CLUSTER_SERVER_SSL_PREFIX + "enabled",
        true,
        Property.NodeScope
    );

    /** Setting for enabling or disabling remote cluster client TLS. Defaults to true. */
    public static final Setting<Boolean> REMOTE_CLUSTER_CLIENT_SSL_ENABLED = Setting.boolSetting(
        REMOTE_CLUSTER_CLIENT_SSL_PREFIX + "enabled",
        true,
        Property.NodeScope
    );

    /** Returns all settings created in {@link XPackSettings}. */
    public static List<Setting<?>> getAllSettings() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        settings.addAll(HTTP_SSL.getEnabledSettings());
        settings.addAll(TRANSPORT_SSL.getEnabledSettings());
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            settings.addAll(REMOTE_CLUSTER_SERVER_SSL.getEnabledSettings());
            settings.addAll(REMOTE_CLUSTER_CLIENT_SSL.getEnabledSettings());
        }
        settings.add(SECURITY_ENABLED);
        settings.add(GRAPH_ENABLED);
        settings.add(MACHINE_LEARNING_ENABLED);
        settings.add(PROFILING_ENABLED);
        settings.add(ENTERPRISE_SEARCH_ENABLED);
        settings.add(AUDIT_ENABLED);
        settings.add(WATCHER_ENABLED);
        settings.add(DLS_FLS_ENABLED);
        settings.add(TRANSPORT_SSL_ENABLED);
        settings.add(HTTP_SSL_ENABLED);
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            settings.add(REMOTE_CLUSTER_SERVER_SSL_ENABLED);
            settings.add(REMOTE_CLUSTER_CLIENT_SSL_ENABLED);
        }
        settings.add(RESERVED_REALM_ENABLED_SETTING);
        settings.add(TOKEN_SERVICE_ENABLED_SETTING);
        settings.add(API_KEY_SERVICE_ENABLED_SETTING);
        settings.add(USER_SETTING);
        settings.add(PASSWORD_HASHING_ALGORITHM);
        settings.add(ENROLLMENT_ENABLED);
        settings.add(SECURITY_AUTOCONFIGURATION_ENABLED);
        settings.add(DOMAIN_TO_REALM_ASSOC_SETTING);
        settings.add(DOMAIN_UID_LITERAL_USERNAME_SETTING);
        settings.add(DOMAIN_UID_SUFFIX_SETTING);
        return Collections.unmodifiableList(settings);
    }

}
