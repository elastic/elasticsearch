/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslConfigurationKeys;
import org.elasticsearch.common.ssl.SslConfigurationLoader;
import org.elasticsearch.env.Environment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import static org.elasticsearch.common.settings.Setting.simpleString;
import static org.elasticsearch.common.settings.Setting.stringListSetting;

/**
 * Loads {@code workload_identity.ssl.*} configuration from {@link Settings} and exposes the
 * resulting {@link SSLIOSessionStrategy} for the Apache HC-based issuer client.
 *
 * <p>The {@link SSLContext} is built once at construction and never reloaded; cert rotation
 * requires a node restart.
 *
 * @see SslConfigurationLoader
 */
public final class WorkloadIdentitySslConfig {

    /** Setting prefix shared by all SSL configuration values for this module. */
    public static final String SETTING_PREFIX = WorkloadIdentityIssuerSettings.SETTING_PREFIX + "ssl.";

    private static final Map<String, Setting<?>> SETTINGS = new HashMap<>();
    private static final Map<String, Setting<SecureString>> SECURE_SETTINGS = new HashMap<>();

    static {
        Setting.Property[] defaultProperties = new Setting.Property[] { Setting.Property.NodeScope, Setting.Property.Filtered };
        Setting.Property[] deprecatedProperties = new Setting.Property[] {
            Setting.Property.DeprecatedWarning,
            Setting.Property.NodeScope,
            Setting.Property.Filtered };
        for (String key : SslConfigurationKeys.getStringKeys()) {
            String settingName = SETTING_PREFIX + key;
            final Setting.Property[] properties = SslConfigurationKeys.isDeprecated(key) ? deprecatedProperties : defaultProperties;
            SETTINGS.put(settingName, simpleString(settingName, properties));
        }
        for (String key : SslConfigurationKeys.getListKeys()) {
            String settingName = SETTING_PREFIX + key;
            final Setting.Property[] properties = SslConfigurationKeys.isDeprecated(key) ? deprecatedProperties : defaultProperties;
            SETTINGS.put(settingName, stringListSetting(settingName, properties));
        }
        for (String key : SslConfigurationKeys.getSecureStringKeys()) {
            String settingName = SETTING_PREFIX + key;
            SECURE_SETTINGS.put(settingName, SecureSetting.secureString(settingName, null));
        }
    }

    private final SslConfiguration configuration;
    private final SSLContext context;

    /**
     * @return all SSL settings registered by this module. Combine with the rest of the module's
     *         settings via {@link WorkloadIdentityIssuerSettings#getSettings()} when registering
     *         with the plugin.
     */
    public static List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.addAll(SETTINGS.values());
        settings.addAll(SECURE_SETTINGS.values());
        return settings;
    }

    public WorkloadIdentitySslConfig(Settings settings, Environment environment) {
        final SslConfigurationLoader loader = new SslConfigurationLoader(SETTING_PREFIX) {
            @Override
            protected boolean hasSettings(String prefix) {
                return settings.getAsSettings(prefix).isEmpty() == false;
            }

            @Override
            protected String getSettingAsString(String key) {
                return settings.get(key);
            }

            @Override
            protected char[] getSecureSetting(String key) {
                final Setting<SecureString> setting = SECURE_SETTINGS.get(key);
                if (setting == null) {
                    throw new IllegalArgumentException("The secure setting [" + key + "] is not registered");
                }
                return setting.get(settings).getChars();
            }

            @Override
            protected List<String> getSettingAsList(String key) {
                return settings.getAsList(key);
            }
        };
        this.configuration = loader.load(environment.configDir());
        this.context = configuration.createSslContext();
    }

    /**
     * @return the immutable {@link SslConfiguration} loaded from settings. Useful for inspecting
     *         protocols, cipher suites, and verification mode.
     */
    public SslConfiguration configuration() {
        return configuration;
    }

    /**
     * @return an {@link SSLIOSessionStrategy} for Apache HC's async client. A fresh strategy is
     *         returned each call, but always over the same {@link SSLContext} captured at
     *         construction.
     */
    public SSLIOSessionStrategy getStrategy() {
        final HostnameVerifier hostnameVerifier = configuration.verificationMode().isHostnameVerificationEnabled()
            ? new DefaultHostnameVerifier()
            : new NoopHostnameVerifier();
        final String[] protocols = configuration.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        final String[] cipherSuites = configuration.getCipherSuites().toArray(Strings.EMPTY_ARRAY);
        return new SSLIOSessionStrategy(context, protocols, cipherSuites, hostnameVerifier);
    }
}
