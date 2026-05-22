/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workload.identity;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfigurationKeys;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bootstraps and publishes the {@link WorkloadIssuerCachingClient}, which issues short-lived workload
 * identity tokens (JWT) that other plugins can later exchange for cloud-provider-specific credentials
 */
public class WorkloadIdentityIssuerClientPlugin extends Plugin {

    private static final String SSL_SETTINGS_PREFIX = "xpack.workload_identity.issuer_client.ssl.";
    private static final TimeValue DEFAULT_CONNECTION_TTL = TimeValue.timeValueMinutes(5);

    private static final Map<String, Setting<?>> SSL_SETTINGS = new HashMap<>();
    private static final Map<String, Setting<SecureString>> SSL_SECURE_SETTINGS = new HashMap<>();

    static {
        final Property[] defaultProperties = new Property[] { Property.NodeScope };
        final Property[] deprecatedProperties = new Property[] { Property.NodeScope, Property.DeprecatedWarning };
        for (String key : SslConfigurationKeys.getStringKeys()) {
            final String settingName = SSL_SETTINGS_PREFIX + key;
            final Property[] properties = SslConfigurationKeys.isDeprecated(key) ? deprecatedProperties : defaultProperties;
            SSL_SETTINGS.put(settingName, Setting.simpleString(settingName, properties));
        }
        for (String key : SslConfigurationKeys.getListKeys()) {
            final String settingName = SSL_SETTINGS_PREFIX + key;
            final Property[] properties = SslConfigurationKeys.isDeprecated(key) ? deprecatedProperties : defaultProperties;
            SSL_SETTINGS.put(settingName, Setting.stringListSetting(settingName, properties));
        }
        for (String key : SslConfigurationKeys.getSecureStringKeys()) {
            final String settingName = SSL_SETTINGS_PREFIX + key;
            SSL_SECURE_SETTINGS.put(settingName, SecureSetting.secureString(settingName, null));
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        final List<Setting<?>> settings = new ArrayList<>(SSL_SETTINGS.size() + SSL_SECURE_SETTINGS.size());
        settings.addAll(SSL_SETTINGS.values());
        settings.addAll(SSL_SECURE_SETTINGS.values());
        return settings;
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        final Settings settings = services.environment().settings();
        final WorkloadIssuerCachingClient client = new WorkloadIssuerCachingClientImpl(
            settings,
            services.threadPool(),
            services.clusterService(),
            null,
            null
        );
        return List.of(new PluginComponentBinding<>(WorkloadIssuerCachingClient.class, client));
    }
}
