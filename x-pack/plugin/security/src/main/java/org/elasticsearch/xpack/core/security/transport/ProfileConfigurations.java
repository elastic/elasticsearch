/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.transport;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PORT_ENABLED;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;
import static org.elasticsearch.xpack.core.XPackSettings.REMOTE_CLUSTER_SSL_ENABLED;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;

public final class ProfileConfigurations {

    private ProfileConfigurations() {}

    public static Map<String, SslConfiguration> get(Settings settings, SSLService sslService, boolean sslEnabledOnly) {
        final boolean transportSslEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
        final boolean remoteClusterPortEnabled = REMOTE_CLUSTER_PORT_ENABLED.get(settings);
        final boolean remoteClusterSslEnabled = remoteClusterPortEnabled && REMOTE_CLUSTER_SSL_ENABLED.get(settings);

        final Map<String, SslConfiguration> profileConfigurations = new HashMap<>();

        if (sslEnabledOnly) {
            if (false == transportSslEnabled && false == remoteClusterSslEnabled) {
                return profileConfigurations;
            } else if (false == transportSslEnabled) {
                profileConfigurations.put(REMOTE_CLUSTER_PROFILE, sslService.getSSLConfiguration(XPackSettings.REMOTE_CLUSTER_SSL_PREFIX));
                return profileConfigurations;
            } else if (false == remoteClusterSslEnabled) {
                populateFromTransportProfiles(settings, sslService, profileConfigurations);
                return profileConfigurations;
            }
        }

        // At this point, either SSL is enabled for both transport and remote cluster, or sslEnabledOnly is false.
        // In both case, we need to include all configurations
        populateFromTransportProfiles(settings, sslService, profileConfigurations);
        if (remoteClusterPortEnabled) {
            assert profileConfigurations.containsKey(REMOTE_CLUSTER_PROFILE) == false;
            profileConfigurations.put(REMOTE_CLUSTER_PROFILE, sslService.getSSLConfiguration(XPackSettings.REMOTE_CLUSTER_SSL_PREFIX));
        }

        return profileConfigurations;
    }

    private static void populateFromTransportProfiles(
        Settings settings,
        SSLService sslService,
        Map<String, SslConfiguration> profileConfigurations
    ) {
        final SslConfiguration defaultConfiguration = sslService.getSSLConfiguration(setting("transport.ssl."));

        Set<String> profileNames = settings.getGroups("transport.profiles.", true).keySet();
        for (String profileName : profileNames) {
            if (profileName.equals(TransportSettings.DEFAULT_PROFILE)) {
                // don't attempt to parse ssl settings from the profile;
                // profiles need to be killed with fire
                // We don't need to check _remote_cluster profile here; when remote access settings are validated, we check that there are
                // no direct usages of the profile, so we can just add it after all the profiles are in place.
                if (settings.getByPrefix("transport.profiles.default.xpack.security.ssl.").isEmpty()) {
                    continue;
                } else {
                    throw new IllegalArgumentException(
                        "SSL settings should not be configured for the default profile. "
                            + "Use the [xpack.security.transport.ssl] settings instead."
                    );
                }
            }

            SslConfiguration configuration = sslService.getSSLConfiguration("transport.profiles." + profileName + "." + setting("ssl"));
            profileConfigurations.put(profileName, configuration);
        }

        assert profileConfigurations.containsKey(TransportSettings.DEFAULT_PROFILE) == false;
        profileConfigurations.put(TransportSettings.DEFAULT_PROFILE, defaultConfiguration);
    }
}
