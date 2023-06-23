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

import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.REMOTE_CLUSTER_SERVER_SSL_ENABLED;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;

/**
 * Settings for a transport profile usually begin with "transport.profiles.NAME."
 * The settings can be either of the two categories:
 *   1. Networking - e.g. `transport.profiles.NAME.tcp.keep_alive: true`
 *   2. SSL - e.g. `transport.profiles.NAME.xpack.security.ssl.client_authentication: none`
 * This class is responsible for building SSL configuration for transport profiles.
 *
 * Among the transport profiles, two of them are special: "default" and "_remote_cluster".
 *
 * The "default" profile has dedicated settings for both networking (e.g. `transport.tcp.keep_alive`)
 * and SSL (e.g. `xpack.security.transport.ssl.client_authentication`).
 * It also accepts networking settings specified with its transport profile name,
 * e.g. `transport.profiles.default.tcp.keep_alive` is valid configuration.
 * But it does *not* allow SSL settings to be specified with its transport profile name,
 * e.g. `transport.profiles.default.xpack.security.ssl.client_authentication` is NOT valid configuration.
 *
 * The "_remote_cluster" profile also has dedicated settings for both networking (e.g. `remote_cluster.tcp.keep_alive`)
 * and SSL (e.g. `xpack.security.remote_cluster_server.ssl.client_authentication`).
 * This profile is completely synthetic in that it does NOT accept either networking or SSL settings
 * with its transport profile name.
 * NOTE the "_remote_cluster" profile name is special ONLY when the remote cluster port is enabled.
 * If the remote cluster port is not enabled, this profile name will be treated just as a normal profile (for BWC).
 *
 * When building SSL configurations for the transport profiles, assuming SSL is enabled,
 * this class builds a map that contains a configuration for each of the configured transport profiles
 * (keyed by its name).
 * The map also contains an entry that has the special key "default" and value being the SSL
 * configuration for the "default" profile.
 * If remote cluster is enabled, the map will also contain an entry that has the special key
 * "_remote_cluster" with the value being the SSL configuration of the synthetic "_remote_cluster" profile.
 *
 * NOTE the "_remote_cluster" profile only applies to the new remote cluster model.
 * The legacy remote cluster model mostly just uses the "default" transport profile.
 */
public final class ProfileConfigurations {

    private ProfileConfigurations() {}

    /**
     * Builds SSL configuration for transport profiles, including the default profile, any explicitly configured
     * profiles and synthetic profiles such as _remote_cluster.
     * NOTE the method builds SSL configurations that are intended for either server usage or server/client usage,
     * but not pure client usage.
     *
     * @param settings Settings of the ES node
     * @param sslService For resolving the SSL configuration based on its prefix
     * @param sslEnabledOnly If true, only include the SSL configuration if SSL is enabled for the profile.
     *                       If false, SSL configuration is included for a profile regardless whether SSL is actually enabled for it.
     * @return A map that contains {@link SslConfiguration} for each named transport profile as well
     *         as an entry for the "default" profile. If the remote_cluster feature is enabled, it also
     *         contains an entry for the synthetic "_remote_cluster" profile.
     */
    public static Map<String, SslConfiguration> get(Settings settings, SSLService sslService, boolean sslEnabledOnly) {
        final boolean transportSslEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
        final boolean remoteClusterPortEnabled = REMOTE_CLUSTER_SERVER_ENABLED.get(settings);
        final boolean remoteClusterServerSslEnabled = remoteClusterPortEnabled && REMOTE_CLUSTER_SERVER_SSL_ENABLED.get(settings);

        final Map<String, SslConfiguration> profileConfigurations = new HashMap<>();

        if (sslEnabledOnly) {
            if (transportSslEnabled == false && remoteClusterServerSslEnabled == false) {
                return profileConfigurations;
            } else if (transportSslEnabled == false) {
                // The single TRANSPORT_SSL_ENABLED setting determines whether SSL is enabled for both
                // the default transport profile and any custom transport profiles. That is, SSL is
                // always either enabled or disabled together for default and custom transport profiles.
                profileConfigurations.put(
                    REMOTE_CLUSTER_PROFILE,
                    sslService.getSSLConfiguration(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_PREFIX)
                );
                return profileConfigurations;
            } else if (remoteClusterServerSslEnabled == false) {
                populateFromTransportProfiles(settings, sslService, profileConfigurations);
                return profileConfigurations;
            }
        }

        // At this point, either SSL is enabled for both transport and remote cluster, or sslEnabledOnly is false.
        // In both case, we need to include all configurations
        populateFromTransportProfiles(settings, sslService, profileConfigurations);
        if (remoteClusterPortEnabled) {
            assert profileConfigurations.containsKey(REMOTE_CLUSTER_PROFILE) == false;
            profileConfigurations.put(
                REMOTE_CLUSTER_PROFILE,
                sslService.getSSLConfiguration(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_PREFIX)
            );
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
