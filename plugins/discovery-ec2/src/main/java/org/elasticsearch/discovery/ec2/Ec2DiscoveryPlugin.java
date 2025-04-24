/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.ec2;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class Ec2DiscoveryPlugin extends Plugin implements DiscoveryPlugin, ReloadablePlugin {

    private static final Logger logger = LogManager.getLogger(Ec2DiscoveryPlugin.class);
    public static final String EC2_SEED_HOSTS_PROVIDER_NAME = "ec2";

    static {
        SpecialPermission.check();
    }

    private final Settings settings;
    // protected for testing
    protected final AwsEc2Service ec2Service;

    public Ec2DiscoveryPlugin(Settings settings) {
        this(settings, new AwsEc2ServiceImpl());
    }

    @SuppressWarnings("this-escape")
    protected Ec2DiscoveryPlugin(Settings settings, AwsEc2ServiceImpl ec2Service) {
        this.settings = settings;
        this.ec2Service = ec2Service;
        // eagerly load client settings when secure settings are accessible
        reload(settings);
    }

    @Override
    public NetworkService.CustomNameResolver getCustomNameResolver(Settings _settings) {
        logger.debug("Register _ec2_, _ec2:xxx_ network names");
        return new Ec2NameResolver();
    }

    @Override
    public Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(TransportService transportService, NetworkService networkService) {
        return Map.of(EC2_SEED_HOSTS_PROVIDER_NAME, () -> new AwsEc2SeedHostsProvider(settings, transportService, ec2Service));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            // Register EC2 discovery settings: discovery.ec2
            Ec2ClientSettings.ACCESS_KEY_SETTING,
            Ec2ClientSettings.SECRET_KEY_SETTING,
            Ec2ClientSettings.SESSION_TOKEN_SETTING,
            Ec2ClientSettings.ENDPOINT_SETTING,
            Ec2ClientSettings.PROTOCOL_SETTING,
            Ec2ClientSettings.PROXY_HOST_SETTING,
            Ec2ClientSettings.PROXY_PORT_SETTING,
            Ec2ClientSettings.PROXY_SCHEME_SETTING,
            Ec2ClientSettings.PROXY_USERNAME_SETTING,
            Ec2ClientSettings.PROXY_PASSWORD_SETTING,
            Ec2ClientSettings.READ_TIMEOUT_SETTING,
            AwsEc2Service.HOST_TYPE_SETTING,
            AwsEc2Service.ANY_GROUP_SETTING,
            AwsEc2Service.GROUPS_SETTING,
            AwsEc2Service.AVAILABILITY_ZONES_SETTING,
            AwsEc2Service.NODE_CACHE_TIME_SETTING,
            AwsEc2Service.TAG_SETTING,
            // Register cloud node settings: cloud.node
            AwsEc2Service.AUTO_ATTRIBUTE_SETTING
        );
    }

    @Override
    public Settings additionalSettings() {
        return getAvailabilityZoneNodeAttributes(settings);
    }

    private static final String IMDS_AVAILABILITY_ZONE_PATH = "/latest/meta-data/placement/availability-zone";

    // pkg private for testing
    static Settings getAvailabilityZoneNodeAttributes(Settings settings) {
        if (AwsEc2Service.AUTO_ATTRIBUTE_SETTING.get(settings)) {
            try {
                return Settings.builder()
                    .put(
                        Node.NODE_ATTRIBUTES.getKey() + "aws_availability_zone",
                        AwsEc2Utils.getInstanceMetadata(IMDS_AVAILABILITY_ZONE_PATH)
                    )
                    .build();
            } catch (Exception e) {
                // this is lenient so the plugin does not fail when installed outside of ec2
                logger.error("failed to get metadata for [placement/availability-zone]", e);
            }
        }

        return Settings.EMPTY;
    }

    @Override
    public void close() throws IOException {
        ec2Service.close();
    }

    @Override
    public void reload(Settings settingsToLoad) {
        ec2Service.refreshAndClearCache(Ec2ClientSettings.getClientSettings(settingsToLoad));
    }
}
