/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProvider;

import java.util.Map;
import java.util.stream.Collectors;

public class BoostConfigurationsService {

    private static final Logger logger = LogManager.getLogger(BoostConfigurationsService.class);

    public static final Setting<Settings> BOOST_CONFIGURATIONS_SETTING = Setting.groupSetting(
        "stateless.cache_boost_preference.boost_configurations.",
        BoostConfigurationsService::buildConfigurationMapFromSettings,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.ProjectScope // this is just a placeholder for now
    );

    public static final String DEFAULT_BOOST_CONFIGURATION_NAME = "_default";

    private volatile Map<String, BoostConfiguration> boostConfigurations;
    private final TimeProvider timeProvider;

    public BoostConfigurationsService(ClusterService clusterService, TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        clusterService.getClusterSettings().initializeAndWatch(BOOST_CONFIGURATIONS_SETTING, settings -> {
            final var updatedBoostConfigurations = buildConfigurationMapFromSettings(settings);
            logger.info("updating boost configurations from settings to {}", updatedBoostConfigurations);
            this.boostConfigurations = updatedBoostConfigurations;
        });
    }

    public BoostWindow getBoostWindow(IndexMetadata indexMetadata, long absoluteTimeInMillis) {
        final var ageInMillis = timeProvider.absoluteTimeInMillis() - absoluteTimeInMillis;

        // TODO: resolve index specific overrides
        final Map<String, BoostWindow.Overrides> overridesPerWindow = null;

        return getBoostConfiguration(indexMetadata).withOverrides(overridesPerWindow).getBoostWindow(ageInMillis);
    }

    // TODO: Add mapping for index to its boost configuration, e.g. based on IndexMode and boost overrides
    private BoostConfiguration getBoostConfiguration(IndexMetadata indexMetadata) {
        return boostConfigurations.get(DEFAULT_BOOST_CONFIGURATION_NAME);
    }

    private static Map<String, BoostConfiguration> buildConfigurationMapFromSettings(Settings settings) {
        return settings.getAsGroups()
            .entrySet()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> BoostConfiguration.fromSettings(entry.getKey(), entry.getValue()))
            );
    }
}
