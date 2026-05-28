/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

import static org.elasticsearch.common.settings.Setting.Property.Dynamic;
import static org.elasticsearch.common.settings.Setting.Property.NodeScope;

public class StatelessCacheBoostSettings {

    // Local non-registered settings for validation, shared by both GroupSetting and RichValueObject setting
    private static final Setting<TimeValue> LEVEL_AGE = Setting.timeSetting("age", TimeValue.ZERO, TimeValue.ZERO);
    private static final Setting<Integer> LEVEL_SP = Setting.intSetting("search_power", 0, 0);

    // 1. Example for GroupSetting
    public static final Setting<Settings> BOOST_CONFIGURATION_LOGS = Setting.groupSetting(
        "stateless.boost_configuration.logs.",
        BoostConfiguration::buildBoostLevelsFromSettings,
        NodeScope,
        Dynamic
    );

    // 2. Example for RichValueObject Setting
    public static final Setting<BoostConfiguration> BOOST_CONFIGURATION_METRICS = new Setting<>("stateless.boost_configuration.metrics", """
        {
          "level0": { "age": "1d", "search_power": 200},
          "level1": { "age": "7d", "search_power": 100},
          "level2": { "age": "365d", "search_power": 10}
        }""", string -> BoostConfiguration.parseFromString("metrics", string), NodeScope, Dynamic);

    // Configuration classes
    public record BoostLevel(String name, TimeValue age, int searchPower) {}

    public record BoostConfiguration(String name, List<BoostLevel> levels) {

        public static BoostConfiguration parseFromString(String name, String configString) {
            final var settings = Settings.builder().loadFromSource(configString, XContentType.JSON).build();
            return new BoostConfiguration(name, buildBoostLevelsFromSettings(settings));
        }

        private static List<BoostLevel> buildBoostLevelsFromSettings(Settings settings) {
            return settings.getAsGroups().entrySet().stream().map(entry -> {
                final var levelSettings = entry.getValue();
                return new BoostLevel(entry.getKey(), LEVEL_AGE.get(levelSettings), LEVEL_SP.get(levelSettings));
            }).toList();
        }
    }

    private static final Logger logger = LogManager.getLogger(StatelessCacheBoostSettings.class);

    private volatile BoostConfiguration boostConfigurationLogs;
    private volatile BoostConfiguration boostConfigurationMetrics;

    public StatelessCacheBoostSettings(ClusterService clusterService) {
        clusterService.getClusterSettings().initializeAndWatch(BOOST_CONFIGURATION_LOGS, settings -> {
            this.boostConfigurationLogs = new BoostConfiguration("logs", BoostConfiguration.buildBoostLevelsFromSettings(settings));
            logger.info("--> Updated boost configuration logs to : {}", boostConfigurationLogs);
        });

        clusterService.getClusterSettings().initializeAndWatch(BOOST_CONFIGURATION_METRICS, value -> {
            this.boostConfigurationMetrics = value;
            logger.info("--> Updated boost configuration metrics to : {}", boostConfigurationMetrics);
        });
    }

    public BoostConfiguration getBoostConfigurationLogs() {
        return boostConfigurationLogs;
    }

    public BoostConfiguration getBoostConfigurationMetrics() {
        return boostConfigurationMetrics;
    }
}
