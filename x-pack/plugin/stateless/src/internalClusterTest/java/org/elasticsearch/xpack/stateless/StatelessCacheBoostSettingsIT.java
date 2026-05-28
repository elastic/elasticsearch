/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.stateless.cache.StatelessCacheBoostSettings;

public class StatelessCacheBoostSettingsIT extends AbstractStatelessPluginIntegTestCase {

    public void testBoostConfiguration() {
        final var node0 = startMasterAndIndexNode();
        final var cacheBoostSettings = internalCluster().getInstance(StatelessCacheBoostSettings.class, node0);

        logger.info("--> logs: {}", cacheBoostSettings.getBoostConfigurationLogs());
        logger.info("--> metrics: {}", cacheBoostSettings.getBoostConfigurationMetrics());

        logger.info("--> update level1 search power to 50");
        updateClusterSettings(
            Settings.builder()
                // GroupSetting supports updating a single field
                .put(StatelessCacheBoostSettings.BOOST_CONFIGURATION_LOGS.getKey() + "level1.search_power", 50)
                // RichValueObject setting must be updated in its entirety
                .put(StatelessCacheBoostSettings.BOOST_CONFIGURATION_METRICS.getKey(), """
                    {
                      "level0": { "age": "1d", "search_power": 200},
                      "level1": { "age": "7d", "search_power": 50},
                      "level2": { "age": "365d", "search_power": 10}
                    }""")

        );

        logger.info("--> logs: {}", cacheBoostSettings.getBoostConfigurationLogs());
        logger.info("--> metrics: {}", cacheBoostSettings.getBoostConfigurationMetrics());
    }
}
