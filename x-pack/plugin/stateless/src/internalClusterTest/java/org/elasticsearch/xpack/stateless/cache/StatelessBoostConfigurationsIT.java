/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.BoostConfigurationsService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

public class StatelessBoostConfigurationsIT extends AbstractStatelessPluginIntegTestCase {

    public void testBoostConfigurationsBasic() {
        final var node0 = startMasterAndIndexNode();
        final var boostConfigurationsService = internalCluster().getInstance(BoostConfigurationsService.class, node0);
        final var threadPool = internalCluster().getInstance(ThreadPool.class, node0);

        // 1. Default settings - 6 days old data in bw0 and 20 days old data in bw1
        {
            final var boostWindow = boostConfigurationsService.getBoostWindow(
                null, // indexMetadata is not used for now, so null
                threadPool.absoluteTimeInMillis() - TimeValue.timeValueDays(6).millis() // 6 days ago
            );
            assertEquals("bw0", boostWindow.name());
            assertEquals(0.5d, boostWindow.cachedRatio(), 1e-9);
            assertEquals(TimeValue.timeValueDays(7), boostWindow.effectiveMaxAge());
        }
        {
            final var boostWindow = boostConfigurationsService.getBoostWindow(
                null, // indexMetadata is not used for now, so null
                threadPool.absoluteTimeInMillis() - TimeValue.timeValueDays(20).millis() // 20 days ago
            );
            assertEquals("bw1", boostWindow.name());
            assertEquals(0.05d, boostWindow.cachedRatio(), 1e-9);
            assertEquals(TimeValue.timeValueDays(3650), boostWindow.effectiveMaxAge());
        }

        // 2. Project overrides so that the 20 days old data is in bw0 and fully cached
        updateClusterSettings(
            Settings.builder()
                .put("_default.bw0.overrides.max_age", TimeValue.timeValueDays(30))
                .put("_default.bw0.overrides.boost_factor_min", 2.0)
                .normalizePrefix(BoostConfigurationsService.BOOST_CONFIGURATIONS_SETTING.getKey())
        );
        {
            final var boostWindow = boostConfigurationsService.getBoostWindow(
                null, // indexMetadata is not used for now, so null
                threadPool.absoluteTimeInMillis() - TimeValue.timeValueDays(20).millis() // 20 days ago
            );
            assertEquals("bw0", boostWindow.name());
            assertEquals(1.0d, boostWindow.cachedRatio(), 1e-9);
        }

        // 3. Remove project overrides and the 20 days old data is back to bw1 with low cached ratio
        updateClusterSettings(
            Settings.builder().putNull(BoostConfigurationsService.BOOST_CONFIGURATIONS_SETTING.getKey() + "_default.bw0.overrides.*")
        );
        {
            final var boostWindow = boostConfigurationsService.getBoostWindow(
                null, // indexMetadata is not used for now, so null
                threadPool.absoluteTimeInMillis() - TimeValue.timeValueDays(20).millis() // 20 days ago
            );
            assertEquals("bw1", boostWindow.name());
            assertEquals(0.05d, boostWindow.cachedRatio(), 1e-9);
            assertEquals(TimeValue.timeValueDays(3650), boostWindow.effectiveMaxAge());
        }
    }
}
