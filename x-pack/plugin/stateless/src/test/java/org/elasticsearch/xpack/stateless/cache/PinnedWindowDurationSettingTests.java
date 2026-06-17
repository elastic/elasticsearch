/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_PINNED_WINDOW_DURATION_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class PinnedWindowDurationSettingTests extends ESTestCase {

    public void testDurationBelowMinimumRejected() {
        Settings settings = Settings.builder()
            .put(STATELESS_CACHE_BOOST_PREFERENCE_PINNED_WINDOW_DURATION_SETTING.getKey(), "10ms")
            .build();
        expectThrows(IllegalArgumentException.class, () -> STATELESS_CACHE_BOOST_PREFERENCE_PINNED_WINDOW_DURATION_SETTING.get(settings));
    }

    public void testPinnedWindowDurationUpdatesDynamically() throws IOException {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ClusterSettings clusterSettings = createClusterSettings(
            Settings.builder().put(STATELESS_CACHE_BOOST_PREFERENCE_PINNED_WINDOW_DURATION_SETTING.getKey(), "1d").build()
        );
        final Settings settings = Settings.builder()
            .put(STATELESS_CACHE_BOOST_PREFERENCE_PINNED_WINDOW_DURATION_SETTING.getKey(), "1d")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "1mb")
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "64kb")
            .put("path.home", createTempDir())
            .build();
        try (
            var clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings);
            var environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new StatelessSharedBlobCacheService(
                environment,
                settings,
                taskQueue.getThreadPool(),
                BlobCacheMetrics.NOOP,
                clusterService,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            )
        ) {
            assertThat(cacheService.getPinnedWindowDuration(), equalTo(TimeValue.timeValueDays(1)));

            clusterSettings.applySettings(
                Settings.builder().put(STATELESS_CACHE_BOOST_PREFERENCE_PINNED_WINDOW_DURATION_SETTING.getKey(), "12h").build()
            );
            assertThat(cacheService.getPinnedWindowDuration(), equalTo(TimeValue.timeValueHours(12)));
        }
    }

    private static ClusterSettings createClusterSettings(Settings settings) {
        Set<Setting<?>> clusterSettings = Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.add(STATELESS_CACHE_BOOST_PREFERENCE_PINNED_WINDOW_DURATION_SETTING);
        return new ClusterSettings(settings, clusterSettings);
    }
}
