/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Set;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.UNKNOWN_TIMESTAMP;
import static org.elasticsearch.xpack.stateless.cache.PinnedWindowEvictionPolicy.PINNED_WINDOW_DURATION_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class PinnedWindowEvictionPolicyTests extends ESTestCase {

    public void testDurationBelowMinimumRejected() {
        Settings settings = Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), "10ms").build();
        expectThrows(IllegalArgumentException.class, () -> PINNED_WINDOW_DURATION_SETTING.get(settings));
    }

    public void testPinnedWindowDurationUpdatesDynamically() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ClusterSettings clusterSettings = createClusterSettings(
            Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), "1d").build()
        );
        try (var clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings)) {
            final var policy = new PinnedWindowEvictionPolicy(clusterService);
            assertThat(policy.getPinnedWindowDuration(), equalTo(TimeValue.timeValueDays(1)));

            clusterSettings.applySettings(Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), "12h").build());
            assertThat(policy.getPinnedWindowDuration(), equalTo(TimeValue.timeValueHours(12)));
        }
    }

    public void testCanEvictAlwaysReturnsTrue() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ClusterSettings clusterSettings = createClusterSettings(Settings.EMPTY);
        try (var clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings)) {
            final var policy = new PinnedWindowEvictionPolicy(clusterService);
            final ShardId shardId = new ShardId("index", randomUUID(), 0);
            final CacheRegion<FileCacheKey> region = region(shardId, 1L, "file-a");
            final CacheRegion<FileCacheKey> incoming = region(shardId, 1L, "file-b");
            assertTrue(policy.canEvict(region, incoming));
        }
    }

    private static CacheRegion<FileCacheKey> region(ShardId shardId, long primaryTerm, String file) {
        return new CacheRegion<>() {
            @Override
            public FileCacheKey key() {
                return new FileCacheKey(shardId, primaryTerm, file);
            }

            @Override
            public long timestampMillis() {
                return UNKNOWN_TIMESTAMP;
            }
        };
    }

    private static ClusterSettings createClusterSettings(Settings settings) {
        Set<Setting<?>> clusterSettings = Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.add(PINNED_WINDOW_DURATION_SETTING);
        return new ClusterSettings(settings, clusterSettings);
    }
}
