/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class EvictionPolicyTests extends ESTestCase {

    private static long size(long numPages) {
        return numPages * SharedBytes.PAGE_SIZE;
    }

    private record TestKey(ShardId shardId, String file) implements SharedBlobCacheService.KeyBase {}

    /**
     * Tests the default eviction policy: all entries are evictable.
     * Fills the cache completely, then inserts new entries and verifies that old entries are evicted to make room.
     */
    public void testDefaultEvictionPolicy() throws IOException {
        final int numRegions = randomIntBetween(4, 100);
        final long regionSizeInBytes = size(100);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size((long) numRegions * 100)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSizeInBytes))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final ShardId shard1 = new ShardId("index1", randomUUID(), 0);
        final ShardId shard2 = new ShardId("index2", randomUUID(), 0);

        final var policy = new DefaultEvictionPolicy<TestKey>();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP,
                policy
            )
        ) {
            assertEquals(numRegions, cacheService.freeRegionCount());

            // Fill the cache with shard1 entries
            for (int i = 0; i < numRegions; i++) {
                var key = new TestKey(shard1, "file-" + i);
                cacheService.get(key, randomLongBetween(1, regionSizeInBytes - 1L), 0);
            }
            assertEquals(0, cacheService.freeRegionCount());
            assertThat(cacheService.countCachedRegions(key -> key.shardId().equals(shard1)), equalTo((long) numRegions));

            // Promote some entries to higher frequencies
            for (int i = 0; i < randomIntBetween(1, numRegions); i++) {
                var key = new TestKey(shard1, "file-" + randomIntBetween(0, numRegions - 1));
                cacheService.get(key, randomLongBetween(1, regionSizeInBytes - 1L), 0);
            }

            // Insert new entries from shard2 — the default policy evicts everything in a single pass
            final int newEntries = randomIntBetween(1, numRegions);
            for (int i = 0; i < newEntries; i++) {
                var key = new TestKey(shard2, "new-file-" + i);
                cacheService.get(key, randomLongBetween(1, regionSizeInBytes - 1L), 0);
            }

            // shard2 entries were all inserted (evicting shard1 entries)
            assertThat(cacheService.countCachedRegions(key -> key.shardId().equals(shard2)), equalTo((long) newEntries));
            // Total regions still equals numRegions
            assertThat(cacheService.countCachedRegions(key -> true), equalTo((long) numRegions));
        }
    }

    /**
     * Tests an eviction policy with quota-based protection.
     * <p>
     * The policy assigns quotas to "recent" and "old" tiers. An entry is evictable if its tier exceeds
     * its quota. When both tiers are at quota, the policy relaxes its criteria and evicts entries from
     * the same tier as the incoming data.
     */
    public void testQuotaBasedPolicy() throws IOException {
        final int numRegions = randomIntBetween(2, 5) * 100;
        final long regionSizeInBytes = size(100);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size((long) numRegions * 100)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSizeInBytes))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final long now = randomLongBetween(TimeUnit.DAYS.toMillis(365), TimeUnit.DAYS.toMillis(365 * 50));

        final ShardId recentShard = new ShardId("recent", randomUUID(), 0);
        final ShardId oldShard = new ShardId("old", randomUUID(), 0);

        final int recentQuota = (int) (numRegions * 0.75);
        final int oldQuota = numRegions - recentQuota;

        record TimestampKey(ShardId shardId, String file, long timestamp) implements SharedBlobCacheService.KeyBase {}

        var policy = new EvictionPolicy<TimestampKey>() {

            final AtomicInteger recentCount = new AtomicInteger(0);
            final AtomicInteger oldCount = new AtomicInteger(0);

            private boolean isRecent(TimestampKey key) {
                return key.timestamp() >= now;
            }

            @Override
            public boolean canEvict(CacheRegion<TimestampKey> region, CacheRegion<TimestampKey> incoming) {
                boolean regionIsRecent = isRecent(region.key());
                if (regionIsRecent && recentCount.get() > recentQuota) {
                    return true;
                }
                if (regionIsRecent == false && oldCount.get() > oldQuota) {
                    return true;
                }
                // No tier is over quota — relax: evict within same tier as incoming
                return regionIsRecent == isRecent(incoming.key());
            }

            @Override
            public void onCached(CacheRegion<TimestampKey> region) {
                if (isRecent(region.key())) {
                    recentCount.incrementAndGet();
                } else {
                    oldCount.incrementAndGet();
                }
            }

            @Override
            public void onEvicted(CacheRegion<TimestampKey> region) {
                if (isRecent(region.key())) {
                    recentCount.decrementAndGet();
                } else {
                    oldCount.decrementAndGet();
                }
            }
        };

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP,
                policy
            )
        ) {
            assertEquals(numRegions, cacheService.freeRegionCount());

            // Fill the entire cache with old data (timestamp = 2 days ago)
            final long oldTimestamp = now - TimeUnit.DAYS.toMillis(2);
            final Set<TimestampKey> oldKeys = new HashSet<>();
            for (int i = 0; i < numRegions; i++) {
                var cacheKey = new TimestampKey(oldShard, "old-file-" + i, oldTimestamp);
                cacheService.get(cacheKey, randomLongBetween(1, regionSizeInBytes - 1L), 0);
                oldKeys.add(cacheKey);
            }
            assertEquals(0, cacheService.freeRegionCount());
            assertThat(cacheService.countCachedRegions(key -> key.shardId().equals(oldShard)), equalTo((long) numRegions));

            // Promote some old regions to higher frequencies via random accesses
            int oldAccessRounds = randomInt(numRegions);
            for (int round = 0; round < oldAccessRounds; round++) {
                var randomKeys = randomNonEmptySubsetOf(oldKeys);
                for (var cacheKey : randomKeys) {
                    cacheService.get(cacheKey, randomLongBetween(1, regionSizeInBytes - 1L), 0);
                }
            }

            // Insert recent data (timestamp = now). The policy allows evicting old data
            // because old currently occupies numRegions > oldQuota regions, even those at high frequency.
            final Set<TimestampKey> recentKeys = new HashSet<>();
            for (int i = 0; i < recentQuota; i++) {
                var cacheKey = new TimestampKey(recentShard, "recent-file-" + i, rarely() ? now + randomLongBetween(0L, 60000L) : now);
                cacheService.get(cacheKey, randomLongBetween(1, regionSizeInBytes - 1L), 0);
                recentKeys.add(cacheKey);
            }

            // Old data should have been evicted down to its quota
            assertThat(cacheService.countCachedRegions(key -> key.shardId().equals(oldShard)), equalTo((long) oldQuota));

            // Recent data should fill its quota
            assertThat(cacheService.countCachedRegions(key -> key.shardId().equals(recentShard)), equalTo((long) recentQuota));

            // Promote random regions from both tiers to higher frequencies
            int mixedAccessRounds = randomIntBetween(1, numRegions);
            for (int round = 0; round < mixedAccessRounds; round++) {
                var randomKeys = randomNonEmptySubsetOf(randomFrom(oldKeys, recentKeys));
                for (var cacheKey : randomKeys) {
                    cacheService.get(cacheKey, randomLongBetween(1, regionSizeInBytes - 1L), 0);
                }
            }

            // Now insert more recent regions. Both tiers are at their quota so the policy
            // relaxes its criteria and evicts entries from the same tier as the incoming data.
            final int extraRecentRegions = randomIntBetween(1, numRegions);
            for (int i = 0; i < extraRecentRegions; i++) {
                var cacheKey = new TimestampKey(recentShard, "extra-recent-file-" + i, now);
                cacheService.get(cacheKey, randomLongBetween(1, regionSizeInBytes - 1L), 0);
            }

            // Each extra recent entry evicts an existing recent entry (same tier).
            // Old data is never touched — it stays at its quota.
            assertThat(cacheService.countCachedRegions(key -> key.shardId().equals(oldShard)), equalTo((long) oldQuota));
            assertThat(cacheService.countCachedRegions(key -> key.shardId().equals(recentShard)), equalTo((long) recentQuota));
        }
    }

    /**
     * Tests an eviction policy that makes decisions purely from the region's representative timestamp.
     */
    public void testTimestampBasedEvictionPolicy() throws IOException {
        final long numRegions = randomIntBetween(4, 50);
        final long regionSizeInBytes = size(100);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions * 100)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSizeInBytes))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();

        final long now = randomLongBetween(TimeUnit.DAYS.toMillis(365), TimeUnit.DAYS.toMillis(365 * 50));
        final long oldTimestamp = now - TimeUnit.DAYS.toMillis(10);
        final long olderThanCachedTimestamp = now - TimeUnit.DAYS.toMillis(20);

        final ShardId oldShard = new ShardId("old", randomUUID(), 0);
        final ShardId newShard = new ShardId("new", randomUUID(), 0);
        final ShardId olderShard = new ShardId("older", randomUUID(), 0);

        // "newer wins": evict the existing region only if it is not newer than the incoming data.
        final var policy = new EvictionPolicy<TestKey>() {
            @Override
            public boolean canEvict(CacheRegion<TestKey> region, CacheRegion<TestKey> incoming) {
                if (incoming.timestampMillis() == SharedBlobCacheService.UNKNOWN_TIMESTAMP) {
                    fail("incoming region must have a known timestamp in this test case");
                }
                return region.timestampMillis() <= incoming.timestampMillis();
            }

            @Override
            public void onCached(CacheRegion<TestKey> region) {}

            @Override
            public void onEvicted(CacheRegion<TestKey> region) {}
        };

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP,
                policy
            )
        ) {
            assertEquals(numRegions, cacheService.freeRegionCount());

            // Fill the cache with oldTimestamp.
            for (int i = 0; i < numRegions; i++) {
                cacheService.get(new TestKey(oldShard, "old-" + i), randomLongBetween(1, regionSizeInBytes - 1L), 0, oldTimestamp);
            }
            assertThat("cache should be full after inserting numRegions old-timestamp regions", cacheService.freeRegionCount(), equalTo(0));
            assertThat(
                "all old-timestamp regions should be cached after filling the cache",
                cacheService.countCachedRegions(key -> key.shardId().equals(oldShard)),
                equalTo(numRegions)
            );

            // Newer data evicts the older stamped regions (oldTimestamp <= now).
            for (int i = 0; i < numRegions; i++) {
                cacheService.get(new TestKey(newShard, "new-" + i), randomLongBetween(1, regionSizeInBytes - 1L), 0, now);
            }
            assertThat(
                "old-timestamp regions should have been evicted by newer incoming data",
                cacheService.countCachedRegions(key -> key.shardId().equals(oldShard)),
                equalTo(0L)
            );
            assertThat(
                "all newer regions should be cached after evicting the older ones",
                cacheService.countCachedRegions(key -> key.shardId().equals(newShard)),
                equalTo(numRegions)
            );

            // Data older than what is cached cannot evict the newer regions: every region refuses eviction, so allocation fails and
            // the older data is never cached.
            for (int i = 0; i < numRegions; i++) {
                final var olderKey = new TestKey(olderShard, "oldest-" + i);
                expectThrows(
                    AlreadyClosedException.class,
                    () -> cacheService.get(olderKey, randomLongBetween(1, regionSizeInBytes - 1L), 0, olderThanCachedTimestamp)
                );
            }
            assertThat(
                "older-than-cached data must not be cached because it cannot evict the newer regions",
                cacheService.countCachedRegions(key -> key.shardId().equals(olderShard)),
                equalTo(0L)
            );
            assertThat(
                "newer regions must remain cached; older incoming data cannot evict them",
                cacheService.countCachedRegions(key -> key.shardId().equals(newShard)),
                equalTo(numRegions)
            );
        }
    }
}
