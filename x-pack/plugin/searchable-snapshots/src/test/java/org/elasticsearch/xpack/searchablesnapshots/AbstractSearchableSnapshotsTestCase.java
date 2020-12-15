/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.SearchableSnapshotRecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.PersistentCache;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public abstract class AbstractSearchableSnapshotsTestCase extends ESIndexInputTestCase {

    private static final ClusterSettings CLUSTER_SETTINGS = new ClusterSettings(
        Settings.EMPTY,
        Sets.union(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            Set.of(
                CacheService.SNAPSHOT_CACHE_SIZE_SETTING,
                CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING,
                CacheService.SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING,
                CacheService.SNAPSHOT_CACHE_MAX_FILES_TO_SYNC_AT_ONCE_SETTING
            )
        )
    );

    protected ThreadPool threadPool;
    protected ClusterService clusterService;
    protected NodeEnvironment nodeEnvironment;

    @Before
    public void setUpTest() throws Exception {
        final DiscoveryNode node = new DiscoveryNode(
            "node",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        threadPool = new TestThreadPool(getTestName(), SearchableSnapshots.executorBuilders());
        clusterService = ClusterServiceUtils.createClusterService(threadPool, node, CLUSTER_SETTINGS);
        nodeEnvironment = newNodeEnvironment();
    }

    @After
    public void tearDownTest() throws Exception {
        IOUtils.close(nodeEnvironment, clusterService);
        assertTrue(ThreadPool.terminate(threadPool, 30L, TimeUnit.SECONDS));
    }

    /**
     * @return a new {@link CacheService} instance configured with default settings
     */
    protected CacheService defaultCacheService() {
        return new CacheService(Settings.EMPTY, clusterService, threadPool, new PersistentCache(nodeEnvironment));
    }

    /**
     * @return a new {@link CacheService} instance configured with random cache size and cache range size settings
     */
    protected CacheService randomCacheService() {
        final Settings.Builder cacheSettings = Settings.builder();
        if (randomBoolean()) {
            cacheSettings.put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), randomCacheSize());
        }
        if (randomBoolean()) {
            cacheSettings.put(CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(), randomCacheRangeSize());
        }
        if (randomBoolean()) {
            cacheSettings.put(CacheService.SNAPSHOT_CACHE_RECOVERY_RANGE_SIZE_SETTING.getKey(), randomCacheRangeSize());
        }
        if (randomBoolean()) {
            cacheSettings.put(
                CacheService.SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING.getKey(),
                TimeValue.timeValueSeconds(scaledRandomIntBetween(1, 120))
            );
        }
        return new CacheService(cacheSettings.build(), clusterService, threadPool, new PersistentCache(nodeEnvironment));
    }

    /**
     * @return a new {@link CacheService} instance configured with the given cache size and cache range size settings
     */
    protected CacheService createCacheService(final ByteSizeValue cacheSize, final ByteSizeValue cacheRangeSize) {
        return new CacheService(
            Settings.builder()
                .put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), cacheSize)
                .put(CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(), cacheRangeSize)
                .build(),
            clusterService,
            threadPool,
            new PersistentCache(nodeEnvironment)
        );
    }

    /**
     * Returns a random shard data path for the specified {@link ShardId}. The returned path can be located on any of the data node paths.
     */
    protected Path randomShardPath(ShardId shardId) {
        return randomFrom(nodeEnvironment.availableShardPaths(shardId));
    }

    /**
     * @return a random {@link ByteSizeValue} that can be used to set {@link CacheService#SNAPSHOT_CACHE_SIZE_SETTING}.
     * Note that it can return a cache size of 0.
     */
    protected static ByteSizeValue randomCacheSize() {
        return new ByteSizeValue(randomNonNegativeLong());
    }

    /**
     * @return a random {@link ByteSizeValue} that can be used to set {@link CacheService#SNAPSHOT_CACHE_RANGE_SIZE_SETTING}
     */
    protected static ByteSizeValue randomCacheRangeSize() {
        return new ByteSizeValue(
            randomLongBetween(CacheService.MIN_SNAPSHOT_CACHE_RANGE_SIZE.getBytes(), CacheService.MAX_SNAPSHOT_CACHE_RANGE_SIZE.getBytes())
        );
    }

    protected static SearchableSnapshotRecoveryState createRecoveryState(boolean finalizedDone) {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), 0),
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING,
            new RecoverySource.SnapshotRecoverySource(
                UUIDs.randomBase64UUID(),
                new Snapshot("repo", new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID())),
                Version.CURRENT,
                new IndexId("some_index", UUIDs.randomBase64UUID(random()))
            )
        );
        DiscoveryNode targetNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        SearchableSnapshotRecoveryState recoveryState = new SearchableSnapshotRecoveryState(shardRouting, targetNode, null);

        recoveryState.setStage(RecoveryState.Stage.INIT)
            .setStage(RecoveryState.Stage.INDEX)
            .setStage(RecoveryState.Stage.VERIFY_INDEX)
            .setStage(RecoveryState.Stage.TRANSLOG);
        recoveryState.getIndex().setFileDetailsComplete();
        if (finalizedDone) {
            recoveryState.setStage(RecoveryState.Stage.FINALIZE).setStage(RecoveryState.Stage.DONE);
        }
        return recoveryState;
    }

    /**
     * Wait for all operations on the threadpool to complete
     */
    protected static void assertThreadPoolNotBusy(ThreadPool threadPool) throws Exception {
        assertBusy(() -> {
            for (ThreadPoolStats.Stats stat : threadPool.stats()) {
                assertEquals(stat.getActive(), 0);
                assertEquals(stat.getQueue(), 0);
            }
        }, 30L, TimeUnit.SECONDS);
    }
}
