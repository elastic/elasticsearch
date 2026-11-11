/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Peer recovery must not depend on a single stateless thread pool having spare capacity: it must still complete while the
 * prewarm pool, or the shard read pool, of the target is fully occupied by an unrelated long-running task. The two pools
 * can fill the same cache ranges, so whichever one is available fills them.
 */
public class RecoveryWithSaturatedPoolsIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockRepository.Plugin.class);
        return plugins;
    }

    /**
     * Cache population and gap filling coordinate on the prewarm pool; recovery (including engine open and translog replay) must
     * still complete while that pool is fully occupied.
     */
    public void testPeerRecoveryCompletesWhilePrewarmPoolIsFullyOccupied() throws Exception {
        // Override random cache sizing from settingsForRoles; must fit multiple regions so warming runs populate.
        assertPeerRecoveryCompletesWhilePoolIsFullyOccupied(
            StatelessPlugin.PREWARM_THREAD_POOL,
            StatelessPlugin.PREWARM_THREAD_POOL_SETTING,
            ByteSizeValue.ofBytes(PAGE_SIZE),
            ByteSizeValue.ofMb(8)
        );
    }

    /**
     * Cache misses of the BCC header reads claim their gaps on the shard read pool, but the region 0 prewarm, which runs on the prewarm
     * pool, fills the same ranges, so recovery makes progress while the shard read pool is fully occupied. The regions are large enough
     * for the whole shard to be in region 0, and so for the engine to open without any other cache miss.
     */
    public void testPeerRecoveryCompletesWhileShardReadPoolIsFullyOccupied() throws Exception {
        assertPeerRecoveryCompletesWhilePoolIsFullyOccupied(
            StatelessPlugin.SHARD_READ_THREAD_POOL,
            StatelessPlugin.SHARD_READ_THREAD_POOL_SETTING,
            ByteSizeValue.ofMb(16),
            ByteSizeValue.ofMb(128)
        );
    }

    private void assertPeerRecoveryCompletesWhilePoolIsFullyOccupied(
        String occupiedPool,
        String occupiedPoolSetting,
        ByteSizeValue regionSize,
        ByteSizeValue cacheSize
    ) throws Exception {
        final Settings oneThreadPool = Settings.builder()
            .put(occupiedPoolSetting + ".core", 1)
            .put(occupiedPoolSetting + ".max", 1)
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), regionSize.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), regionSize.getStringRep())
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // hollow relocation targets do not prewarm region 0, so there would be nothing to fill the BCC header ranges in their place
            .put(HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), false)
            .build();

        final String sourceNode = startMasterAndIndexNode(oneThreadPool);
        final String targetNode = startIndexNode(oneThreadPool);

        final CountDownLatch releaseOccupier = new CountDownLatch(1);
        final CountDownLatch occupierStarted = new CountDownLatch(1);
        final ThreadPool targetThreadPool = internalCluster().getInstance(ThreadPool.class, targetNode);
        // Hold the only thread of the pool before any index workload queues tasks on it (single-threaded pool).
        targetThreadPool.executor(occupiedPool).execute(() -> {
            occupierStarted.countDown();
            safeAwait(releaseOccupier);
        });
        safeAwait(occupierStarted);

        final String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", targetNode)
            )
        );
        ensureGreen(indexName);

        indexDocs(indexName, randomIntBetween(50, 1000));
        flush(indexName);

        try {
            assertAcked(
                admin().indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode))
            );

            ensureGreen(indexName);
            assertThat(findIndexShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(targetNode)));
        } finally {
            releaseOccupier.countDown();
        }
    }
}
