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
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Peer recovery must not depend on the stateless prewarm pool having spare capacity. Cache population and gap filling
 * coordinate on the prewarm pool; recovery (including engine open and translog replay) must still complete while that
 * pool is fully occupied by an unrelated long-running task.
 */
public class RecoveryWithSaturatedPrewarmPoolIT extends AbstractStatelessPluginIntegTestCase {

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

    public void testPeerRecoveryCompletesWhilePrewarmPoolIsFullyOccupied() throws Exception {
        // Override random cache sizing from settingsForRoles (see class Javadoc); must fit multiple regions so warming runs populate.
        final ByteSizeValue regionSize = ByteSizeValue.ofBytes(PAGE_SIZE);
        final ByteSizeValue cacheSize = ByteSizeValue.ofMb(8);
        final Settings prewarmPoolOneThread = Settings.builder()
            .put(StatelessPlugin.PREWARM_THREAD_POOL_SETTING + ".core", 1)
            .put(StatelessPlugin.PREWARM_THREAD_POOL_SETTING + ".max", 1)
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), regionSize.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), regionSize.getStringRep())
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .build();

        final String sourceNode = startMasterAndIndexNode(prewarmPoolOneThread);
        final String targetNode = startIndexNode(prewarmPoolOneThread);

        final CountDownLatch releasePrewarmOccupier = new CountDownLatch(1);
        final CountDownLatch prewarmOccupierStarted = new CountDownLatch(1);
        final ThreadPool targetThreadPool = internalCluster().getInstance(ThreadPool.class, targetNode);
        // Hold the only prewarm thread before any index workload queues populate tasks on that pool (single-threaded pool).
        targetThreadPool.executor(StatelessPlugin.PREWARM_THREAD_POOL).execute(() -> {
            prewarmOccupierStarted.countDown();
            safeAwait(releasePrewarmOccupier);
        });
        safeAwait(prewarmOccupierStarted);

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

            // With reverted synchronous populate(), generic can block on gap fill while prewarm is held → deadlock / no green
            ensureGreen(indexName);
            assertThat(findIndexShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(targetNode)));
        } finally {
            releasePrewarmOccupier.countDown();
        }
    }
}
