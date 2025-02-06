/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.engine.HollowIndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.MergeMetrics;
import co.elastic.elasticsearch.stateless.engine.ThreadPoolMergeScheduler;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static co.elastic.elasticsearch.stateless.StatelessMergeIT.blockMergePool;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class HollowIndexShardsMergesIT extends AbstractStatelessIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(HollowIndexMergesTestPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    public static class HollowIndexMergesTestPlugin extends Stateless {
        private static AtomicReference<MergeFinder> mergeFinderRef = new AtomicReference<>();

        public HollowIndexMergesTestPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected StatelessSharedBlobCacheService createSharedBlobCacheService(
            NodeEnvironment nodeEnvironment,
            Settings settings,
            ThreadPool threadPool,
            BlobCacheMetrics blobCacheMetrics
        ) {
            // Use the DIRECT executor to be able to block blob store reads in the merge threads
            return new StatelessSharedBlobCacheService(
                nodeEnvironment,
                settings,
                threadPool,
                blobCacheMetrics,
                threadPool::relativeTimeInNanos
            );
        }

        @Override
        protected MergePolicy getMergePolicy(EngineConfig engineConfig) {
            return new FilterMergePolicy(super.getMergePolicy(engineConfig)) {
                @Override
                public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext)
                    throws IOException {
                    var findMergeFn = mergeFinderRef.get();
                    if (findMergeFn == null) {
                        return super.findMerges(mergeTrigger, segmentInfos, mergeContext);
                    }
                    return findMergeFn.findMerges(segmentInfos, mergeContext);
                }
            };
        }

        static void setTestMergeFinder(MergeFinder mergeFinder) {
            mergeFinderRef.set(mergeFinder);
        }

        static void disableTestMergeFinder() {
            mergeFinderRef.set(null);
        }

        interface MergeFinder {
            MergePolicy.MergeSpecification findMerges(SegmentInfos segmentInfos, MergePolicy.MergeContext mergeContext);
        }
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(ThreadPoolMergeScheduler.MERGE_THREAD_POOL_SCHEDULER.getKey(), true)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    public void testQueuedOrOnGoingMergesPreventHollowing() throws Exception {
        var maxNumberOfMergeThreads = 1;
        var indexNodeSettings = Settings.builder()
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(1))
            .put(Stateless.MERGE_THREAD_POOL_SETTING + ".max", maxNumberOfMergeThreads)
            .build();

        var indexNodeA = startMasterAndIndexNode(indexNodeSettings);
        var indexNodeB = startMasterAndIndexNode(indexNodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);

        var mergesRunningLatch = new CountDownLatch(maxNumberOfMergeThreads);
        var blockRunningMergesLatch = new CountDownLatch(1);
        setNodeRepositoryStrategy(indexNodeA, new StatelessMockRepositoryStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                if (Thread.currentThread().getName().contains(Stateless.MERGE_THREAD_POOL)) {
                    mergesRunningLatch.countDown();
                    safeAwait(blockRunningMergesLatch);
                }
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }
        });

        // When all merge threads are blocked, all the scheduled merges for the shard will be waiting in the queue to be executed.
        // In that scenario, the relocation should not hollow the shard.
        var blockAllMergeThreads = randomBoolean();
        var unblockMergePoolLatch = new CountDownLatch(1);
        if (blockAllMergeThreads) {
            var threadPool = internalCluster().getInstance(ThreadPool.class, indexNodeA);
            blockMergePool(threadPool, unblockMergePoolLatch);
        }

        final var segmentCountMergedTogether = 2;
        var mergesScheduledOnRegularFlushes = randomBoolean();
        if (mergesScheduledOnRegularFlushes) {
            HollowIndexMergesTestPlugin.setTestMergeFinder((segmentInfos, mergeContext) -> {
                var mergeCandidates = segmentInfos.asList()
                    .stream()
                    .filter(segmentCommitInfo -> mergeContext.getMergingSegments().contains(segmentCommitInfo) == false)
                    .toList();

                // We want to merge pairs of segments together, to trigger more than one merge
                if (mergeCandidates.size() < segmentCountMergedTogether) {
                    return null;
                }
                var mergeSpec = new MergePolicy.MergeSpecification();
                mergeSpec.add(new MergePolicy.OneMerge(mergeCandidates.stream().limit(segmentCountMergedTogether).toList()));
                return mergeSpec;
            });
        }

        // We want to have at least two merges running in case that the merges are triggered by regular flushes instead of by a force merge
        for (int i = 0; i < segmentCountMergedTogether * 2; i++) {
            indexDocs(indexName, between(50, 100));
            flush(indexName);
        }

        if (mergesScheduledOnRegularFlushes == false) {
            // We're not interested in the end result, just that it's executing while the relocation starts
            client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).execute();
        }

        if (blockAllMergeThreads || mergesScheduledOnRegularFlushes) {
            // Ensure that either all the merges are enqueued (i.e. when all merge threads are blocked) or at least one of the merges
            // is blocked when the merges are scheduled on flushes.
            assertMergesAreEnqueued(indexNodeA);
        }

        if (blockAllMergeThreads == false) {
            safeAwait(mergesRunningLatch);
        }

        if (randomBoolean()) {
            // Just to ensure that the engine considers that there's no active ingestion going on
            safeSleep(100);
            assertShardEngineIsInstanceOf(indexName, 0, indexNodeA, IndexEngine.class);
        }

        HollowIndexMergesTestPlugin.disableTestMergeFinder();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA));
        ensureGreen(indexName);

        if (blockAllMergeThreads) {
            unblockMergePoolLatch.countDown();
        } else {
            blockRunningMergesLatch.countDown();
        }

        assertShardEngineIsInstanceOf(indexName, 0, indexNodeB, IndexEngine.class);
    }

    public void testHollowShardsDoNotTriggerMerges() {
        var indexNodeSettings = Settings.builder()
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(1))
            .build();

        var indexNodeA = startMasterAndIndexNode(indexNodeSettings);
        var indexNodeB = startMasterAndIndexNode(indexNodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", indexNodeB).build());
        ensureGreen(indexName);

        indexDocs(indexName, between(50, 100));

        // Ensure that the shard becomes hollow
        safeSleep(100);

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA));
        ensureGreen(indexName);

        assertShardEngineIsInstanceOf(indexName, 0, indexNodeB, HollowIndexEngine.class);

        var numberOfFlushes = randomIntBetween(10, 20);
        for (int i = 0; i < numberOfFlushes; i++) {
            flush(indexName);
        }

        var numberOfForceMerges = randomIntBetween(5, 10);
        for (int i = 0; i < numberOfForceMerges; i++) {
            assertNoFailures(safeGet(client().admin().indices().prepareForceMerge(indexName).execute()));
        }

        var indicesStats = client().admin().indices().prepareStats(indexName).setMerge(true).get();
        var mergeCount = indicesStats.getIndices().get(indexName).getPrimaries().merge.getTotal();
        assertThat(mergeCount, is(equalTo(0L)));

        var nodesStatsResponse = client().admin().cluster().prepareNodesStats(indexNodeB).setThreadPool(true).get();
        assertThat(nodesStatsResponse.getNodes().size(), equalTo(1));
        var nodeStats = nodesStatsResponse.getNodes().get(0);
        var mergeThreadPoolStats = nodeStats.getThreadPool()
            .stats()
            .stream()
            .filter(s -> Stateless.MERGE_THREAD_POOL.equals(s.name()))
            .findAny()
            .get();

        assertThat(mergeThreadPoolStats.completed(), is(equalTo(0L)));
        assertThat(mergeThreadPoolStats.active(), is(equalTo(0)));
    }

    private static void assertShardEngineIsInstanceOf(String indexName, int shardId, String node, Class<?> engineType) {
        var indexShard = findIndexShard(resolveIndex(indexName), shardId, node);
        assertThat(indexShard.getEngineOrNull(), instanceOf(engineType));
    }

    private static void assertMergesAreEnqueued(String indexNode) throws Exception {
        assertBusy(
            () -> assertThat(internalCluster().getInstance(MergeMetrics.class, indexNode).getQueuedMergeSizeInBytes(), is(greaterThan(0L)))
        );
    }
}
