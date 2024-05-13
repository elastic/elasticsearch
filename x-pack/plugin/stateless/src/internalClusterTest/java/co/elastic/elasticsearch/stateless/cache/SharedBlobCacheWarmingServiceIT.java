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

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.IndexingDiskController;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils.getObjectStoreMockRepository;
import static org.elasticsearch.test.MockLogAppender.assertThatLogger;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class SharedBlobCacheWarmingServiceIT extends AbstractStatelessIntegTestCase {

    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofKb(4);
    private static final ByteSizeValue CACHE_SIZE = ByteSizeValue.ofMb(8);

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        plugins.add(MockRepository.Plugin.class);
        plugins.add(InternalSettingsPlugin.class);
        plugins.add(ShutdownPlugin.class);
        return plugins;
    }

    public void testCacheIsWarmedOnCommitUpload() throws IOException {
        startMasterOnlyNode();

        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(256))
            .build();
        var indexNodeA = startIndexNode(cacheSettings);

        final String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(EngineConfig.USE_COMPOUND_FILE, randomBoolean())
            )
        );
        ensureGreen(indexName);

        IndexShard indexShard = findIndexShard(indexName);
        IndexEngine shardEngine = getShardEngine(indexShard, IndexEngine.class);
        final long generationToBlock = shardEngine.getCurrentGeneration() + 1;
        final var mockRepository = getObjectStoreMockRepository(internalCluster().getInstance(ObjectStoreService.class, indexNodeA));
        final var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNodeA);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        commitService.addConsumerForNewUploadedBcc(indexShard.shardId(), bccInfo -> {
            if (bccInfo.uploadedBcc().primaryTermAndGeneration().generation() == generationToBlock) {
                logger.info("--> block object store repository");
                mockRepository.setRandomControlIOExceptionRate(1.0);
                mockRepository.setRandomDataFileIOExceptionRate(1.0);
                mockRepository.setMaximumNumberOfFailures(Long.MAX_VALUE);
                mockRepository.setRandomIOExceptionPattern(".*" + StatelessCompoundCommit.blobNameFromGeneration(generationToBlock) + ".*");
                future.onResponse(null);
            }
        });

        indexDocs(indexName, 10);
        flush(indexName);
        future.actionGet();

        // Forces a read from the cache in case the refresh read occurred before the files were marked as uploaded
        assertThat(indexShard.store().readLastCommittedSegmentsInfo().getGeneration(), equalTo(generationToBlock));
    }

    @TestLogging(value = "co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService:DEBUG", reason = "verify debug output")
    public void testCacheIsWarmedBeforeIndexingShardRelocation() {
        startMasterOnlyNode();

        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .build();
        var indexNodeA = startIndexNode(cacheSettings);

        final String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(EngineConfig.USE_COMPOUND_FILE, randomBoolean())
            )
        );

        indexDocs(indexName, randomIntBetween(100, 10000));
        refresh(indexName);

        var indexNodeB = startIndexNode(cacheSettings);
        ensureStableCluster(3);

        failObjectStoreAndFetchFromIndexingNodeAfterPrewarming(indexName, indexNodeB);

        assertThatLogger(() -> {
            var shutdownNodeId = client().admin().cluster().prepareState().get().getState().nodes().resolveNode(indexNodeA).getId();
            assertAcked(
                client().execute(
                    PutShutdownNodeAction.INSTANCE,
                    new PutShutdownNodeAction.Request(
                        shutdownNodeId,
                        SingleNodeShutdownMetadata.Type.SIGTERM,
                        "Shutdown for cache warming test",
                        null,
                        null,
                        TimeValue.timeValueMinutes(randomIntBetween(1, 5))
                    )
                )
            );
            ensureGreen(indexName);
            assertThat(findIndexShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(indexNodeB)));
        },
            SharedBlobCacheWarmingService.class,
            new MockLogAppender.SeenEventExpectation(
                "notifies warming completed",
                SharedBlobCacheWarmingService.class.getCanonicalName(),
                Level.DEBUG,
                "* warming completed in *"
            )
        );
    }

    public void testCacheIsWarmedBeforeSearchShardRecovery() {
        startMasterOnlyNode();

        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .build();
        if (STATELESS_UPLOAD_DELAYED) {
            cacheSettings = Settings.builder()
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), randomIntBetween(1, 10))
                .put(cacheSettings)
                .build();
        }
        startIndexNode(cacheSettings);

        var searchNodeA = startSearchNode(cacheSettings);
        ensureStableCluster(3);

        final String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(EngineConfig.USE_COMPOUND_FILE, randomBoolean())
            )
        );

        long totalDocs = 0L;
        if (randomBoolean()) {
            int initialCommits = randomIntBetween(1, 3);
            for (int i = 0; i < initialCommits; i++) {
                int numDocs = randomIntBetween(1, 1_000);
                indexDocs(indexName, numDocs);
                flush(indexName);
                totalDocs += numDocs;
            }
        }

        final int iters = randomIntBetween(1, 5);
        for (int i = 0; i < iters; i++) {
            int numDocs = randomIntBetween(1, 1_000);
            indexDocs(indexName, numDocs);
            refresh(indexName);
            totalDocs += numDocs;
        }

        ensureGreen(indexName);

        // Verify that we performed pre-warming and don't need to hit the object store on searches
        failObjectStoreAndFetchFromIndexingNodeAfterPrewarming(indexName, searchNodeA);

        setReplicaCount(1, indexName);
        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName).setSize(0), totalDocs);

        var searchNodeB = startSearchNode(cacheSettings);
        ensureStableCluster(4);

        // The cache also gets pre-warmed when a shard gets relocated to a new node
        failObjectStoreAndFetchFromIndexingNodeAfterPrewarming(indexName, searchNodeB);
        shutdownNode(searchNodeA);
        ensureGreen(indexName);
        assertThat(findSearchShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(searchNodeB)));
        assertHitCount(prepareSearch(indexName).setSize(0), totalDocs);
    }

    public void testSearchNodeWarmingFromIndexingNodeInMixedUploadDelaySettings() {
        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1L))
            .build();
        boolean indexNodeUploadDelayed = randomBoolean();
        var indexNodeSettings = indexNodeUploadDelayed
            ? Settings.builder()
                .put(cacheSettings)
                .put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), true)
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
                .build()
            : cacheSettings;
        startMasterOnlyNode();
        startIndexNode(indexNodeSettings);

        final String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(EngineConfig.USE_COMPOUND_FILE, randomBoolean())
            )
        );
        ensureGreen(indexName);

        int totalDocs = randomIntBetween(1, 10);
        indexDocs(indexName, totalDocs);
        refresh(indexName);

        var searchNodeSettings = Settings.builder()
            .put(cacheSettings)
            .put(SharedBlobCacheWarmingService.STATELESS_BLOB_CACHE_WARMING_ALLOW_FETCH_FROM_INDEXING.getKey(), true)
            .build();
        var searchNode = startSearchNode(searchNodeSettings);
        ensureStableCluster(3);

        // When upload is delayed, instantiate latch for seeing at least one request to the indexing node for getting VBCC chunks
        CountDownLatch latch = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(searchNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                latch.countDown();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // After pre-warming, we fail when the search node tries to fetch from the object store or the indexing node
        failObjectStoreAndFetchFromIndexingNodeAfterPrewarming(indexName, searchNode);

        setReplicaCount(1, indexName);
        if (indexNodeUploadDelayed) {
            safeAwait(latch);
        }
        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName).setSize(0), totalDocs);
    }

    private static void shutdownNode(String indexNode) {
        var shutdownNodeId = client().admin().cluster().prepareState().get().getState().nodes().resolveNode(indexNode).getId();
        assertAcked(
            client().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(
                    shutdownNodeId,
                    SingleNodeShutdownMetadata.Type.SIGTERM,
                    "Shutdown for cache warming test",
                    null,
                    null,
                    TimeValue.timeValueMinutes(randomIntBetween(1, 5))
                )
            )
        );
    }

    private void failObjectStoreAndFetchFromIndexingNodeAfterPrewarming(String indexName, String node) {
        final long generationToBlock = getShardEngine(findIndexShard(indexName), IndexEngine.class).getCurrentGeneration();
        final var warmingService = (BlockingSharedBlobCacheWarmingService) internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(TestStateless.class)
            .findFirst()
            .orElseThrow(() -> new AssertionError(TestStateless.class.getName() + " plugin not found"))
            .getSharedBlobCacheWarmingService();
        final var mockRepositoryB = getObjectStoreMockRepository(internalCluster().getInstance(ObjectStoreService.class, node));
        final var transportService = MockTransportService.getInstance(node);
        warmingService.addListener(ActionListener.running(() -> {
            logger.info("--> fail object store repository after warming");
            mockRepositoryB.setRandomControlIOExceptionRate(1.0);
            mockRepositoryB.setRandomDataFileIOExceptionRate(1.0);
            mockRepositoryB.setMaximumNumberOfFailures(Long.MAX_VALUE);
            mockRepositoryB.setRandomIOExceptionPattern(".*" + StatelessCompoundCommit.blobNameFromGeneration(generationToBlock) + ".*");
            transportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                    assert false : "should not have sent a request for VBCC data to the indexing node but sent request " + request;
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }));
    }

    public static class TestStateless extends Stateless {

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool,
            Settings settings
        ) {
            return new BlockingSharedBlobCacheWarmingService(cacheService, threadPool, settings);
        }
    }

    private static class BlockingSharedBlobCacheWarmingService extends SharedBlobCacheWarmingService {

        private final CopyOnWriteArrayList<ActionListener<Void>> listeners = new CopyOnWriteArrayList<>();

        BlockingSharedBlobCacheWarmingService(StatelessSharedBlobCacheService cacheService, ThreadPool threadPool, Settings settings) {
            super(cacheService, threadPool, settings);
        }

        void addListener(ActionListener<Void> listener) {
            listeners.add(listener);
        }

        @Override
        protected void warmCache(IndexShard indexShard, StatelessCompoundCommit commit, ActionListener<Void> listener) {
            var wrappedListener = new SubscribableListener<Void>();
            for (ActionListener<Void> voidActionListener : listeners) {
                wrappedListener.addListener(voidActionListener);
            }
            wrappedListener.addListener(listener); // completed last
            super.warmCache(indexShard, commit, wrappedListener);
            safeAwait(wrappedListener);
        }
    }
}
