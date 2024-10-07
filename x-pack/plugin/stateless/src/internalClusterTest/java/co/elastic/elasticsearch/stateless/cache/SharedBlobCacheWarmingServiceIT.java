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
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService.Type;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.UnsafePlainActionFuture;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService.TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING;
import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils.getObjectStoreMockRepository;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SharedBlobCacheWarmingServiceIT extends AbstractStatelessIntegTestCase {

    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofBytes(4L * PAGE_SIZE);
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
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(256))
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
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
        final var mockRepository = getObjectStoreMockRepository(getObjectStoreService(indexNodeA));
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
    public void testCacheIsWarmedBeforeIndexingShardRelocation_AfterHandoff() {
        startMasterOnlyNode();

        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            // TODO ES-9345 The test waits for warming to complete and then fails the object store accesses, but once ES-9345 is merged
            // it will need to wait for the shard to be started for that.
            .put(StatelessCommitService.STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), false)
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

        failObjectStoreAndFetchFromIndexingNodeAfterPrewarming(indexName, indexNodeB, Type.INDEXING);

        assertThatLogger(() -> {
            var shutdownNodeId = client().admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .nodes()
                .resolveNode(indexNodeA)
                .getId();
            assertAcked(
                client().execute(
                    PutShutdownNodeAction.INSTANCE,
                    new PutShutdownNodeAction.Request(
                        TEST_REQUEST_TIMEOUT,
                        TEST_REQUEST_TIMEOUT,
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
            expectCacheWarmingCompleteEvent(Type.INDEXING_EARLY),
            expectCacheWarmingCompleteEvent(Type.INDEXING)
        );
    }

    @TestLogging(value = "co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService:DEBUG", reason = "verify debug output")
    public void testCacheIsWarmedBeforeIndexingShardRelocation_Initial() {
        startMasterOnlyNode();

        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
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
        flushAndRefresh(indexName);

        var indexNodeB = startIndexNode(cacheSettings);
        ensureStableCluster(3);

        // Block start-relocation action until warming is complete (ensure warming entirely precedes shard-relocation)
        final var targetHasCompletedInitialPreWarming = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(indexNodeB);
        transportService.addSendBehavior(
            (Transport.Connection connection, long requestId, String action, TransportRequest request, TransportRequestOptions options) -> {
                if (TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME.equals(action)) {
                    safeAwait(targetHasCompletedInitialPreWarming);
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );

        PlainActionFuture<CompletedWarmingDetails> earlyWarmingIsComplete = new PlainActionFuture<>();
        runOnWarmingComplete(indexNodeB, Type.INDEXING_EARLY, earlyWarmingIsComplete);
        assertThatLogger(() -> {
            var shutdownNodeId = client().admin()
                .cluster()
                .prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .nodes()
                .resolveNode(indexNodeA)
                .getId();
            assertAcked(
                client().execute(
                    PutShutdownNodeAction.INSTANCE,
                    new PutShutdownNodeAction.Request(
                        TEST_REQUEST_TIMEOUT,
                        TEST_REQUEST_TIMEOUT,
                        shutdownNodeId,
                        SingleNodeShutdownMetadata.Type.SIGTERM,
                        "Shutdown for cache warming test",
                        null,
                        null,
                        TimeValue.timeValueMinutes(randomIntBetween(1, 5))
                    )
                )
            );

            CompletedWarmingDetails earlyWarmingDetails = safeGet(earlyWarmingIsComplete);
            logger.info("Early-warmed to generation {}, unblocking start-relocation action", earlyWarmingDetails.commit().generation());
            // block access to objects from the generation we warmed before post-handoff pre-warming starts
            blockAccessToGenerationBeforePostHandoffPreWarmingStarts(indexNodeB, earlyWarmingDetails.commit().generation());
            // unblock start-relocation action
            targetHasCompletedInitialPreWarming.countDown();

            // Now the relocation should complete successfully
            ensureGreen(indexName);
            assertThat(findIndexShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(indexNodeB)));
        },
            SharedBlobCacheWarmingService.class,
            expectCacheWarmingCompleteEvent(Type.INDEXING_EARLY),
            expectCacheWarmingCompleteEvent(Type.INDEXING)
        );
    }

    @TestLogging(value = "co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService:DEBUG", reason = "verify debug output")
    public void testCacheIsWarmedBeforeIndexingShardRecovery() {
        startMasterOnlyNode();

        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .build();
        var indexNode = startIndexNode(cacheSettings);

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

        ensureStableCluster(2);

        failObjectStoreAndFetchFromIndexingNodeAfterPrewarming(indexName, indexNode, Type.INDEXING);

        assertThatLogger(() -> {
            try {
                internalCluster().restartNode(indexNode);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            ensureGreen(indexName);
            assertThat(findIndexShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(indexNode)));
        },
            SharedBlobCacheWarmingService.class,
            // no "indexing early" warming here because it's a recovery not a relocation
            expectCacheWarmingCompleteEvent(Type.INDEXING)
        );
    }

    public void testCacheIsWarmedBeforeSearchShardRecovery() {
        startMasterOnlyNode();

        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), randomIntBetween(1, 10))
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .build();
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
        failObjectStoreAndFetchFromIndexingNodeAfterPrewarming(indexName, searchNodeA, Type.SEARCH);

        setReplicaCount(1, indexName);
        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName).setSize(0), totalDocs);

        stopFailingObjectStore(searchNodeA);
        disableTransportBlocking(searchNodeA);
        ensureSearchHits(indexName, totalDocs);

        var searchNodeB = startSearchNode(cacheSettings);
        ensureStableCluster(4);

        // The cache also gets pre-warmed when a shard gets relocated to a new node
        failObjectStoreAndFetchFromIndexingNodeAfterPrewarming(indexName, searchNodeB, Type.SEARCH);
        shutdownNode(searchNodeA);
        ensureGreen(indexName);
        assertThat(findSearchShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(searchNodeB)));
        assertHitCount(prepareSearch(indexName).setSize(0), totalDocs);

        stopFailingObjectStore(searchNodeB);
        disableTransportBlocking(searchNodeB);
        ensureSearchHits(indexName, totalDocs);
    }

    public void testCacheIsWarmedBeforeSearchShardRecoveryWhenVBCCGetsUploaded() {
        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE.getStringRep())
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 10)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), "1g")
            // Fetch size must be smaller or equal to the minimized pre-warm range (default to 1/4 region size). See also ES-9185
            .put(TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(PAGE_SIZE))
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .build();
        final var indexNode = startMasterAndIndexNode(cacheSettings);

        var searchNode = startSearchNode(cacheSettings);
        ensureStableCluster(2);

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

        var shardId = findIndexShard(indexName).shardId();
        var statelessCommitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        assertNotNull(statelessCommitService.getCurrentVirtualBcc(shardId));

        // Do not update latest uploaded info on new commit notifications so that the search node is unaware that the VBCC got uploaded
        // The MutableObjectStoreUploadTracker should be updated by the SwitchingCacheBlobReader when receiving the first
        // uploaded exception and that is how it will then retry from the object store.
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                NewCommitNotificationRequest notificationRequest = (NewCommitNotificationRequest) request;
                var routingTable = mock(IndexShardRoutingTable.class);
                when(routingTable.shardId()).thenReturn(shardId);
                NewCommitNotificationRequest alteredRequest = new NewCommitNotificationRequest(
                    routingTable,
                    notificationRequest.getCompoundCommit(),
                    notificationRequest.getBatchedCompoundCommitGeneration(),
                    null,
                    notificationRequest.getClusterStateVersion(),
                    notificationRequest.getNodeId()
                );
                handler.messageReceived(alteredRequest, channel, task);
            });

        // Upload VBCC on first message to get a chunk from the indexing node. This will return a ResourceAlreadyUploadedException and will
        // make the warming service to fetch from the object store.
        final var flushed = new AtomicBoolean(false);
        final var flushCountdown = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                assert ThreadPool.assertCurrentThreadPool(Stateless.PREWARM_THREAD_POOL);
                if (flushed.compareAndExchange(false, true) == false) {
                    // Spawn a new thread to avoid using flush with the prewarm thread pool which could trigger a false assertion
                    // See https://github.com/elastic/elasticsearch-serverless/issues/2518
                    final Thread thread = new Thread(() -> {
                        flush(indexName);
                        flushCountdown.countDown();
                    });
                    thread.start();
                    try {
                        thread.join();
                    } catch (InterruptedException e) {
                        fail(e, "interrupted while waiting for flush to complete");
                    }
                }
                // This point may block some transport threads.
                safeAwait(flushCountdown);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(
                TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public void sendResponse(Exception exception) {
                        channel.sendResponse(exception);
                    }

                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        assert false : "unexpectedly trying to send response " + response;
                    }
                }, task)
            );

        failObjectStoreAndFetchFromIndexingNodeAfterPrewarming(indexName, searchNode, Type.SEARCH);

        setReplicaCount(1, indexName);
        ensureGreen(indexName);
        assertTrue(flushed.get());
        assertHitCount(prepareSearch(indexName).setSize(0), totalDocs);

        stopFailingObjectStore(searchNode);
        disableTransportBlocking(searchNode);
        ensureSearchHits(indexName, totalDocs);
    }

    /**
     * An {@link org.elasticsearch.test.MockLog.SeenEventExpectation} that ensure we've seen the log message
     * indicating that the cache warming with the provided description has completed.
     */
    private static MockLog.LoggingExpectation expectCacheWarmingCompleteEvent(Type type) {
        return new MockLog.SeenEventExpectation(
            Strings.format("notifies that %s warming completed", type),
            SharedBlobCacheWarmingService.class.getCanonicalName(),
            Level.DEBUG,
            Strings.format("* %s warming completed in *", type)
        );
    }

    private static void shutdownNode(String indexNode) {
        var shutdownNodeId = client().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .nodes()
            .resolveNode(indexNode)
            .getId();
        assertAcked(
            client().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
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

    private void blockAccessToGenerationBeforePostHandoffPreWarmingStarts(String node, long generationToBlock) {
        final var mockRepositoryB = getObjectStoreMockRepository(getObjectStoreService(node));
        getSharedBlobCacheWarmingService(node).addBeforeWarmingStartsListener(warmingType -> {
            logger.info("Disabling object store access to generation {}", generationToBlock);
            if (Type.INDEXING == warmingType) {
                mockRepositoryB.setRandomControlIOExceptionRate(1.0);
                mockRepositoryB.setRandomDataFileIOExceptionRate(1.0);
                mockRepositoryB.setMaximumNumberOfFailures(Long.MAX_VALUE);
                mockRepositoryB.setRandomIOExceptionPattern(
                    ".*" + StatelessCompoundCommit.blobNameFromGeneration(generationToBlock) + ".*"
                );
            }
        });
    }

    private void failObjectStoreAndFetchFromIndexingNodeAfterPrewarming(String indexName, String node, Type type) {
        final long generationToBlock = getShardEngine(findIndexShard(indexName), IndexEngine.class).getCurrentGeneration();
        final var mockRepository = getObjectStoreMockRepository(getObjectStoreService(node));
        final var transportService = MockTransportService.getInstance(node);
        runOnWarmingComplete(node, type, ActionListener.running(() -> {
            logger.info("--> fail object store repository after warming");
            mockRepository.setRandomControlIOExceptionRate(1.0);
            mockRepository.setRandomDataFileIOExceptionRate(1.0);
            mockRepository.setMaximumNumberOfFailures(Long.MAX_VALUE);
            mockRepository.setRandomIOExceptionPattern(".*" + StatelessCompoundCommit.blobNameFromGeneration(generationToBlock) + ".*");
            transportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]")) {
                    assert false : "should not have sent a request for VBCC data to the indexing node but sent request " + request;
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }));
    }

    private static void runOnWarmingComplete(String node, Type type, ActionListener<CompletedWarmingDetails> listener) {
        final var warmingService = getSharedBlobCacheWarmingService(node);
        warmingService.addWarmingCompletedListener(listener.delegateFailure((l, results) -> {
            if (type.equals(results.type())) {
                l.onResponse(results);
            }
        }));
    }

    private static BlockingSharedBlobCacheWarmingService getSharedBlobCacheWarmingService(String node) {
        return (BlockingSharedBlobCacheWarmingService) internalCluster().getInstance(PluginsService.class, node)
            .filterPlugins(TestStateless.class)
            .findFirst()
            .orElseThrow(() -> new AssertionError(TestStateless.class.getName() + " plugin not found"))
            .getSharedBlobCacheWarmingService();
    }

    public static class TestStateless extends Stateless {

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool,
            TelemetryProvider telemetryProvider,
            Settings settings
        ) {
            return new BlockingSharedBlobCacheWarmingService(cacheService, threadPool, telemetryProvider, settings);
        }
    }

    /**
     * The details of the warming that just completed
     */
    private record CompletedWarmingDetails(Type type, StatelessCompoundCommit commit) {}

    private static class BlockingSharedBlobCacheWarmingService extends SharedBlobCacheWarmingService {

        private final CopyOnWriteArrayList<ActionListener<CompletedWarmingDetails>> warmingCompletedListeners =
            new CopyOnWriteArrayList<>();
        private final CopyOnWriteArrayList<Consumer<Type>> beforeWarmingStartsListeners = new CopyOnWriteArrayList<>();

        BlockingSharedBlobCacheWarmingService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool,
            TelemetryProvider telemetryProvider,
            Settings settings
        ) {
            super(cacheService, threadPool, telemetryProvider, settings);
        }

        /**
         * These aren't {@link ActionListener}s because they never fail, and are called repeatedly
         */
        void addBeforeWarmingStartsListener(Consumer<Type> actionListener) {
            beforeWarmingStartsListeners.add(actionListener);
        }

        void addWarmingCompletedListener(ActionListener<CompletedWarmingDetails> listener) {
            warmingCompletedListeners.add(listener);
        }

        @Override
        protected void warmCache(
            Type type,
            IndexShard indexShard,
            StatelessCompoundCommit commit,
            BlobStoreCacheDirectory directory,
            ActionListener<Void> listener
        ) {
            var wrappedListener = new SubscribableListener<Void>();
            CompletedWarmingDetails results = new CompletedWarmingDetails(type, commit);
            for (ActionListener<CompletedWarmingDetails> voidActionListener : warmingCompletedListeners) {
                wrappedListener.addListener(voidActionListener.map(nothing -> results));
            }
            wrappedListener.addListener(listener); // completed last
            // notify beforeWarmingStartedListeners
            for (Consumer<Type> beforeWarmingStartsListener : beforeWarmingStartsListeners) {
                beforeWarmingStartsListener.accept(type);
            }
            super.warmCache(type, indexShard, commit, directory, wrappedListener);
            safeAwait(wrappedListener);
        }
    }

    // overload this to allow unsafe usage
    public static <T> T safeAwait(SubscribableListener<T> listener) {
        final var future = new UnsafePlainActionFuture<T>(ThreadPool.Names.GENERIC);
        listener.addListener(future);
        return safeGet(future);
    }

    private static void disableTransportBlocking(String node) {
        MockTransportService.getInstance(node)
            .addSendBehavior(
                (connection, requestId, action, request, options) -> connection.sendRequest(requestId, action, request, options)
            );
    }

    private static void stopFailingObjectStore(String node) {
        var mockRepository = getObjectStoreMockRepository(getObjectStoreService(node));
        mockRepository.setRandomControlIOExceptionRate(0.0);
        mockRepository.setRandomDataFileIOExceptionRate(0.0);
    }

    private static void ensureSearchHits(String indexName, long totalDocs) {
        var res = prepareSearch(indexName).setSize((int) totalDocs).get();
        try {
            assertEquals(totalDocs, res.getHits().getHits().length);
        } finally {
            res.decRef();
        }
    }
}
