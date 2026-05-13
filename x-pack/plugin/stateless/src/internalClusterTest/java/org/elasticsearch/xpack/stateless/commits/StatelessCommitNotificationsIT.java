/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.shard.GlobalCheckpointListeners;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import org.elasticsearch.xpack.stateless.action.NewCommitNotificationRequest;
import org.elasticsearch.xpack.stateless.action.TransportFetchShardCommitsInUseAction;
import org.elasticsearch.xpack.stateless.action.TransportNewCommitNotificationAction;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.engine.RefreshManagerService;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.lucene.IndexDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexService;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class StatelessCommitNotificationsIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        return plugins;
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        public final AtomicReference<CyclicBarrier> afterFlushBarrierRef = new AtomicReference<>();

        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Collection<Object> createComponents(PluginServices services) {
            final Collection<Object> components = super.createComponents(services);
            components.add(
                new PluginComponentBinding<>(
                    StatelessCommitService.class,
                    components.stream().filter(c -> c instanceof TestStatelessCommitService).findFirst().orElseThrow()
                )
            );
            return components;
        }

        @Override
        protected StatelessCommitService createStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            return new TestStatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            );
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            HollowShardsService hollowShardsService,
            SharedBlobCacheWarmingService sharedBlobCacheWarmingService,
            RefreshManagerService refreshManagerService,
            ReshardIndexService reshardIndexService,
            DocumentParsingProvider documentParsingProvider,
            IndexEngine.EngineMetrics engineMetrics
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                hollowShardsService,
                sharedBlobCacheWarmingService,
                refreshManagerService,
                reshardIndexService,
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                engineMetrics,
                statelessCommitService.getShardLocalCommitsTracker(engineConfig.getShardId()).shardLocalReadersTracker()
            ) {
                @Override
                protected void afterFlush(long generation) {
                    final CyclicBarrier barrier = afterFlushBarrierRef.get();
                    if (barrier != null) {
                        safeAwait(barrier);
                        safeAwait(barrier);
                    }
                    super.afterFlush(generation);
                }
            };
        }
    }

    public static class TestStatelessCommitService extends StatelessCommitService {

        public AtomicReference<CyclicBarrier> getMaxUploadedBccTermAndGenBarrierRef = new AtomicReference<>();

        public TestStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            super(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            );
        }

        @Override
        protected ShardCommitState createShardCommitState(
            ShardId shardId,
            long primaryTerm,
            BooleanSupplier inititalizingNoSearchSupplier,
            Supplier<MappingLookup> mappingLookupSupplier,
            TriConsumer<Long, GlobalCheckpointListeners.GlobalCheckpointListener, TimeValue> addGlobalCheckpointListenerFunction,
            Runnable triggerTranslogReplicator
        ) {
            return new ShardCommitState(
                shardId,
                primaryTerm,
                inititalizingNoSearchSupplier,
                mappingLookupSupplier,
                addGlobalCheckpointListenerFunction,
                triggerTranslogReplicator
            ) {
                @Override
                public PrimaryTermAndGeneration getMaxUploadedBccTermAndGen() {
                    final CyclicBarrier barrier = getMaxUploadedBccTermAndGenBarrierRef.get();
                    if (barrier != null) {
                        safeAwait(barrier);
                        safeAwait(barrier);
                    }
                    return super.getMaxUploadedBccTermAndGen();
                }
            };
        }
    }

    public void testAlwaysSendCommitNotificationOnCreation() throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder()
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 10)
                .build()
        );
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        indexDocs(indexName, between(10, 20));
        refresh(indexName);

        final TestStatelessPlugin testStateless = findPlugin(indexNode, TestStatelessPlugin.class);
        final CyclicBarrier afterFlushBarrier = new CyclicBarrier(2);
        testStateless.afterFlushBarrierRef.set(afterFlushBarrier);

        final IndexShard indexShard = findIndexShard(indexName);
        final IndexEngine indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        final TestStatelessCommitService commitService = (TestStatelessCommitService) indexEngine.getStatelessCommitService();
        final CyclicBarrier getMaxUploadedBccTermAndGenBarrier = new CyclicBarrier(2);
        commitService.getMaxUploadedBccTermAndGenBarrierRef.set(getMaxUploadedBccTermAndGenBarrier);

        final List<NewCommitNotificationRequest> requests = Collections.synchronizedList(new ArrayList<>());
        MockTransportService.getInstance(indexNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportNewCommitNotificationAction.NAME + "[u]")) {
                requests.add((NewCommitNotificationRequest) request);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final Thread flushThread = new Thread(() -> flush(indexName));
        flushThread.start();
        // Wait for the flush thread to enter afterFlush so that it is about to call ensureMaxGenerationToUploadForFlush
        // which uploads the current VBCC
        safeAwait(afterFlushBarrier);
        testStateless.afterFlushBarrierRef.set(null);

        indexDocs(indexName, between(10, 20));
        final Thread refreshThread = new Thread(() -> refresh(indexName));
        refreshThread.start();
        // Wait for the refresh thread to append the new commit and checking the maxUploadedBccTermAndGen for sending
        // out new commit notification on creation
        safeAwait(getMaxUploadedBccTermAndGenBarrier);
        commitService.getMaxUploadedBccTermAndGenBarrierRef.set(null);

        // Let the flush thread proceed to upload the VBCC
        safeAwait(afterFlushBarrier);
        // Wait till the commit notification to be sent due to the flush which is strictly after uploading the commit
        assertBusy(() -> assertFalse(requests.isEmpty()));
        // Let the refresh thread continue
        safeAwait(getMaxUploadedBccTermAndGenBarrier);

        flushThread.join();
        refreshThread.join();

        // Both notifications (on creation and on upload) are sent out
        assertBusy(() -> assertThat(requests, hasSize(2)));
        // The two notifications are identical because they are for the same VBCC.
        // The notification on creation is also an uploaded notification since the maxUploadedBccTermAndGen gets bumped
        // by the concurrent flush.
        assertTrue("request " + requests.get(0), requests.get(0).isUploaded());
        assertThat(
            "request " + requests.get(0),
            requests.get(0).getCompoundCommit().generation(),
            equalTo(indexEngine.getCurrentGeneration())
        );
        assertThat(
            "request " + requests.get(0),
            requests.get(0).getBatchedCompoundCommitGeneration(),
            equalTo(indexEngine.getCurrentGeneration() - 1L)
        );
        assertThat(requests.get(0), equalTo(requests.get(1)));

        ensureGreen(indexName);
    }

    public void testSkipCommitNotificationAndFetchCommitsInUseForDeletedIndex() throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder()
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
                .build()
        );
        final String searchNode = startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                .build()
        );
        ensureGreen(indexName);
        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);

        // Index and flush a commit so that it gets used by the search node
        indexDocs(indexName, between(10, 20));
        flush(indexName);
        final var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        assertThat(commitService.getAllSearchNodesRetainingCommitsForShard(shardId), not(empty()));

        indexDocs(indexName, between(10, 20));
        refresh(indexName);
        final var virtualBcc = commitService.getCurrentVirtualBcc(shardId);
        assertNotNull(virtualBcc);

        // Trigger a flush and wait for the upload to be blocked at calling the upload consumer then delete the index
        final var barrier = new CyclicBarrier(2);
        commitService.addConsumerForNewUploadedBcc(shardId, uploadedBccInfo -> {
            safeAwait(barrier);
            safeAwait(barrier);
        });
        final Thread thread = new Thread(() -> flush(indexName));
        thread.start();
        safeAwait(barrier);
        safeGet(indicesAdmin().prepareDelete(indexName).execute());

        final MockTransportService searchNodeTransportService = MockTransportService.getInstance(searchNode);
        searchNodeTransportService.addRequestHandlingBehavior(
            TransportFetchShardCommitsInUseAction.NAME + "[n]",
            (handler, request, channel, task) -> {
                throw new AssertionError("fetch commits-in-use should be skipped for deleted index");
            }
        );
        searchNodeTransportService.addRequestHandlingBehavior(
            TransportNewCommitNotificationAction.NAME + "[u]",
            (handler, request, channel, task) -> {
                throw new AssertionError("commit notification should be skipped for deleted index");
            }
        );

        // Unblock the consumer so that StatelessCommitService attempts to send the new commit notification. It should not trigger NPE.
        try (var mockLog = MockLog.capture(StatelessCommitService.class)) {
            mockLog.addExpectation(new MockLog.LoggingExpectation() {
                private final AtomicBoolean seen = new AtomicBoolean(false);

                @Override
                public void match(LogEvent event) {
                    if (event.getThrown() instanceof NullPointerException) {
                        seen.set(true);
                    }
                }

                @Override
                public void assertMatched() {
                    assertFalse("Unexpected NullPointerException", seen.get());
                }
            });

            safeAwait(barrier); // unblock the upload consumer
            // Wait for the VBCC to be closed which indicates the commit notification is processed
            assertBusy(() -> assertFalse(virtualBcc.hasReferences()));
            mockLog.assertAllExpectationsMatched(); // should not see NPE
        }
        thread.join(30_000);
        assertFalse(thread.isAlive());
    }

    /**
     * Verifies that after a BCC is uploaded the {@link VirtualBatchedCompoundCommit} stays reachable for chunk-fetch requests on the
     * indexing node until the search tier has acknowledged the new-uploaded-commit notification (or the configured timeout fires). This
     * ensures that a search shard recovering while the notification is in flight can still read commit data directly from the indexing
     * node rather than being forced to fall back to the blob store.
     * <p>
     * Also verifies that local Lucene files are freed immediately after upload (not deferred to the notification response):
     * {@code IndexDirectory.estimateSizeInBytes()} must reach zero while the notification is still blocked.
     */
    public void testVbccRemainsServableUntilSearchTierNotificationCompletes() throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder()
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                // Use a long timeout so only the notification response (not the timeout) ends the window in this test.
                .put(StatelessCommitService.STATELESS_COMMITS_RELEASE_FILES_AFTER_NOTIFICATION_TIMEOUT.getKey(), "30s")
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
                .build()
        );
        final String searchNode = startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        // Use the index node's shard explicitly; findIndexShard(name) may return the search shard on the other node.
        final var indexShard = internalCluster().getInstance(IndicesService.class, indexNode).indexServiceSafe(index).getShard(0);
        final var indexDir = IndexDirectory.unwrapDirectory(indexShard.store().directory());
        assertNotNull("IndexDirectory must be present on the index node", indexDir);
        final var indexShardPath = indexShard.shardPath().resolveIndex();

        final var uploadedInfoRef = new AtomicReference<StatelessCommitService.UploadedBccInfo>();
        commitService.addConsumerForNewUploadedBcc(shardId, info -> uploadedInfoRef.compareAndSet(null, info));

        // notificationArrivedLatch fires once the search node has received the upload notification (VBCC is in recentlyUploadedVbccs).
        // goAheadLatch is counted-down by the test to let the handler respond and trigger the after-notification cleanup.
        final var notificationArrivedLatch = new CountDownLatch(1);
        final var goAheadLatch = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                final var req = (NewCommitNotificationRequest) request;
                if (req.isUploaded()) {
                    notificationArrivedLatch.countDown(); // signal: notification arrived, VBCC in recentlyUploadedVbccs
                    safeAwait(goAheadLatch);              // hold: wait for test to finish asserting
                }
                handler.messageReceived(request, channel, task);
            });

        indexDocs(indexName, between(5, 10));
        final Thread flushThread = new Thread(() -> flush(indexName));
        flushThread.start();

        // Wait for the notification to reach the blocked search node.
        safeAwait(notificationArrivedLatch);
        assertNotNull("upload consumer must have fired", uploadedInfoRef.get());

        final var uploadedInfo = uploadedInfoRef.get();
        final long vbccGeneration = uploadedInfo.uploadedBcc().primaryTermAndGeneration().generation();
        final long primaryTerm = uploadedInfo.uploadedBcc().primaryTermAndGeneration().primaryTerm();

        // Build a minimal chunk request. 1 byte is enough to verify the VBCC is reachable.
        final var request = new GetVirtualBatchedCompoundCommitChunkRequest(shardId, primaryTerm, vbccGeneration, 0, 1, indexNode);

        // Must NOT throw: the VBCC is in recentlyUploadedVbccs and serves via openInputPreferLocal (local disk when available).
        final var output = new org.elasticsearch.common.io.stream.BytesStreamOutput();
        commitService.readVirtualBatchedCompoundCommitChunk(request, output);
        assertThat("chunk read must return at least one byte", output.size(), not(equalTo(0)));

        // Release the notification: the after-notification cleanup fires, closes the VBCC, and releases local file refs.
        goAheadLatch.countDown();
        flushThread.join(30_000);
        assertFalse(flushThread.isAlive());

        // Once the VBCC is removed from recentlyUploadedVbccs, chunk requests must fail. Use assertBusy because the
        // cleanup is dispatched asynchronously after the notification response is processed.
        assertBusy(
            () -> assertThrows(
                ResourceNotFoundException.class,
                () -> commitService.readVirtualBatchedCompoundCommitChunk(
                    request,
                    new org.elasticsearch.common.io.stream.BytesStreamOutput()
                )
            )
        );
        // After the VBCC closes, local file refs are released → files freed from disk.
        assertThat("local files must be freed after VBCC is closed", indexDir.estimateSizeInBytes(), equalTo(0L));
        for (String fileName : uploadedInfo.blobFileRanges().keySet()) {
            assertFalse("file [" + fileName + "] must be deleted after VBCC closes", Files.exists(indexShardPath.resolve(fileName)));
        }

        ensureGreen(indexName);
    }

    /**
     * Verifies that when the search tier never responds to the new-uploaded-commit notification, the configured timeout fires and
     * releases the {@link VirtualBatchedCompoundCommit}, making subsequent chunk-fetch requests fail. Also verifies that local Lucene
     * files are freed immediately after upload (not deferred to the timeout or notification).
     */
    public void testVbccReleasedAfterTimeoutWhenSearchTierNeverResponds() throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder()
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                // Short timeout so the VBCC is released quickly without waiting for a response.
                .put(StatelessCommitService.STATELESS_COMMITS_RELEASE_FILES_AFTER_NOTIFICATION_TIMEOUT.getKey(), "100ms")
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
                .build()
        );
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);
        final var shardId = new ShardId(index, 0);
        final var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        // Use the index node's shard explicitly; findIndexShard(name) may return the search shard on the other node.
        final var indexShard = internalCluster().getInstance(IndicesService.class, indexNode).indexServiceSafe(index).getShard(0);
        final var indexDir = IndexDirectory.unwrapDirectory(indexShard.store().directory());
        assertNotNull("IndexDirectory must be present on the index node", indexDir);
        final var indexShardPath = indexShard.shardPath().resolveIndex();

        final var uploadedInfoRef = new AtomicReference<StatelessCommitService.UploadedBccInfo>();
        commitService.addConsumerForNewUploadedBcc(shardId, info -> uploadedInfoRef.compareAndSet(null, info));

        indexDocs(indexName, between(5, 10));
        final Thread flushThread = new Thread(() -> flush(indexName));
        flushThread.start();

        assertBusy(() -> assertNotNull("upload consumer must have fired", uploadedInfoRef.get()));

        final var uploadedInfo = uploadedInfoRef.get();
        final long vbccGeneration = uploadedInfo.uploadedBcc().primaryTermAndGeneration().generation();
        final long primaryTerm = uploadedInfo.uploadedBcc().primaryTermAndGeneration().primaryTerm();

        final var request = new GetVirtualBatchedCompoundCommitChunkRequest(shardId, primaryTerm, vbccGeneration, 0, 1, indexNode);

        // After the timeout the VBCC is closed: local file refs released → files freed, chunk requests fail.
        assertBusy(() -> {
            assertThat("local files must be freed after timeout fires", indexDir.estimateSizeInBytes(), equalTo(0L));
            for (String fileName : uploadedInfo.blobFileRanges().keySet()) {
                assertFalse("file [" + fileName + "] must be deleted after timeout fires", Files.exists(indexShardPath.resolve(fileName)));
            }
            assertThrows(
                ResourceNotFoundException.class,
                () -> commitService.readVirtualBatchedCompoundCommitChunk(
                    request,
                    new org.elasticsearch.common.io.stream.BytesStreamOutput()
                )
            );
        });

        flushThread.join(30_000);
        assertFalse(flushThread.isAlive());
        ensureGreen(indexName);
    }

}
