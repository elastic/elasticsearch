/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryCommitTooNewException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.stateless.action.NewCommitNotificationRequest;
import org.elasticsearch.xpack.stateless.action.TransportNewCommitNotificationAction;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.TestStatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.RegisterCommitRequest;
import org.elasticsearch.xpack.stateless.recovery.RegisterCommitResponse;
import org.elasticsearch.xpack.stateless.recovery.TransportRegisterCommitForRecoveryAction;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationHandoffAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class CorruptionWhileRelocatingIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testMergeWhileRelocationCausesCorruption() throws Exception {

        Settings indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // Following test pauses relocation and tries to force merge (which needs to unhollow), thus deadlocking
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), Boolean.FALSE)
            .build();

        final var indexNode = startMasterAndIndexNode(indexNodeSettings);
        final var searchNode = startSearchNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1)
                // make sure nothing triggers flushes under the hood
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 0)
                .build()
        );
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);

        // Create multiple segments that must be large enough so that the compound commit never fits in a single cache region
        indexDocs(indexName, 1_000);
        flush(indexName);

        indexDocs(indexName, 1_000);
        flush(indexName);

        indexDocs(indexName, 1_000);
        flush(indexName);

        indexDocs(indexName, 1_000);
        flush(indexName);

        // No flush after this so that there is something to flush during relocation
        indexDocs(indexName, 1_000);

        var sourceShard = findIndexShard(index, 0, indexNode);

        // Values of primary term and generation before the relocation
        final var primaryTerm = sourceShard.getOperationPrimaryTerm();
        final var generation = sourceShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
        logger.info("--> before relocation primary term={} and generation={}", primaryTerm, generation);

        // Value of generation once the relocation is completed
        final var finalGeneration = generation + 1L /* flush before handoff on source */ + 1L /* flush after handoff on target */;

        final var receivedNotifications = new AtomicInteger(0);
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                assertThat(request, instanceOf(NewCommitNotificationRequest.class));
                var notification = (NewCommitNotificationRequest) request;
                if (notification.getTerm() == primaryTerm && notification.getGeneration() == finalGeneration && notification.isUploaded()) {
                    var count = receivedNotifications.incrementAndGet();
                    logger.info(
                        "--> search node received commit notification [primary term={}, generation={}, parent task={}]: {}",
                        notification.getTerm(),
                        notification.getGeneration(),
                        task.getParentTaskId(),
                        count
                    );
                }
                handler.messageReceived(request, channel, task);
            });

        final var finalCommitBlobName = BatchedCompoundCommit.blobNameFromGeneration(finalGeneration);

        // We want more commits to be made by the source shard while the relocation handoff is executing, so we block the handoff here
        var newIndexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        final var pauseHandoff = new CountDownLatch(1);
        final var resumeHandoff = new CountDownLatch(1);
        MockTransportService.getInstance(newIndexNode)
            .addRequestHandlingBehavior(
                PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {

                    private void await() {
                        pauseHandoff.countDown();
                        logger.info("--> relocation handoff paused");
                        safeAwait(resumeHandoff);
                        logger.info("--> relocation handoff resumed");
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        await();
                        channel.sendResponse(response);
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        await();
                        channel.sendResponse(exception);
                    }

                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }
                }, task)
            );

        logger.info("--> move index shard from: {} to: {}", indexNode, newIndexNode);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, indexNode, newIndexNode));

        logger.info("--> waiting for relocation handoff to be initiated");
        safeAwait(pauseHandoff);

        logger.info("--> now forcing a new merge on the source shard");
        ActionFuture<BroadcastResponse> mergeFuture = client(indexNode).admin()
            .indices()
            .prepareForceMerge(indexName)
            .setMaxNumSegments(1)
            .execute();

        // Pause to let merge potentially succeed
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(300));

        var objectStoreService = getCurrentMasterObjectStoreService();
        var blobContainer = objectStoreService.getProjectBlobContainer(sourceShard.shardId(), primaryTerm);

        // Check that the blob has not been uploaded
        assertFalse(blobContainer.blobExists(operationPurpose, finalCommitBlobName));

        Store sourceStore = sourceShard.store();

        logger.info("--> resuming relocation");
        resumeHandoff.countDown();

        logger.info("--> waiting for the commit to appear in blob store");
        assertBusy(() -> assertTrue(blobContainer.blobExists(operationPurpose, finalCommitBlobName)));

        BroadcastResponse mergeResponse = mergeFuture.actionGet();
        assertEquals("Force-merge failed on indexing shard", 1, mergeResponse.getSuccessfulShards());
        assertEquals(2, mergeResponse.getTotalShards());

        // wait for the source node to complete the hand-off too. Since it's response runs on generic, the test framework might fail
        // because it may not run. Todo: reevaluate generic use in `TransportStatelessPrimaryRelocationAction
        assertBusy(() -> assertEquals(0, sourceStore.refCount()));
    }

    public void testRelocationHandoffFailure() throws Exception {
        final var indexNode = startMasterAndIndexNode();
        startSearchNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1)
                // make sure nothing triggers flushes under the hood
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 0)
                .build()
        );
        ensureGreen(indexName);

        indexDocs(indexName, 1_000);

        // We want more commits to be made by the source shard while the relocation handoff is executing, so we block the handoff here
        var newIndexNode = startIndexNode();
        final var pauseHandoff = new CountDownLatch(1);
        MockTransportService.getInstance(newIndexNode)
            .addRequestHandlingBehavior(
                PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {

                    private void await() {
                        pauseHandoff.countDown();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        await();
                        // Swallow response as we want to kill the node before relocation succeeds
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        await();
                        channel.sendResponse(exception);
                    }

                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }
                }, task)
            );

        // Async index another 1000 documents during the relocation.
        Thread thread = new Thread(() -> {
            for (int i = 0; i < 10; ++i) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                var bulkRequest = client(indexNode).prepareBulk();
                for (int j = 0; j < 100; j++) {
                    bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
                }
                assertNoFailures(bulkRequest.get());
            }
        });

        boolean startImmediately = randomBoolean();

        if (startImmediately) {
            thread.start();
        }

        logger.info("--> move index shard from: {} to: {}", indexNode, newIndexNode);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, indexNode, newIndexNode));

        logger.info("--> waiting for relocation handoff to be initiated");
        safeAwait(pauseHandoff);

        if (startImmediately == false) {
            thread.start();
        }

        logger.info("--> stopping target node before relocation succeeds");
        internalCluster().stopNode(newIndexNode);

        logger.info("--> waiting for concurrently indexing documents to be completed");
        thread.join();

        refresh(indexName);

        assertResponse(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
            assertNoFailures(searchResponse);
            assertEquals(2000, searchResponse.getHits().getTotalHits().value());
        });
    }

    /// A search shard that registers for recovery while the primary is mid-handoff must not be given a commit whose
    /// generation is above `maxGenerationToUpload`. That commit will never be uploaded by the relocation source.
    ///
    /// The sequence forced here:
    /// - The old primary enters `RELOCATING`, pinning `maxGenerationToUpload = M`.
    /// - A force merge on the old node creates generation `M+1`, whose upload is paused for good. With a flushing force merge,
    ///   `M+1` is frozen and pending upload. Otherwise it is created by the post-merge refresh and usually stays in the current,
    ///   unfrozen VBCC, which is the VBCC a registration would otherwise hand out.
    /// - A recovering search shard registers with the old primary, which is still the primary in the routing table.
    /// - The handoff completes and generation `M+1` is discarded, never reaching the object store.
    ///
    public void testSearchShardRegistrationDuringRelocationStaysWithinMaxGenerationToUpload() throws Exception {
        final Settings indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), Boolean.FALSE)
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            // Refresh after every merge, so that a force merge without flush still creates a commit.
            .put(IndexEngine.MERGE_FORCE_REFRESH_SIZE.getKey(), ByteSizeValue.ZERO)
            .build();

        final var oldIndexNode = startMasterAndIndexNode(indexNodeSettings);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .build()
        );
        ensureGreen(indexName);

        // Several segments, so that the force merge below actually rewrites them into a new layout.
        indexDocs(indexName, 1_000);
        flush(indexName);
        indexDocs(indexName, 1_000);
        flush(indexName);

        final var index = resolveIndex(indexName);
        final var sourceShard = findIndexShard(index, 0, oldIndexNode);
        final var newIndexNode = startIndexNode(indexNodeSettings);

        // Hold the search shard's commit registration on the old node until the force merge has created a commit above
        // maxGenerationToUpload, then capture the generation the old node hands back.
        final var pauseRegistration = new CountDownLatch(1);
        final var resumeRegistration = new CountDownLatch(1);
        final var registrationResponse = new SubscribableListener<RegisterCommitResponse>();
        MockTransportService.getInstance(oldIndexNode)
            .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                pauseRegistration.countDown();
                safeAwait(resumeRegistration);
                handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public void sendResponse(TransportResponse response) {
                        if (response instanceof RegisterCommitResponse rcr && rcr.getCompoundCommit() != null) {
                            // Record the first registration, later completions are ignored by the SubscribableListener
                            registrationResponse.onResponse(rcr);
                        }
                        channel.sendResponse(response);
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        channel.sendResponse(exception);
                    }

                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }
                }, task);
            });

        // Hold the handoff response so that the old primary stays in RELOCATING while the force merge and the
        // search shard registration run.
        final var pauseHandoff = new CountDownLatch(1);
        final var resumeHandoff = new CountDownLatch(1);
        MockTransportService.getInstance(newIndexNode)
            .addRequestHandlingBehavior(
                PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public void sendResponse(TransportResponse response) {
                        pauseHandoff.countDown();
                        safeAwait(resumeHandoff);
                        channel.sendResponse(response);
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        pauseHandoff.countDown();
                        safeAwait(resumeHandoff);
                        channel.sendResponse(exception);
                    }

                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }
                }, task)
            );

        final var sourceCommitService = internalCluster().getInstance(StatelessCommitService.class, oldIndexNode);

        logger.info("--> moving index shard from {} to {}", oldIndexNode, newIndexNode);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, oldIndexNode, newIndexNode));
        try {
            logger.info("--> waiting for the relocation handoff to be paused");
            safeAwait(pauseHandoff);

            // The handoff request is only sent once the markRelocating listener has fired, so the bound is already pinned.
            final long maxGenerationToUpload = sourceCommitService.getMaxGenerationToUpload(sourceShard.shardId());
            assertThat("markRelocating pinned before the handoff request is sent", maxGenerationToUpload, lessThan(Long.MAX_VALUE));

            startSearchNode();
            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
            logger.info("--> waiting for the search shard registration to reach the old indexing node");
            safeAwait(pauseRegistration);

            logger.info("--> force merging on the old node to create a commit above maxGenerationToUpload");
            client(oldIndexNode).admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).setFlush(randomBoolean()).execute();

            assertBusy(
                () -> assertThat(
                    getMaxCurrentOrPendingUploadGeneration(sourceCommitService, sourceShard.shardId()),
                    greaterThan(maxGenerationToUpload)
                )
            );

            logger.info("--> resuming the search shard registration");
            resumeRegistration.countDown();

            final var response = safeAwait(registrationResponse);
            assertThat(
                "a commit above maxGenerationToUpload must never be handed to a recovering search shard",
                response.getCompoundCommit().generation(),
                lessThanOrEqualTo(maxGenerationToUpload)
            );

            logger.info("--> resuming the relocation handoff");
            resumeHandoff.countDown();
        } finally {
            resumeRegistration.countDown();
            resumeHandoff.countDown();
        }

        ensureGreen(indexName);
        assertResponse(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
            assertNoFailures(searchResponse);
            assertEquals(2000, searchResponse.getHits().getTotalHits().value());
        });
    }

    private static long getMaxCurrentOrPendingUploadGeneration(StatelessCommitService commitService, ShardId shardId) {
        final var currentVirtualBcc = commitService.getCurrentVirtualBcc(shardId);
        return Math.max(
            currentVirtualBcc != null ? currentVirtualBcc.getMaxGeneration() : -1L,
            commitService.getMaxPendingOrUploadedGeneration(shardId)
        );
    }

    public void testNewCommitNotificationWaitsForUploadBound() {
        final boolean mergeCommitLandsBelowBound = randomBoolean();
        final Settings indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), Boolean.FALSE)
            .put(IndexEngine.MERGE_FORCE_REFRESH_SIZE.getKey(), ByteSizeValue.ofGb(1))
            // Only freeze a VBCC for upload when a flush asks for it
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
            .build();

        final var oldIndexNode = startMasterAndIndexNode(indexNodeSettings);
        final var searchNode = startSearchNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .build()
        );
        ensureGreen(indexName);

        indexDocs(indexName, 100);
        flush(indexName);
        indexDocs(indexName, 100);
        flush(indexName);
        refresh(indexName);

        final var index = resolveIndex(indexName);
        final var sourceShard = findIndexShard(index, 0, oldIndexNode);
        final var shardId = sourceShard.shardId();
        final var sourceEngine = (IndexEngine) sourceShard.getEngineOrNull();
        final var sourceCommitService = (TestStatelessCommitService) sourceEngine.getStatelessCommitService();
        final String oldNodeEphemeralId = internalCluster().getInstance(ClusterService.class, oldIndexNode).localNode().getEphemeralId();

        // Only track notifications from the old primary: the new primary may reuse the merge generation for its own commits
        final Set<Long> generationsNotifiedBySource = ConcurrentHashMap.newKeySet();
        final Set<Long> nonUploadGenerationsNotifiedBySource = ConcurrentHashMap.newKeySet();
        final AtomicLong mergeGeneration = new AtomicLong(-1L);
        final var mergeNotified = new SubscribableListener<Void>();
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                if (request instanceof NewCommitNotificationRequest notificationRequest
                    && notificationRequest.getCompoundCommit().nodeEphemeralId().equals(oldNodeEphemeralId)) {
                    final long generation = notificationRequest.getCompoundCommit().generation();
                    generationsNotifiedBySource.add(generation);
                    if (notificationRequest.isUploaded() == false) {
                        nonUploadGenerationsNotifiedBySource.add(generation);
                    }
                    if (generation == mergeGeneration.get()) {
                        mergeNotified.onResponse(null);
                    }
                }
                handler.messageReceived(request, channel, task);
            });

        final var markRelocatingCalled = new CountDownLatch(1);
        final var resumeMarkRelocating = new CountDownLatch(1);
        final var markRelocatingDone = new CountDownLatch(1);
        sourceCommitService.setStrategy(new TestStatelessCommitService.Strategy() {
            @Override
            public ActionListener<Void> markRelocating(
                Supplier<ActionListener<Void>> originalSupplier,
                ShardId shardId,
                long minRelocatedGeneration,
                ActionListener<Void> listener
            ) {
                markRelocatingCalled.countDown();
                safeAwait(resumeMarkRelocating);
                try {
                    return originalSupplier.get();
                } finally {
                    markRelocatingDone.countDown();
                }
            }
        });

        final var newIndexNode = startIndexNode(indexNodeSettings);
        logger.info("--> moving index shard from {} to {}", oldIndexNode, newIndexNode);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, oldIndexNode, newIndexNode));

        final var mergeUploadBlocked = new CountDownLatch(1);
        final var releaseCommitUploads = new CountDownLatch(1);
        try {
            logger.info("--> waiting for markRelocating to be called");
            safeAwait(markRelocatingCalled);
            assertTrue(sourceCommitService.relocationUploadBoundIsInstalled(shardId));
            assertThat("bound is not yet pinned", sourceCommitService.getMaxGenerationToUpload(shardId), equalTo(Long.MAX_VALUE));

            final long generationBeforeMerge = sourceEngine.getCurrentGeneration();
            // Block commit uploads on the source, so that the merge commit can only reach the search shard through the
            // new-commit notification, and fail if a merge commit above the bound is ever uploaded.
            setNodeRepositoryStrategy(oldIndexNode, new StatelessMockRepositoryStrategy() {
                @Override
                public void blobContainerWriteBlobAtomic(
                    CheckedRunnable<IOException> originalRunnable,
                    OperationPurpose purpose,
                    String blobName,
                    long blobSize,
                    BlobContainer.BlobMultiPartInputStreamProvider provider,
                    boolean failIfAlreadyExists,
                    Executor executor
                ) throws IOException {
                    maybeBlockCommitUpload(blobName);
                    super.blobContainerWriteBlobAtomic(
                        originalRunnable,
                        purpose,
                        blobName,
                        blobSize,
                        provider,
                        failIfAlreadyExists,
                        executor
                    );
                }

                @Override
                public void blobContainerWriteBlobAtomic(
                    CheckedRunnable<IOException> originalRunnable,
                    OperationPurpose purpose,
                    String blobName,
                    InputStream inputStream,
                    long blobSize,
                    boolean failIfAlreadyExists
                ) throws IOException {
                    maybeBlockCommitUpload(blobName);
                    super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
                }

                private void maybeBlockCommitUpload(String blobName) {
                    if (BatchedCompoundCommit.startsWithBlobPrefix(blobName)) {
                        if (BatchedCompoundCommit.parseGenerationFromBlobName(blobName) > generationBeforeMerge) {
                            if (mergeCommitLandsBelowBound) {
                                mergeUploadBlocked.countDown();
                            } else {
                                throw new AssertionError("merge commit lands above bound and should not be uploaded: " + blobName);
                            }
                        }
                        safeAwait(releaseCommitUploads);
                    }
                }
            });

            final ActionFuture<BroadcastResponse> forceMergeFuture;
            if (mergeCommitLandsBelowBound) {
                logger.info("--> force merging with flush on the old node, generationBeforeMerge=[{}]", generationBeforeMerge);
                // The force-merge flush waits for its upload, which is blocked, so do not wait for the response yet
                forceMergeFuture = client(oldIndexNode).admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).execute();
                safeAwait(mergeUploadBlocked);
                mergeGeneration.set(sourceCommitService.getMaxPendingOrUploadedGeneration(shardId));
            } else {
                logger.info("--> force merging without flush on the old node, generationBeforeMerge=[{}]", generationBeforeMerge);
                forceMergeFuture = client(oldIndexNode).admin()
                    .indices()
                    .prepareForceMerge(indexName)
                    .setMaxNumSegments(1)
                    .setFlush(false)
                    .execute();
                assertNoFailures(safeGet(forceMergeFuture));
                // Use a refresh flush to commit but not upload the merge.
                safeAwait((ActionListener<Engine.RefreshResult> l) -> sourceEngine.maybeRefresh("test", l));
                mergeGeneration.set(sourceEngine.getCurrentGeneration());

                final var currentVirtualBcc = sourceCommitService.getCurrentVirtualBcc(shardId);
                assertNotNull(currentVirtualBcc);
                assertThat(currentVirtualBcc.getMaxGeneration(), equalTo(mergeGeneration.get()));
                assertThat(
                    "the merge commit is neither pending upload nor uploaded",
                    sourceCommitService.getMaxPendingOrUploadedGeneration(shardId),
                    lessThan(mergeGeneration.get())
                );
            }

            assertThat(mergeGeneration.get(), greaterThan(generationBeforeMerge));
            assertThat(
                "merge is not notified before the upload bound is pinned",
                generationsNotifiedBySource,
                not(hasItem(mergeGeneration.get()))
            );

            logger.info("--> resuming markRelocating, merge generation=[{}]", mergeGeneration.get());
            resumeMarkRelocating.countDown();
            safeAwait(markRelocatingDone);

            final long maxGenerationToUpload = sourceCommitService.getMaxGenerationToUpload(shardId);
            if (mergeCommitLandsBelowBound) {
                assertThat("pinned bound includes the merge commit", maxGenerationToUpload, greaterThanOrEqualTo(mergeGeneration.get()));
                safeAwait(mergeNotified);
                assertThat(nonUploadGenerationsNotifiedBySource, hasItem(mergeGeneration.get()));
                releaseCommitUploads.countDown();
                assertNoFailures(safeGet(forceMergeFuture));
                ensureGreen(indexName);
            } else {
                assertThat("the pinned bound excludes the merge commit", maxGenerationToUpload, lessThan(mergeGeneration.get()));
                releaseCommitUploads.countDown();
                ensureGreen(indexName);
                assertThat(
                    "a commit above the pinned upload bound must never be advertised by the relocation source",
                    generationsNotifiedBySource,
                    not(hasItem(mergeGeneration.get()))
                );
            }

            assertResponse(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
                assertNoFailures(searchResponse);
                assertEquals(200, searchResponse.getHits().getTotalHits().value());
            });
        } finally {
            resumeMarkRelocating.countDown();
            releaseCommitUploads.countDown();
            setNodeRepositoryStrategy(oldIndexNode, StatelessMockRepositoryStrategy.DEFAULT);
        }
    }

    /// A search shard can recover from a commit that the relocation target uploaded after accepting the handoff, and register it
    /// with the relocation source, which has not yet received the handoff response and is still the primary in the routing table.
    /// The target keeps the primary term, so that commit can have the same term and generation as a BCC that is pending upload on
    /// the source but paused above `maxGenerationToUpload`. The source cannot hand that paused BCC, since it never uploads it.
    /// The registration should instead fail as too new, and is retried against the target once the relocation completes.
    public void testSearchShardRegistrationWithTargetCommitDoesNotGetPausedBccFromSource() throws Exception {
        final Settings indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), Boolean.FALSE)
            .build();

        final var oldIndexNode = startMasterAndIndexNode(indexNodeSettings);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);
        final int docCount = randomIntBetween(10, 100);
        indexDocs(indexName, docCount);
        flush(indexName);

        final var index = resolveIndex(indexName);
        final var sourceShard = findIndexShard(index, 0, oldIndexNode);
        final var shardId = sourceShard.shardId();
        final var newIndexNode = startIndexNode(indexNodeSettings);

        // Hold the handoff response, so that the old primary keeps its commit state open and stays the primary in the
        // routing table, while the new primary has already opened its engine.
        final var handoffProcessed = new CountDownLatch(1);
        final var releaseHandoffResponse = new SubscribableListener<Void>();
        final var newNodeTransportService = MockTransportService.getInstance(newIndexNode);
        newNodeTransportService.addRequestHandlingBehavior(
            PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
            (handler, request, channel, task) -> handler.messageReceived(
                request,
                new TestTransportChannel(new ChannelActionListener<>(channel).<TransportResponse>delegateFailure((l, response) -> {
                    handoffProcessed.countDown();
                    releaseHandoffResponse.addListener(l.map(ignored -> response));
                })),
                task
            )
        );

        // Capture the first registration that reaches the old primary, and its outcome.
        final var firstRegistrationRequestOnOldNode = new SubscribableListener<RegisterCommitRequest>();
        final var firstRegistrationOutcomeOnOldNode = new SubscribableListener<RegisterCommitResponse>();
        final var oldNodeTransportService = MockTransportService.getInstance(oldIndexNode);
        oldNodeTransportService.addRequestHandlingBehavior(
            TransportRegisterCommitForRecoveryAction.NAME,
            (handler, request, channel, task) -> {
                firstRegistrationRequestOnOldNode.onResponse((RegisterCommitRequest) request);
                handler.messageReceived(request, new TestTransportChannel(ActionListener.wrap(response -> {
                    firstRegistrationOutcomeOnOldNode.onResponse((RegisterCommitResponse) response);
                    channel.sendResponse(response);
                }, e -> {
                    firstRegistrationOutcomeOnOldNode.onFailure(e);
                    channel.sendResponse(e);
                })), task);
            }
        );

        final var sourceCommitService = internalCluster().getInstance(StatelessCommitService.class, oldIndexNode);

        logger.info("--> moving index shard from {} to {}", oldIndexNode, newIndexNode);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, oldIndexNode, newIndexNode));
        try {
            logger.info("--> waiting for the new node to process the relocation handoff");
            safeAwait(handoffProcessed);

            final long maxGenerationToUpload = sourceCommitService.getMaxGenerationToUpload(shardId);
            assertThat("upload bound expected to be pinned", maxGenerationToUpload, lessThan(Long.MAX_VALUE));

            logger.info("--> flushing on the old node to create a paused commit above maxGenerationToUpload");
            // The old node never uploads this commit, so do not wait for the flush to be durable.
            sourceShard.getEngineOrNull().flush(true, true, ActionListener.noop());
            final long pausedGeneration = maxGenerationToUpload + 1;
            assertThat(sourceShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration(), equalTo(pausedGeneration));
            assertThat(sourceCommitService.getCurrentVirtualBcc(shardId), nullValue());
            assertThat(sourceCommitService.getMaxPendingOrUploadedGeneration(shardId), equalTo(pausedGeneration));

            logger.info("--> flushing on the new node to upload a commit with the same term and generation");
            final var targetShard = findIndexShard(index, 0, newIndexNode);
            assertThat(targetShard.getOperationPrimaryTerm(), equalTo(sourceShard.getOperationPrimaryTerm()));
            safeAwait(SubscribableListener.<Engine.FlushResult>newForked(l -> targetShard.getEngineOrNull().flush(true, true, l)));
            assertThat(targetShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration(), equalTo(pausedGeneration));
            final var targetCommitService = internalCluster().getInstance(StatelessCommitService.class, newIndexNode);
            safeAwait((ActionListener<Void> l) -> targetCommitService.addListenerForUploadedGeneration(shardId, pausedGeneration, l));

            logger.info("--> starting a search shard, which registers the new node's commit with the old node");
            startSearchNode();
            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
            assertThat(
                safeAwait(firstRegistrationRequestOnOldNode).getCompoundCommitPrimaryTermAndGeneration(),
                equalTo(new PrimaryTermAndGeneration(sourceShard.getOperationPrimaryTerm(), pausedGeneration))
            );
            assertThat(
                "a paused BCC must not be treated as available to a recovering search shard",
                safeAwaitFailure(firstRegistrationOutcomeOnOldNode),
                instanceOf(RecoveryCommitTooNewException.class)
            );
        } finally {
            releaseHandoffResponse.onResponse(null);
            newNodeTransportService.clearAllRules();
            oldNodeTransportService.clearAllRules();
        }

        ensureGreen(indexName);
        assertResponse(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
            assertNoFailures(searchResponse);
            assertEquals(docCount, searchResponse.getHits().getTotalHits().value());
        });
    }
}
