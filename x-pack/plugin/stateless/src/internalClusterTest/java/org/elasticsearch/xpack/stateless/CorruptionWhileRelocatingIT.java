/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.stateless.action.NewCommitNotificationRequest;
import org.elasticsearch.xpack.stateless.action.TransportNewCommitNotificationAction;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.recovery.RegisterCommitResponse;
import org.elasticsearch.xpack.stateless.recovery.TransportRegisterCommitForRecoveryAction;

import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationHandoffAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class CorruptionWhileRelocatingIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
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

        final var finalCommitBlobName = StatelessCompoundCommit.blobNameFromGeneration(finalGeneration);

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
    /// generation is above `maxGenerationToUpload`. That commit will never be uploaded by the relocation source
    ///
    /// The sequence forced here:
    /// - The old primary enters `RELOCATING`, pinning `maxGenerationToUpload = M`.
    /// - A force merge on the old node creates generation `M+1`, whose upload is paused for good.
    /// - A recovering search shard registers with the old primary, which is still the primary in the routing table.
    /// - The handoff completes and generation `M+1` is discarded, never reaching the object store.
    ///
    public void testSearchShardRegistrationDuringRelocationStaysWithinMaxGenerationToUpload() throws Exception {
        final Settings indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), Boolean.FALSE)
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
        final var firstRegistration = new SubscribableListener<RegisterCommitResponse>();
        final var firstRegistrationCaptured = new AtomicBoolean();
        MockTransportService.getInstance(oldIndexNode)
            .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                pauseRegistration.countDown();
                safeAwait(resumeRegistration);
                handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public void sendResponse(TransportResponse response) {
                        if (response instanceof RegisterCommitResponse rcr && rcr.getCompoundCommit() != null) {
                            logger.info("--> old primary handed back generation [{}]", rcr.getCompoundCommit().generation());
                            // Record the first registration
                            if (firstRegistrationCaptured.compareAndSet(false, true)) {
                                firstRegistration.onResponse(rcr);
                            }
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
        // search shard registration runs.
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

        logger.info("--> moving index shard from {} to {}", oldIndexNode, newIndexNode);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, oldIndexNode, newIndexNode));
        logger.info("--> waiting for the relocation handoff to be paused");
        safeAwait(pauseHandoff);

        // markRelocating has run, so maxGenerationToUpload is the generation of the last flush on the source.
        final long maxGenerationToUpload = sourceShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();

        startSearchNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        logger.info("--> waiting for the search shard registration to reach the old indexing node");
        safeAwait(pauseRegistration);

        logger.info("--> force merging on the old node to create a commit above maxGenerationToUpload");
        client(oldIndexNode).admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).execute();

        final var sourceCommitService = internalCluster().getInstance(StatelessCommitService.class, oldIndexNode);
        assertBusy(
            () -> assertThat(
                sourceCommitService.getMaxPendingOrUploadedGeneration(sourceShard.shardId()),
                greaterThan(maxGenerationToUpload)
            )
        );

        logger.info(
            "--> before resuming registration: maxGenerationToUpload=[{}], maxPendingOrUploaded=[{}], latestUploadedBcc=[{}], engine=[{}]",
            maxGenerationToUpload,
            sourceCommitService.getMaxPendingOrUploadedGeneration(sourceShard.shardId()),
            sourceCommitService.getLatestUploadedBcc(sourceShard.shardId()).primaryTermAndGeneration(),
            sourceShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration()
        );
        logger.info("--> resuming the search shard registration");
        resumeRegistration.countDown();

        final var registrationResponse = safeAwait(firstRegistration);
        logger.info(
            "--> registration returned [{}], maxGenerationToUpload=[{}], source engine is at [{}]",
            registrationResponse.getCompoundCommit().primaryTermAndGeneration(),
            maxGenerationToUpload,
            sourceShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration()
        );

        logger.info("--> resuming the relocation handoff");
        resumeHandoff.countDown();

        ensureGreen(indexName);
        assertResponse(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
            assertNoFailures(searchResponse);
            assertEquals(2000, searchResponse.getHits().getTotalHits().value());
        });
    }
}
