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

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
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

import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CorruptionWhileRelocatingIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), "1h");
    }

    public void testMergeWhileRelocationCausesCorruption() throws Exception {
        final var indexNode = startMasterAndIndexNode();
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
                if (notification.getTerm() == primaryTerm && notification.getGeneration() == finalGeneration && notification.isUpload()) {
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
        var newIndexNode = startIndexNode();
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
        clusterAdmin().prepareReroute().add(new MoveAllocationCommand(indexName, 0, indexNode, newIndexNode)).execute().actionGet();

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

        var objectStoreService = internalCluster().getCurrentMasterNodeInstance(ObjectStoreService.class);
        var blobContainer = objectStoreService.getBlobContainer(sourceShard.shardId(), primaryTerm);

        // Check that the blob has not been uploaded
        assertFalse(blobContainer.blobExists(operationPurpose, finalCommitBlobName));

        Store sourceStore = sourceShard.store();

        logger.info("--> resuming relocation");
        resumeHandoff.countDown();

        logger.info("--> waiting for search node to receive the notification for the post-handoff commit");
        assertBusy(() -> assertThat(receivedNotifications.get(), equalTo(1)));

        assertTrue(blobContainer.blobExists(operationPurpose, finalCommitBlobName));

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
        clusterAdmin().prepareReroute().add(new MoveAllocationCommand(indexName, 0, indexNode, newIndexNode)).execute().actionGet();

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
            assertEquals(2000, searchResponse.getHits().getTotalHits().value);
        });
    }
}
