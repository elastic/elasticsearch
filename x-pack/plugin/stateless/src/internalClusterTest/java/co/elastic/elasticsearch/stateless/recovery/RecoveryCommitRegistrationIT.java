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

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.recovery.RecoveryCommitTooNewException;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class RecoveryCommitRegistrationIT extends AbstractStatelessIntegTestCase {

    public void testSearchShardRecoveryRegistersCommit() {
        startMasterOnlyNode();
        startIndexNode();
        var searchNode = startSearchNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.HOURS)).build()
        );
        ensureGreen(indexName);
        // Create some commits
        int commits = randomIntBetween(0, 3);
        for (int i = 0; i < commits; i++) {
            indexDocs(indexName, randomIntBetween(10, 50));
            flush(indexName);
        }
        AtomicInteger registerCommitRequestsSent = new AtomicInteger();
        MockTransportService.getInstance(searchNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportRegisterCommitForRecoveryAction.NAME)) {
                registerCommitRequestsSent.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });
        // Start a search shard
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);
        assertThat(registerCommitRequestsSent.get(), equalTo(1));
    }

    public void testSearchShardRecoveryRegistrationRetry() {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        startSearchNode();
        final var indexName = randomIdentifier();
        var maxRetries = randomFrom(0, 5);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.HOURS))
                .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), maxRetries)
                .build()
        );
        ensureGreen(indexName);
        // Create some commits
        int commits = randomIntBetween(0, 3);
        for (int i = 0; i < commits; i++) {
            indexDocs(indexName, randomIntBetween(10, 50));
            flush(indexName);
        }
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        // Make sure we hit the transport action's retries by failing more than the number of allocation attempts
        final var toFailCount = maxRetries + 1;
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger receivedRegistration = new AtomicInteger();
        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                receivedRegistration.incrementAndGet();
                if (failed.get() < toFailCount) {
                    failed.incrementAndGet();
                    channel.sendResponse(
                        randomFrom(
                            new ShardNotFoundException(shardId, "cannot register"),
                            new RecoveryCommitTooNewException(shardId, "cannot register")
                        )
                    );
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        // Trigger enough cluster state updates to see the reties succeed.
        for (int i = 0; i < toFailCount + 1; i++) {
            indicesAdmin().preparePutMapping(indexName).setSource("field" + i, "type=keyword").get();
        }
        ensureGreen(indexName);
        assertThat(failed.get(), equalTo(toFailCount));
        assertThat(receivedRegistration.get(), greaterThan(toFailCount));
    }

    // If during a relocation, a commit registration is triggered right after the last pre-handoff flush,
    // the registration request would reach the old indexing shard with the newer commit written as a result
    // of the relocation by the new indexing shard. However, the old indexing shard is not aware of this new
    // commit. Here, we make sure in that case, the search shard's registration fails on the old indexing shard
    // and the search shard resends the request to the new indexing shard.
    public void testUnpromotableRecoveryCommitRegistrationDuringRelocation() {
        var nodeSettings = Settings.builder().put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s").build();

        startMasterOnlyNode(nodeSettings);
        var indexNodeA = startIndexNode(nodeSettings);
        startSearchNode(nodeSettings);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.HOURS))
                // To ensure we hit registration retries not allocation retries
                .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 0)
                .build()
        );
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(10, 50));
        refresh(indexName);
        var indexNodeB = startIndexNode(nodeSettings);

        var nodeAReceivedRegistration = new CountDownLatch(1);
        var indexNodeATransport = MockTransportService.getInstance(indexNodeA);
        indexNodeATransport.addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
            logger.info("--> NodeA received commit registration with request {}", request);
            nodeAReceivedRegistration.countDown();
            handler.messageReceived(request, channel, task);
        });
        var nodeBReceivedRegistration = new CountDownLatch(1);
        var indexNodeBTransport = MockTransportService.getInstance(indexNodeB);
        indexNodeBTransport.addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
            logger.info("--> NodeB received commit registration with request {}", request);
            nodeBReceivedRegistration.countDown();
            handler.messageReceived(request, channel, task);
        });
        // Ensure the search shard starts recovering after the new indexing shard (on node B) has done a post-handoff commit
        // but before the new indexing shard being started so that the registration goes to the old indexing shard.
        var continueNodeBSendingShardStarted = new CountDownLatch(1);
        var nodeBSendingShardStarted = new CountDownLatch(1);
        indexNodeBTransport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(ShardStateAction.SHARD_STARTED_ACTION_NAME)) {
                logger.info("--> blocking NodeB sending shard started request {} to master", request);
                nodeBSendingShardStarted.countDown();
                safeAwait(continueNodeBSendingShardStarted);
                logger.info("--> NodeB sent shard started request {} to master", request);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        // initiate a relocation
        logger.info("--> move primary shard from: {} to: {}", indexNodeA, indexNodeB);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, indexNodeA, indexNodeB));
        logger.info("--> waiting for nodeB sending SHARD_STARTED ");
        safeAwait(nodeBSendingShardStarted);
        // start search shard, You should see the last commit but the registration should go to the old indexing shard
        indicesAdmin().prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .execute();
        logger.info("--> waiting for NodeA to receive commit registration");
        safeAwait(nodeAReceivedRegistration);
        continueNodeBSendingShardStarted.countDown();
        // the registration should be resent to the new indexing shard
        try {
            nodeBReceivedRegistration.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        ensureGreen(indexName);
    }

    /**
     * Test a race condition where an upload happens, the search shard sees the commit and informs indexing shard prior to the indexing
     * shard realizing that the commit has happened.
     */
    public void testUnpromotableCommitRegistrationDuringUpload() throws InterruptedException {
        startMasterOnlyNode();
        var indexNodeA = startIndexNode();
        startSearchNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(10, 50));

        flush(indexName);

        AtomicReference<AssertionError> rethrow = new AtomicReference<>();
        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final CyclicBarrier uploadBarrier = new CyclicBarrier(2);
        internalCluster().getInstance(StatelessCommitService.class, indexNodeA).addConsumerForNewUploadedBcc(shardId, info -> {
            try {
                safeAwait(uploadBarrier);
                safeAwait(uploadBarrier);
            } catch (AssertionError e) {
                logger.error(e.getMessage(), e);
                rethrow.set(e);
            }
        });
        Thread thread = new Thread(() -> {
            indexDocs(indexName, randomIntBetween(1, 50));
            flush(indexName);
        });

        thread.start();
        try {
            safeAwait(uploadBarrier);
            assertAcked(
                admin().indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            );

            ensureGreen(indexName);

            safeAwait(uploadBarrier);
        } finally {
            thread.join(10000);
        }
        assertThat(thread.isAlive(), is(false));
        assertNull(rethrow.get());
    }
}
