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
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.TestStateless;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.TestStatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryCommitTooNewException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class RecoveryCommitRegistrationIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        return List.copyOf(plugins);
    }

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
        var indexNodeA = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
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

    public void testSearchShardCloseDuringCommitRegistration() throws Exception {
        final String indexNodeA = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);
        final var indexName = randomIdentifier();
        // Create an index with 2 primary shards to get ShardNotFoundException instead of IndexNotFoundException in the test
        createIndex(
            indexName,
            indexSettings(2, 1).put(INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE).build()
        );
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(1, 50));
        flush(indexName);

        final IndicesService indexNodeAIndicesService = internalCluster().getInstance(IndicesService.class, indexNodeA);
        final IndexService indexNodeAIndexService = indexNodeAIndicesService.iterator().next();
        final IndexShard indexShard = indexNodeAIndexService.getShard(0);

        final String indexNodeB = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(4);

        final CyclicBarrier commitRegistrationBarrier = new CyclicBarrier(2);
        final AtomicBoolean blockedOnce = new AtomicBoolean(false);
        final MockTransportService indexNodeATransportService = MockTransportService.getInstance(indexNodeA);
        indexNodeATransportService.addRequestHandlingBehavior(
            TransportRegisterCommitForRecoveryAction.NAME,
            (handler, request, channel, task) -> {
                final RegisterCommitRequest registerCommitRequest = (RegisterCommitRequest) request;
                if (registerCommitRequest.getShardId().getId() == 0 && blockedOnce.compareAndSet(false, true)) {
                    safeAwait(commitRegistrationBarrier);
                    safeAwait(commitRegistrationBarrier);
                }
                handler.messageReceived(request, channel, task);
            }
        );

        // Start search shard recovery on searchNodeB and wait it stuck at commit registration
        // 2 replica shard so that the one good copy is able to trigger refresh requests
        logger.info("--> updating number_of_replicas to 2");
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2));
        safeAwait(commitRegistrationBarrier);

        // Fail the indexing shard which should in turn fails the recovery
        logger.info("--> failing indexing shard");
        indexShard.failShard("broken", new RuntimeException("boom"));
        logger.info("--> wait for indexing shard to disappear");
        assertBusy(() -> assertThat(indexNodeAIndexService.hasShard(0), is(false)));

        logger.info("--> continue commit registration");
        // Let commit registration continue and it should fail but keep waiting
        safeAwait(commitRegistrationBarrier);

        // indexing shard should recover on indexNodeB
        ensureYellow(indexName);

        logger.info("--> start indexing");
        // indexing
        for (int i = 0; i < 10; i++) {
            final BulkRequestBuilder bulkRequestBuilder = client(indexNodeB).prepareBulk();
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(client(indexNodeB).prepareIndex(indexName).setSource("field", randomAlphaOfLengthBetween(20, 50)));
            }
            bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            bulkRequestBuilder.get(TEST_REQUEST_TIMEOUT);
            safeSleep(randomLongBetween(1, 100));
        }

        logger.info("--> waiting for index to be green");
        ensureGreen(indexName);
    }

    public void testSearchShardWillNotRegisterWithOldPrimaryAfterItIsRelocated() {
        final Settings nodeSettings = disableIndexingDiskAndMemoryControllersNodeSettings();
        startMasterOnlyNode(nodeSettings);
        final String indexNode = startIndexNode(nodeSettings);
        startSearchNode(nodeSettings);
        ensureStableCluster(3);
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final int numIndexingBatches = between(1, 10);
        for (int i = 0; i < numIndexingBatches; i++) {
            indexDocs(indexName, between(1, 50));
            if (randomBoolean()) {
                refresh(indexName);
            }
        }

        // Set up a countdown to notify markRelocated is called on the source primary
        final IndexShard indexShard = findIndexShard(indexName);
        final IndexEngine indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        final var statelessCommitService = (TestStatelessCommitService) indexEngine.getStatelessCommitService();
        final var markRelocatedLatch = new CountDownLatch(1);
        statelessCommitService.setStrategy(new TestStatelessCommitService.Strategy() {
            @Override
            public ActionListener<Void> markRelocating(
                Supplier<ActionListener<Void>> originalSupplier,
                ShardId shardId,
                long minRelocatedGeneration,
                ActionListener<Void> listener
            ) {
                return originalSupplier.get().delegateFailure((l, ignore) -> {
                    l.onResponse(null); // mark relocated
                    markRelocatedLatch.countDown();
                });
            }
        });

        final String newIndexNode = startIndexNode(nodeSettings);
        ensureStableCluster(4);
        final var indexNodeTransportService = MockTransportService.getInstance(indexNode);
        final var newIndexNodeTransportService = MockTransportService.getInstance(newIndexNode);

        final var startPrimaryRelocationLatch = new CountDownLatch(1);
        final var continuePrimaryRelocationLatch = new CountDownLatch(1);
        indexNodeTransportService.addRequestHandlingBehavior(START_RELOCATION_ACTION_NAME, (handler, request, channel, task) -> {
            startPrimaryRelocationLatch.countDown();
            safeAwait(continuePrimaryRelocationLatch);
            handler.messageReceived(request, channel, task);
        });
        logger.info("--> start indexing shard relocating and wait for it to block at the start action");
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNode));
        safeAwait(startPrimaryRelocationLatch);

        final var startRegistrationLatch = new CountDownLatch(1);
        final var continueRegistrationLatch = new CountDownLatch(1);
        indexNodeTransportService.addRequestHandlingBehavior(
            TransportRegisterCommitForRecoveryAction.NAME,
            (handler, request, channel, task) -> {
                startRegistrationLatch.countDown();
                safeAwait(continueRegistrationLatch);
                handler.messageReceived(
                    request,
                    new TestTransportChannel(
                        new ChannelActionListener<>(channel).delegateFailure(
                            (l, r) -> fail("commit registration with old primary should have failed")
                        )
                    ),
                    task
                );
            }
        );
        logger.info("--> start search shard recovery and wait for it to block at sending commit registration");
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        safeAwait(startRegistrationLatch);

        // Registration should retry with the new primary eventually
        final CountDownLatch registeredWithNewPrimaryLatch = new CountDownLatch(1);
        newIndexNodeTransportService.addRequestHandlingBehavior(
            TransportRegisterCommitForRecoveryAction.NAME,
            (handler, request, channel, task) -> {
                handler.messageReceived(request, new TestTransportChannel(new ChannelActionListener<>(channel).delegateFailure((l, r) -> {
                    l.onResponse(r);
                    registeredWithNewPrimaryLatch.countDown();
                })), task);
            }
        );

        logger.info("--> continue primary relocation and wait for old primary to be marked as relocated");
        continuePrimaryRelocationLatch.countDown();
        safeAwait(markRelocatedLatch);

        logger.info("--> let search shard commit registration continue and it should succeed by retry with new primary");
        continueRegistrationLatch.countDown();
        safeAwait(registeredWithNewPrimaryLatch);

        ensureGreen(indexName);
    }

    public void testRegisterCommitForRecoveryThrowsExceptionAndShardGetRelocatedEventually() {
        var indexNode = startMasterAndIndexNode();
        startSearchNode();
        startSearchNode();

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        int numDocs = randomIntBetween(1, 100);
        indexDocs(indexName, numDocs);
        flushAndRefresh(indexName);

        var failOnce = new AtomicBoolean(false);
        var expectedNumberOfCalls = new CountDownLatch(2);

        var indexNodeTransportService = MockTransportService.getInstance(indexNode);
        indexNodeTransportService.addRequestHandlingBehavior(
            TransportRegisterCommitForRecoveryAction.NAME,
            (handler, request, channel, task) -> {
                expectedNumberOfCalls.countDown();
                if (failOnce.compareAndSet(false, true)) {
                    logger.info("--> failing first search shard bootstrap so that it got retried");
                    channel.sendResponse(new ElasticsearchException("simulated failure"));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            }
        );

        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 2)    // allow at most 1 allocation failure
        );
        ensureGreen(indexName);

        assertThat(failOnce.get(), is(true));
        safeAwait(expectedNumberOfCalls);
    }

    public void testRelocatingSearchShardWithSlownessInRegisterCommitForRecovery() {
        var indexNode = startMasterAndIndexNode();
        var searchNodeA = startSearchNode();
        var searchNodeB = startSearchNode();

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", searchNodeB).build());
        ensureGreen(indexName);

        int numDocs = randomIntBetween(1, 100);
        indexDocs(indexName, numDocs);
        flushAndRefresh(indexName);

        // the first request for initial bootstrapped shard, the second - for relocating
        var registerCommitForRecoveryRequestCount = new CountDownLatch(2);

        var searchNodeIds = Set.of(getNodeId(searchNodeA), getNodeId(searchNodeB));
        var receivedRequestsFromNodeIds = Collections.synchronizedSet(new HashSet<>());

        var indexNodeTransportService = MockTransportService.getInstance(indexNode);
        indexNodeTransportService.addRequestHandlingBehavior(
            TransportRegisterCommitForRecoveryAction.NAME,
            (handler, request, channel, task) -> {
                registerCommitForRecoveryRequestCount.countDown();
                receivedRequestsFromNodeIds.add(((RegisterCommitRequest) request).getNodeId());
                handler.messageReceived(request, channel, task);
            }
        );

        logger.info("--> start delaying network on master/index node");
        final NetworkDisruption networkDisruption = isolateMasterDisruption(
            NetworkDisruption.NetworkDelay.random(random(), timeValueMillis(100), timeValueMillis(1000))
        );
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        // start recovery
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), hasItem(searchNodeA));
        assertThat(internalCluster().nodesInclude(indexName), not(hasItem(searchNodeB)));

        // start relocation
        updateIndexSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", searchNodeA));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), not(hasItem(searchNodeA)));
        assertThat(internalCluster().nodesInclude(indexName), hasItem(searchNodeB));

        safeAwait(registerCommitForRecoveryRequestCount);
        assertThat(receivedRequestsFromNodeIds, equalTo(searchNodeIds));

        logger.info("--> stop delaying network");
        networkDisruption.stopDisrupting();
        internalCluster().clearDisruptionScheme(true);

        var searchShard = findSearchShard(indexName);
        var directory = SearchDirectory.unwrapDirectory(searchShard.store().directory());
        var searchShardTermAndGen = directory.getCurrentCommit().primaryTermAndGeneration();
        var indexingShardTermAndGen = getIndexingShardTermAndGeneration(indexName, 0);

        // check that search shard bootstrapped from most recent commit
        assertThat(searchShardTermAndGen, equalTo(indexingShardTermAndGen));
    }

    public void testRetryRecoveryCommitRegistrationIfPrimaryMoves() throws Exception {
        startMasterOnlyNode();

        var indexNode1 = startIndexNode();

        var indexName = randomIdentifier();
        // do not retry recovery on allocation level
        createIndex(indexName, indexSettings(1, 0).put(SETTING_ALLOCATION_MAX_RETRY.getKey(), 0).build());
        ensureGreen(indexName);
        int totalDocs = randomIntBetween(1, 10);
        indexDocs(indexName, totalDocs);
        flushAndRefresh(indexName);

        var searchNode = startSearchNode();

        var waitForChangesOnIndexingSide = new CountDownLatch(1);
        var mockTransportServiceSearchNode = (MockTransportService) internalCluster().getInstance(TransportService.class, searchNode);
        mockTransportServiceSearchNode.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportRegisterCommitForRecoveryAction.NAME)) {
                safeAwait(waitForChangesOnIndexingSide);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        var indexNode2 = startIndexNode();

        var mockTransportServiceIndexNode1 = (MockTransportService) internalCluster().getInstance(TransportService.class, indexNode1);

        // Note that search node is expected to throw "org.elasticsearch.index.engine.EngineException: Engine not started"
        // since search shard is in the middle of recovery and engine has not started yet

        // request is expected to land on previous indexing node and IndexNotFoundException should be thrown
        mockTransportServiceIndexNode1.addRequestHandlingBehavior(
            TransportRegisterCommitForRecoveryAction.NAME,
            (handler, request, channel, task) -> {
                handler.messageReceived(request, new TestTransportChannel(new ChannelActionListener<>(channel).delegateResponse((l, e) -> {
                    assertThat(e, instanceOf(IndexNotFoundException.class));
                    l.onFailure(e);
                }).delegateFailure((l, r) -> fail("previous indexing is expected to throw IndexNotFoundException"))), task);
            }
        );

        setReplicaCount(1, indexName);

        logger.info("--> primary relocates from {} to {}", indexNode1, indexNode2);
        updateIndexSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", indexNode1));
        ensureYellow(indexName);
        assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNode1)));

        logger.info("--> resume recovery commit registration");
        waitForChangesOnIndexingSide.countDown();

        // note that recovery registration should be successfully retried against new indexing node

        ensureGreen(indexName);
    }

    public void testFailRecoveryCommitRegistrationIfIndexGetsDeleted() throws Exception {

        var indexNode = startMasterAndIndexNode();

        var indexName = randomIdentifier();
        // do not retry recovery on allocation level
        createIndex(indexName, indexSettings(1, 0).put(SETTING_ALLOCATION_MAX_RETRY.getKey(), 0).build());
        ensureGreen(indexName);
        int totalDocs = randomIntBetween(1, 10);
        indexDocs(indexName, totalDocs);
        flushAndRefresh(indexName);

        var searchNode = startSearchNode();

        var waitForChangesOnIndexingSide = new CountDownLatch(1);

        var mockTransportServiceSearchNode = (MockTransportService) internalCluster().getInstance(TransportService.class, searchNode);
        mockTransportServiceSearchNode.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportRegisterCommitForRecoveryAction.NAME)) {
                safeAwait(waitForChangesOnIndexingSide);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        var mockTransportServiceIndexNode = (MockTransportService) internalCluster().getInstance(TransportService.class, indexNode);

        mockTransportServiceIndexNode.addRequestHandlingBehavior(
            TransportRegisterCommitForRecoveryAction.NAME,
            (handler, request, channel, task) -> {
                handler.messageReceived(request, new TestTransportChannel(new ChannelActionListener<>(channel).delegateResponse((l, e) -> {
                    assertThat(e, instanceOf(IndexNotFoundException.class));
                    l.onFailure(e);
                }).delegateFailure((l, r) -> fail("Indexing node is expected to throw IndexNotFoundException"))), task);
            }
        );

        setReplicaCount(1, indexName);

        logger.info("--> removing index {}", indexName);
        admin().indices().delete(new DeleteIndexRequest(indexName));
        awaitClusterState(logger, indexNode, state -> state.getRoutingTable().index(indexName) == null);

        logger.info("--> resume recovery commit registration");
        waitForChangesOnIndexingSide.countDown();

        // recovery failed at this point and cluster should not have any indices/shards
        ensureGreen();
    }
}
