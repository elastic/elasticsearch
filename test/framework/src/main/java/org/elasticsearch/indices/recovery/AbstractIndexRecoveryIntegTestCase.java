/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.RecoverySettingsChunkSizePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractIndexRecoveryIntegTestCase extends ESIntegTestCase {
    private static final String REPO_NAME = "test-repo-1";
    private static final String SNAP_NAME = "test-snap-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            MockTransportService.TestPlugin.class,
            MockFSIndexStore.TestPlugin.class,
            RecoverySettingsChunkSizePlugin.class,
            InternalSettingsPlugin.class,
            MockEngineFactoryPlugin.class
        );
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        super.beforeIndexDeletion();
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
        internalCluster().assertSeqNos();
        internalCluster().assertSameDocIdsOnShards();
    }

    protected void checkTransientErrorsDuringRecoveryAreRetried(String recoveryActionToBlock) throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "500ms")
            .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), "10s")
            .build();
        // start a master node
        internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster().startNode(
            Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build()
        );
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = clusterAdmin().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        indicesAdmin().prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .get();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = scaledRandomIntBetween(100, 8000);
        // Index 3/4 of the documents and flush. And then index the rest. This attempts to ensure that there
        // is a mix of file chunks and translog ops
        int threeFourths = (int) (numDocs * 0.75);
        for (int i = 0; i < threeFourths; i++) {
            requests.add(client().prepareIndex(indexName).setSource("{}", XContentType.JSON));
        }
        indexRandom(true, requests);
        flush(indexName);
        requests.clear();

        for (int i = threeFourths; i < numDocs; i++) {
            requests.add(client().prepareIndex(indexName).setSource("{}", XContentType.JSON));
        }
        indexRandom(true, requests);
        ensureSearchable(indexName);

        ClusterStateResponse stateResponse = clusterAdmin().prepareState().get();
        final String blueNodeId = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertFalse(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty());

        SearchResponse searchResponse = client().prepareSearch(indexName).get();
        assertHitCount(searchResponse, numDocs);

        logger.info("--> will temporarily interrupt recovery action between blue & red on [{}]", recoveryActionToBlock);

        if (recoveryActionToBlock.equals(PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT)) {
            createSnapshotThatCanBeUsedDuringRecovery(indexName);
        }

        MockTransportService blueTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            blueNodeName
        );
        MockTransportService redTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            redNodeName
        );

        final AtomicBoolean recoveryStarted = new AtomicBoolean(false);
        final AtomicBoolean finalizeReceived = new AtomicBoolean(false);

        final SingleStartEnforcer validator = new SingleStartEnforcer(indexName, recoveryStarted, finalizeReceived);
        redTransportService.addSendBehavior(blueTransportService, (connection, requestId, action, request, options) -> {
            validator.accept(action, request);
            connection.sendRequest(requestId, action, request, options);
        });
        Runnable connectionBreaker = () -> {
            // Always break connection from source to remote to ensure that actions are retried
            logger.info("--> closing connections from source node to target node");
            blueTransportService.disconnectFromNode(redTransportService.getLocalDiscoNode());
            if (randomBoolean()) {
                // Sometimes break connection from remote to source to ensure that recovery is re-established
                logger.info("--> closing connections from target node to source node");
                redTransportService.disconnectFromNode(blueTransportService.getLocalDiscoNode());
            }
        };
        TransientReceiveRejected handlingBehavior = new TransientReceiveRejected(recoveryActionToBlock, recoveryStarted, connectionBreaker);
        redTransportService.addRequestHandlingBehavior(PeerRecoveryTargetService.Actions.FINALIZE, (handler, request, channel, task) -> {
            finalizeReceived.set(true);
            handler.messageReceived(request, channel, task);
        });
        redTransportService.addRequestHandlingBehavior(recoveryActionToBlock, handlingBehavior);

        try {
            logger.info("--> starting recovery from blue to red");
            updateIndexSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "red,blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1),
                indexName
            );

            ensureGreen();
            if (recoveryActionToBlock.equals(PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT)) {
                assertThat(handlingBehavior.blocksRemaining.get(), is(equalTo(0)));
            }
            searchResponse = client(redNodeName).prepareSearch(indexName).setPreference("_local").get();
            assertHitCount(searchResponse, numDocs);
        } finally {
            blueTransportService.clearAllRules();
            redTransportService.clearAllRules();
        }
    }

    public void checkDisconnectsWhileRecovering(String recoveryActionToBlock) throws Exception {
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), "100ms")
            .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), "1s")
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "1s")
            .build();
        // start a master node
        internalCluster().startNode(nodeSettings);

        final String blueNodeName = internalCluster().startNode(
            Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build()
        );
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        ClusterHealthResponse response = clusterAdmin().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));

        indicesAdmin().prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .get();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numDocs; i++) {
            requests.add(client().prepareIndex(indexName).setSource("{}", XContentType.JSON));
        }
        indexRandom(true, requests);
        ensureSearchable(indexName);

        ClusterStateResponse stateResponse = clusterAdmin().prepareState().get();
        final String blueNodeId = internalCluster().getInstance(ClusterService.class, blueNodeName).localNode().getId();

        assertFalse(stateResponse.getState().getRoutingNodes().node(blueNodeId).isEmpty());

        SearchResponse searchResponse = client().prepareSearch(indexName).get();
        assertHitCount(searchResponse, numDocs);

        final boolean dropRequests = randomBoolean();
        logger.info("--> will {} between blue & red on [{}]", dropRequests ? "drop requests" : "break connection", recoveryActionToBlock);

        // Generate a snapshot to recover from it if the action that we're blocking is sending the request snapshot files
        if (recoveryActionToBlock.equals(PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT)) {
            createSnapshotThatCanBeUsedDuringRecovery(indexName);
        }

        MockTransportService blueMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            blueNodeName
        );
        MockTransportService redMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            redNodeName
        );
        TransportService redTransportService = internalCluster().getInstance(TransportService.class, redNodeName);
        TransportService blueTransportService = internalCluster().getInstance(TransportService.class, blueNodeName);
        final CountDownLatch requestFailed = new CountDownLatch(1);

        if (randomBoolean()) {
            final StubbableTransport.SendRequestBehavior sendRequestBehavior = (connection, requestId, action, request, options) -> {
                if (recoveryActionToBlock.equals(action) || requestFailed.getCount() == 0) {
                    requestFailed.countDown();
                    logger.info("--> preventing {} request by throwing ConnectTransportException", action);
                    throw new ConnectTransportException(connection.getNode(), "DISCONNECT: prevented " + action + " request");
                }
                connection.sendRequest(requestId, action, request, options);
            };
            // Fail on the sending side
            blueMockTransportService.addSendBehavior(redTransportService, sendRequestBehavior);
            redMockTransportService.addSendBehavior(blueTransportService, sendRequestBehavior);
        } else {
            // Fail on the receiving side.
            blueMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel, task) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                redMockTransportService.disconnectFromNode(blueMockTransportService.getLocalDiscoNode());
                handler.messageReceived(request, channel, task);
            });
            redMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel, task) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                blueMockTransportService.disconnectFromNode(redMockTransportService.getLocalDiscoNode());
                handler.messageReceived(request, channel, task);
            });
        }

        logger.info("--> starting recovery from blue to red");
        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "red,blue")
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1),
            indexName
        );
        requestFailed.await();

        logger.info("--> clearing rules to allow recovery to proceed");
        blueMockTransportService.clearAllRules();
        redMockTransportService.clearAllRules();

        ensureGreen();
        searchResponse = client(redNodeName).prepareSearch(indexName).setPreference("_local").get();
        assertHitCount(searchResponse, numDocs);
    }

    public void checkDisconnectsDuringRecovery(boolean useSnapshotBasedRecoveries) throws Exception {
        boolean primaryRelocation = randomBoolean();
        final String indexName = "test";
        final Settings nodeSettings = Settings.builder()
            .put(
                RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(),
                TimeValue.timeValueMillis(randomIntBetween(0, 100))
            )
            .build();
        TimeValue disconnectAfterDelay = TimeValue.timeValueMillis(randomIntBetween(0, 100));
        // start a master node
        String masterNodeName = internalCluster().startMasterOnlyNode(nodeSettings);

        final String blueNodeName = internalCluster().startNode(
            Settings.builder().put("node.attr.color", "blue").put(nodeSettings).build()
        );
        final String redNodeName = internalCluster().startNode(Settings.builder().put("node.attr.color", "red").put(nodeSettings).build());

        indicesAdmin().prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .get();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numDocs; i++) {
            requests.add(client().prepareIndex(indexName).setSource("{}", XContentType.JSON));
        }
        indexRandom(true, requests);
        ensureSearchable(indexName);
        assertHitCount(client().prepareSearch(indexName).get(), numDocs);

        if (useSnapshotBasedRecoveries) {
            createSnapshotThatCanBeUsedDuringRecovery(indexName);
        }

        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            masterNodeName
        );
        MockTransportService blueMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            blueNodeName
        );
        MockTransportService redMockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            redNodeName
        );

        redMockTransportService.addSendBehavior(blueMockTransportService, new StubbableTransport.SendRequestBehavior() {
            private final AtomicInteger count = new AtomicInteger();

            @Override
            public void sendRequest(
                Transport.Connection connection,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions options
            ) throws IOException {
                logger.info("--> sending request {} on {}", action, connection.getNode());
                if (PeerRecoverySourceService.Actions.START_RECOVERY.equals(action) && count.incrementAndGet() == 1) {
                    // ensures that it's considered as valid recovery attempt by source
                    try {
                        assertBusy(
                            () -> assertThat(
                                "Expected there to be some initializing shards",
                                client(blueNodeName).admin()
                                    .cluster()
                                    .prepareState()
                                    .setLocal(true)
                                    .get()
                                    .getState()
                                    .getRoutingTable()
                                    .index("test")
                                    .shard(0)
                                    .getAllInitializingShards(),
                                not(empty())
                            )
                        );
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    connection.sendRequest(requestId, action, request, options);
                    try {
                        Thread.sleep(disconnectAfterDelay.millis());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new ConnectTransportException(
                        connection.getNode(),
                        "DISCONNECT: simulation disconnect after successfully sending " + action + " request"
                    );
                } else {
                    connection.sendRequest(requestId, action, request, options);
                }
            }
        });

        final AtomicBoolean finalized = new AtomicBoolean();
        blueMockTransportService.addSendBehavior(redMockTransportService, (connection, requestId, action, request, options) -> {
            logger.info("--> sending request {} on {}", action, connection.getNode());
            if (action.equals(PeerRecoveryTargetService.Actions.FINALIZE)) {
                finalized.set(true);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        for (MockTransportService mockTransportService : Arrays.asList(redMockTransportService, blueMockTransportService)) {
            mockTransportService.addSendBehavior(masterTransportService, (connection, requestId, action, request, options) -> {
                logger.info("--> sending request {} on {}", action, connection.getNode());
                if ((primaryRelocation && finalized.get()) == false) {
                    assertNotEquals(action, ShardStateAction.SHARD_FAILED_ACTION_NAME);
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        if (primaryRelocation) {
            logger.info("--> starting primary relocation recovery from blue to red");
            updateIndexSettings(
                Settings.builder().put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "red"),
                indexName
            );

            ensureGreen(); // also waits for relocation / recovery to complete
            // if a primary relocation fails after the source shard has been marked as relocated, both source and target are failed. If the
            // source shard is moved back to started because the target fails first, it's possible that there is a cluster state where the
            // shard is marked as started again (and ensureGreen returns), but while applying the cluster state the primary is failed and
            // will be reallocated. The cluster will thus become green, then red, then green again. Triggering a refresh here before
            // searching helps, as in contrast to search actions, refresh waits for the closed shard to be reallocated.
            client().admin().indices().prepareRefresh(indexName).get();
        } else {
            logger.info("--> starting replica recovery from blue to red");
            updateIndexSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "color", "red,blue")
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1),
                indexName
            );

            ensureGreen();
        }

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch(indexName).get(), numDocs);
        }
    }

    // We only use this method in IndexRecoveryWithSnapshotsIT that's located in the x-pack plugin
    // that implements snapshot based recoveries.
    private static void createSnapshotThatCanBeUsedDuringRecovery(String indexName) throws Exception {
        // Ensure that the safe commit == latest commit
        assertBusy(() -> {
            ShardStats stats = indicesAdmin().prepareStats(indexName)
                .clear()
                .get()
                .asMap()
                .entrySet()
                .stream()
                .filter(e -> e.getKey().shardId().getId() == 0)
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
            assertThat(stats, is(notNullValue()));
            assertThat(stats.getSeqNoStats(), is(notNullValue()));

            assertThat(
                Strings.toString(stats.getSeqNoStats()),
                stats.getSeqNoStats().getMaxSeqNo(),
                equalTo(stats.getSeqNoStats().getGlobalCheckpoint())
            );
        }, 60, TimeUnit.SECONDS);

        // Force merge to make sure that the resulting snapshot would contain the same index files as the safe commit
        ForceMergeResponse forceMergeResponse = client().admin().indices().prepareForceMerge(indexName).setFlush(randomBoolean()).get();
        assertThat(forceMergeResponse.getTotalShards(), equalTo(forceMergeResponse.getSuccessfulShards()));

        // create repo
        assertAcked(
            clusterAdmin().preparePutRepository(REPO_NAME)
                .setType("fs")
                .setSettings(
                    Settings.builder()
                        .put("location", randomRepoPath())
                        .put(BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.getKey(), true)
                        .put("compress", false)
                )
                .get()
        );

        // create snapshot
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(REPO_NAME, SNAP_NAME)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertThat(
            clusterAdmin().prepareGetSnapshots(REPO_NAME).setSnapshots(SNAP_NAME).get().getSnapshots().get(0).state(),
            equalTo(SnapshotState.SUCCESS)
        );
    }

    private class SingleStartEnforcer implements BiConsumer<String, TransportRequest> {

        private final AtomicBoolean recoveryStarted;
        private final AtomicBoolean finalizeReceived;
        private final String indexName;

        private SingleStartEnforcer(String indexName, AtomicBoolean recoveryStarted, AtomicBoolean finalizeReceived) {
            this.indexName = indexName;
            this.recoveryStarted = recoveryStarted;
            this.finalizeReceived = finalizeReceived;
        }

        @Override
        public void accept(String action, TransportRequest request) {
            // The cluster state applier will immediately attempt to retry the recovery on a cluster state
            // update. We want to assert that the first and only recovery attempt succeeds
            if (PeerRecoverySourceService.Actions.START_RECOVERY.equals(action)) {
                StartRecoveryRequest startRecoveryRequest = (StartRecoveryRequest) request;
                ShardId shardId = startRecoveryRequest.shardId();
                logger.info("--> attempting to send start_recovery request for shard: " + shardId);
                if (indexName.equals(shardId.getIndexName()) && recoveryStarted.get() && finalizeReceived.get() == false) {
                    throw new IllegalStateException("Recovery cannot be started twice");
                }
            }
        }
    }

    private class TransientReceiveRejected implements StubbableTransport.RequestHandlingBehavior<TransportRequest> {

        private final String actionName;
        private final AtomicBoolean recoveryStarted;
        private final Runnable connectionBreaker;
        private final AtomicInteger blocksRemaining;

        private TransientReceiveRejected(String actionName, AtomicBoolean recoveryStarted, Runnable connectionBreaker) {
            this.actionName = actionName;
            this.recoveryStarted = recoveryStarted;
            this.connectionBreaker = connectionBreaker;
            this.blocksRemaining = new AtomicInteger(randomIntBetween(1, 3));
        }

        @Override
        public void messageReceived(
            TransportRequestHandler<TransportRequest> handler,
            TransportRequest request,
            TransportChannel channel,
            Task task
        ) throws Exception {
            recoveryStarted.set(true);
            if (blocksRemaining.getAndUpdate(i -> i == 0 ? 0 : i - 1) != 0) {
                String rejected = "rejected";
                String circuit = "circuit";
                String network = "network";
                String reason = randomFrom(rejected, circuit, network);
                if (reason.equals(rejected)) {
                    logger.info("--> preventing {} response by throwing exception", actionName);
                    throw new EsRejectedExecutionException();
                } else if (reason.equals(circuit)) {
                    logger.info("--> preventing {} response by throwing exception", actionName);
                    throw new CircuitBreakingException("Broken", CircuitBreaker.Durability.PERMANENT);
                } else if (reason.equals(network)) {
                    logger.info("--> preventing {} response by breaking connection", actionName);
                    connectionBreaker.run();
                } else {
                    throw new AssertionError("Unknown failure reason: " + reason);
                }
            }
            handler.messageReceived(request, channel, task);
        }
    }
}
