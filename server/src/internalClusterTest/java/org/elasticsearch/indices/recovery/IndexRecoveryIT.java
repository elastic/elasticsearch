/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.coordination.ApplyCommitRequest;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.ReplicaShardAllocatorIT;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.GlobalCheckpointListeners;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.recovery.RecoveryState.Stage;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.action.DocWriteResponse.Result.UPDATED;
import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.index.MergePolicyConfig.INDEX_MERGE_ENABLED;
import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.indices.IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexRecoveryIT extends AbstractIndexRecoveryIntegTestCase {

    private static final String INDEX_NAME = "test-idx-1";
    private static final String REPO_NAME = "test-repo-1";
    private static final String SNAP_NAME = "test-snap-1";

    private static final int MIN_DOC_COUNT = 500;
    private static final int MAX_DOC_COUNT = 1000;
    private static final int SHARD_COUNT_1 = 1;
    private static final int REPLICA_COUNT_0 = 0;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestAnalysisPlugin.class);
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        super.beforeIndexDeletion();
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
        internalCluster().assertSeqNos();
        internalCluster().assertSameDocIdsOnShards();
    }

    private void assertRecoveryStateWithoutStage(
        RecoveryState state,
        int shardId,
        RecoverySource recoverySource,
        boolean primary,
        String sourceNode,
        String targetNode
    ) {
        assertThat(state.getShardId().getId(), equalTo(shardId));
        assertThat(state.getRecoverySource(), equalTo(recoverySource));
        assertThat(state.getPrimary(), equalTo(primary));
        if (sourceNode == null) {
            assertNull(state.getSourceNode());
        } else {
            assertNotNull(state.getSourceNode());
            assertThat(state.getSourceNode().getName(), equalTo(sourceNode));
        }
        if (targetNode == null) {
            assertNull(state.getTargetNode());
        } else {
            assertNotNull(state.getTargetNode());
            assertThat(state.getTargetNode().getName(), equalTo(targetNode));
        }
    }

    private void assertRecoveryState(
        RecoveryState state,
        int shardId,
        RecoverySource type,
        boolean primary,
        Stage stage,
        String sourceNode,
        String targetNode
    ) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.getStage(), equalTo(stage));
    }

    private void assertOnGoingRecoveryState(
        RecoveryState state,
        int shardId,
        RecoverySource type,
        boolean primary,
        String sourceNode,
        String targetNode
    ) {
        assertRecoveryStateWithoutStage(state, shardId, type, primary, sourceNode, targetNode);
        assertThat(state.getStage(), not(equalTo(Stage.DONE)));
    }

    /**
     * Creates node settings that will throttle shard recovery to 'chunkSize' bytes per second.
     *
     * @param chunkSizeBytes size of the chunk in bytes
     * @return A Settings.Builder
     */
    public Settings.Builder createRecoverySettingsChunkPerSecond(long chunkSizeBytes) {
        return Settings.builder()
            // Set the chunk size in bytes
            .put(RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE.getKey(), ByteSizeValue.of(chunkSizeBytes, ByteSizeUnit.BYTES))
            // Set one chunk of bytes per second.
            .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), chunkSizeBytes, ByteSizeUnit.BYTES);
    }

    /**
     * Updates the cluster state settings to throttle recovery data transmission to 'dataSize' every 10 seconds.
     *
     * @param dataSize size in bytes to recover in 10 seconds
     */
    private void throttleRecovery10Seconds(ByteSizeValue dataSize) {
        long chunkSize = Math.max(1, dataSize.getBytes() / 10);
        updateClusterSettings(createRecoverySettingsChunkPerSecond(chunkSize));
    }

    /**
     * Sets high MB per second throttling for recovery on all nodes in the cluster.
     */
    private void unthrottleRecovery() {
        updateClusterSettings(
            Settings.builder()
                // 200mb is an arbitrary number intended to be large enough to avoid more throttling.
                .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "200mb")
                .put(RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE.getKey(), RecoverySettings.DEFAULT_CHUNK_SIZE)
        );
    }

    /**
     * Initiates a shard recovery and verifies that it's running.
     *
     * @param sourceNode node holding the shard
     * @param targetNode node that will recover the shard
     * @throws Exception
     */
    public void startShardRecovery(String sourceNode, String targetNode) throws Exception {
        logger.info("--> updating cluster settings with moving shard from node `{}` to node `{}`", sourceNode, targetNode);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(INDEX_NAME, 0, sourceNode, targetNode));

        logger.info("--> requesting shard recovery");
        indicesAdmin().prepareRecoveries(INDEX_NAME).get();

        logger.info("--> waiting for recovery to begin on both the source and target nodes");
        final Index index = resolveIndex(INDEX_NAME);
        assertBusy(() -> {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, sourceNode);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsSource(), equalTo(1));
            indicesService = internalCluster().getInstance(IndicesService.class, targetNode);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsTarget(), equalTo(1));
        });

        logger.info("--> checking cluster recovery stats reflect the ongoing recovery on each node");
        NodesStatsResponse statsResponse = clusterAdmin().prepareNodesStats()
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
            .get();
        for (NodeStats nodeStats : statsResponse.getNodes()) {
            final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
            if (nodeStats.getNode().getName().equals(sourceNode)) {
                assertThat(sourceNode + "  should have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(1));
                assertThat(sourceNode + " should not have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(0));
            }
            if (nodeStats.getNode().getName().equals(targetNode)) {
                assertThat(targetNode + " should not have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(0));
                assertThat(targetNode + " should have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(1));
            }
        }
    }

    /**
     * Asserts that the cluster stats show no shard recovery is active in the cluster and that 'nodeName' has >=0
     * throttling stats if 'isRecoveryThrottlingNode' or ==0 if not.
     *
     * @param nodeName the name of the node
     * @param isRecoveryThrottlingNode whether to expect throttling to have occurred on the node
     */
    public void assertNodeHasThrottleTimeAndNoRecoveries(String nodeName, Boolean isRecoveryThrottlingNode) {
        NodesStatsResponse nodesStatsResponse = clusterAdmin().prepareNodesStats(nodeName)
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
            .get();
        assertThat(nodesStatsResponse.getNodes(), hasSize(1));
        NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
        final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
        assertThat(recoveryStats.currentAsSource(), equalTo(0));
        assertThat(recoveryStats.currentAsTarget(), equalTo(0));
        if (isRecoveryThrottlingNode) {
            assertThat(
                "Throttling should be >0 for '" + nodeName + "'. Node stats: " + nodesStatsResponse,
                recoveryStats.throttleTime().millis(),
                greaterThan(0L)
            );
        } else {
            assertThat(
                "Throttling should be =0 for '" + nodeName + "'. Node stats: " + nodesStatsResponse,
                recoveryStats.throttleTime().millis(),
                equalTo(0L)
            );
        }
    }

    public void testGatewayRecovery() throws Exception {
        logger.info("--> start nodes");
        String node = internalCluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT_1, REPLICA_COUNT_0);

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = indicesAdmin().prepareRecoveries(INDEX_NAME).get();
        assertThat(response.shardRecoveryStates().size(), equalTo(SHARD_COUNT_1));
        assertThat(response.shardRecoveryStates().get(INDEX_NAME).size(), equalTo(1));

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat(recoveryStates.size(), equalTo(1));

        RecoveryState recoveryState = recoveryStates.get(0);

        assertRecoveryState(recoveryState, 0, RecoverySource.ExistingStoreRecoverySource.INSTANCE, true, Stage.DONE, null, node);

        validateIndexRecoveryState(recoveryState.getIndex());
    }

    public void testGatewayRecoveryTestActiveOnly() throws Exception {
        logger.info("--> start nodes");
        internalCluster().startNode();

        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT_1, REPLICA_COUNT_0);

        logger.info("--> restarting cluster");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = indicesAdmin().prepareRecoveries(INDEX_NAME).setActiveOnly(true).get();

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat(recoveryStates.size(), equalTo(0));  // Should not expect any responses back
    }

    public void testReplicaRecovery() throws Exception {
        final String nodeA = internalCluster().startNode();
        createIndex(INDEX_NAME, SHARD_COUNT_1, REPLICA_COUNT_0);
        ensureGreen(INDEX_NAME);

        final int numOfDocs = scaledRandomIntBetween(0, 200);
        try (BackgroundIndexer indexer = new BackgroundIndexer(INDEX_NAME, client(), numOfDocs)) {
            waitForDocs(numOfDocs, indexer);
        }

        refresh(INDEX_NAME);
        assertHitCount(prepareSearch(INDEX_NAME).setSize(0), numOfDocs);

        final boolean closedIndex = randomBoolean();
        if (closedIndex) {
            assertAcked(indicesAdmin().prepareClose(INDEX_NAME));
            ensureGreen(INDEX_NAME);
        }

        // force a shard recovery from nodeA to nodeB
        final String nodeB = internalCluster().startNode();
        setReplicaCount(1, INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final RecoveryResponse response = indicesAdmin().prepareRecoveries(INDEX_NAME).get();

        // we should now have two total shards, one primary and one replica
        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat(recoveryStates.size(), equalTo(2));

        List<RecoveryState> nodeAResponses = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeAResponses.size(), equalTo(1));
        List<RecoveryState> nodeBResponses = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBResponses.size(), equalTo(1));

        // validate node A recovery
        final RecoveryState nodeARecoveryState = nodeAResponses.get(0);
        final RecoverySource expectedRecoverySource;
        if (closedIndex == false) {
            expectedRecoverySource = RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        } else {
            expectedRecoverySource = RecoverySource.ExistingStoreRecoverySource.INSTANCE;
        }
        assertRecoveryState(nodeARecoveryState, 0, expectedRecoverySource, true, Stage.DONE, null, nodeA);
        validateIndexRecoveryState(nodeARecoveryState.getIndex());

        // validate node B recovery
        final RecoveryState nodeBRecoveryState = nodeBResponses.get(0);
        assertRecoveryState(nodeBRecoveryState, 0, PeerRecoverySource.INSTANCE, false, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryState.getIndex());

        internalCluster().stopNode(nodeA);

        if (closedIndex) {
            assertAcked(indicesAdmin().prepareOpen(INDEX_NAME));
        }
        assertHitCount(prepareSearch(INDEX_NAME).setSize(0), numOfDocs);
    }

    public void testCancelNewShardRecoveryAndUsesExistingShardCopy() throws Exception {
        logger.info("--> start node A");
        final String nodeA = internalCluster().startNode();

        updateClusterSettings(
            Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
        );
        logger.info("--> create index on node: {}", nodeA);
        createIndex(
            INDEX_NAME,
            indexSettings(1, 1).put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .build()
        );

        int numDocs = randomIntBetween(10, 200);
        final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex(INDEX_NAME).setSource(
                "foo-int",
                randomInt(),
                "foo-string",
                randomAlphaOfLength(32),
                "foo-float",
                randomFloat()
            );
        }
        indexRandom(randomBoolean(), docs);

        logger.info("--> start node B");
        // force a shard recovery from nodeA to nodeB
        final String nodeB = internalCluster().startNode();

        logger.info("--> add replica for {} on node: {}", INDEX_NAME, nodeB);
        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0),
            INDEX_NAME
        );
        ensureGreen(INDEX_NAME);

        logger.info("--> start node C");
        final String nodeC = internalCluster().startNode();

        ReplicaShardAllocatorIT.ensureActivePeerRecoveryRetentionLeasesAdvanced(INDEX_NAME);

        // hold peer recovery on phase 2 after nodeB down
        CountDownLatch phase1ReadyBlocked = new CountDownLatch(1);
        CountDownLatch allowToCompletePhase1Latch = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(nodeA);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.CLEAN_FILES.equals(action)) {
                phase1ReadyBlocked.countDown();
                safeAwait(allowToCompletePhase1Latch);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        logger.info("--> restart node B");
        internalCluster().restartNode(nodeB, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                safeAwait(phase1ReadyBlocked);
                // nodeB stopped, peer recovery from nodeA to nodeC, it will be cancelled after nodeB get started.
                RecoveryResponse response = indicesAdmin().prepareRecoveries(INDEX_NAME).get();

                List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
                List<RecoveryState> nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
                assertThat(nodeCRecoveryStates.size(), equalTo(1));

                assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, false, nodeA, nodeC);
                validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());

                return super.onNodeStopped(nodeName);
            }
        });

        // wait for peer recovery from nodeA to nodeB which is a no-op recovery so it skips the CLEAN_FILES stage and hence is not blocked
        ensureGreen();
        allowToCompletePhase1Latch.countDown();
        transportService.clearAllRules();

        // make sure nodeA has primary and nodeB has replica
        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        List<ShardRouting> startedShards = RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED);
        assertThat(startedShards.size(), equalTo(2));
        for (ShardRouting shardRouting : startedShards) {
            if (shardRouting.primary()) {
                assertThat(state.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(nodeA));
            } else {
                assertThat(state.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(nodeB));
            }
        }
    }

    public void testRerouteRecovery() throws Exception {
        logger.info("--> start node A");
        final String nodeA = internalCluster().startNode();

        logger.info("--> create index on node: {}", nodeA);
        ByteSizeValue shardSize = createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT_1, REPLICA_COUNT_0).getShards()[0].getStats()
            .getStore()
            .size();

        logger.info("--> start node B");
        final String nodeB = internalCluster().startNode(
            // Ensure that the target node has a high enough recovery max bytes per second to avoid any throttling
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "200mb")
        );

        ensureGreen();

        logger.info("--> slowing down recoveries");
        throttleRecovery10Seconds(shardSize);

        logger.info("--> move shard from: {} to: {}", nodeA, nodeB);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(INDEX_NAME, 0, nodeA, nodeB));

        logger.info("--> waiting for recovery to start both on source and target");
        final Index index = resolveIndex(INDEX_NAME);
        assertBusy(() -> {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeA);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsSource(), equalTo(1));
            indicesService = internalCluster().getInstance(IndicesService.class, nodeB);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsTarget(), equalTo(1));
        });

        logger.info("--> request recoveries");
        RecoveryResponse response = indicesAdmin().prepareRecoveries(INDEX_NAME).get();

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        List<RecoveryState> nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        List<RecoveryState> nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));

        assertRecoveryState(nodeARecoveryStates.get(0), 0, RecoverySource.EmptyStoreRecoverySource.INSTANCE, true, Stage.DONE, null, nodeA);
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertOnGoingRecoveryState(nodeBRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, true, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        logger.info("--> request node recovery stats");
        NodesStatsResponse statsResponse = clusterAdmin().prepareNodesStats()
            .clear()
            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
            .get();
        for (NodeStats nodeStats : statsResponse.getNodes()) {
            final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
            if (nodeStats.getNode().getName().equals(nodeA)) {
                assertThat("node A should have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(1));
                assertThat("node A should not have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(0));
            }
            if (nodeStats.getNode().getName().equals(nodeB)) {
                assertThat("node B should not have ongoing recovery as source", recoveryStats.currentAsSource(), equalTo(0));
                assertThat("node B should have ongoing recovery as target", recoveryStats.currentAsTarget(), equalTo(1));
            }
        }

        logger.info("--> speeding up recoveries");
        unthrottleRecovery();

        // wait for it to be finished
        ensureGreen();

        response = indicesAdmin().prepareRecoveries(INDEX_NAME).get();

        recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);
        assertThat(recoveryStates.size(), equalTo(1));

        assertRecoveryState(recoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, true, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(recoveryStates.get(0).getIndex());

        Consumer<String> assertNodeHasThrottleTimeAndNoRecoveries = nodeName -> {
            NodesStatsResponse nodesStatsResponse = clusterAdmin().prepareNodesStats(nodeName)
                .clear()
                .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
                .get();
            assertThat(nodesStatsResponse.getNodes(), hasSize(1));
            NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
            final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
            assertThat(recoveryStats.currentAsSource(), equalTo(0));
            assertThat(recoveryStats.currentAsTarget(), equalTo(0));
        };
        // we have to use assertBusy as recovery counters are decremented only when the last reference to the RecoveryTarget
        // is decremented, which may happen after the recovery was done.
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeA));
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries.accept(nodeB));

        logger.info("--> bump replica count");
        setReplicaCount(1, INDEX_NAME);
        ensureGreen();

        logger.info("--> start node C");
        String nodeC = internalCluster().startNode();
        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForNodes("3").get().isTimedOut());

        logger.info("--> slowing down recoveries");
        throttleRecovery10Seconds(shardSize);

        logger.info("--> move replica shard from: {} to: {}", nodeA, nodeC);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(INDEX_NAME, 0, nodeA, nodeC));

        response = indicesAdmin().prepareRecoveries(INDEX_NAME).get();
        recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);

        nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));
        List<RecoveryState> nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(nodeARecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, false, Stage.DONE, nodeB, nodeA);
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertRecoveryState(nodeBRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, true, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, false, nodeB, nodeC);
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());

        if (randomBoolean()) {
            // shutdown node with relocation source of replica shard and check if recovery continues
            internalCluster().stopNode(nodeA);
            ensureStableCluster(2);

            response = indicesAdmin().prepareRecoveries(INDEX_NAME).get();
            recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);

            nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
            assertThat(nodeARecoveryStates.size(), equalTo(0));
            nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
            assertThat(nodeBRecoveryStates.size(), equalTo(1));
            nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
            assertThat(nodeCRecoveryStates.size(), equalTo(1));

            assertRecoveryState(nodeBRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, true, Stage.DONE, nodeA, nodeB);
            validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

            assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, false, nodeB, nodeC);
            validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());
        }

        logger.info("--> speeding up recoveries");
        unthrottleRecovery();
        ensureGreen();

        response = indicesAdmin().prepareRecoveries(INDEX_NAME).get();
        recoveryStates = response.shardRecoveryStates().get(INDEX_NAME);

        nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(0));
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));
        nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(nodeBRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, true, Stage.DONE, nodeA, nodeB);
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        // relocations of replicas are marked as REPLICA and the source node is the node holding the primary (B)
        assertRecoveryState(nodeCRecoveryStates.get(0), 0, PeerRecoverySource.INSTANCE, false, Stage.DONE, nodeB, nodeC);
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());
    }

    /**
     * Tests shard recovery throttling on the source node. Node statistics should show throttling time on the source node, while no
     * throttling should be shown on the target node because the source will send data more slowly than the target's throttling threshold.
     */
    public void testSourceThrottling() throws Exception {
        // --- Cluster setup.

        logger.info("--> starting node A with default settings");
        final String nodeA = internalCluster().startNode();

        logger.info("--> creating index on node A");
        ByteSizeValue shardSize = createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT_1, REPLICA_COUNT_0).getShards()[0].getStats()
            .getStore()
            .size();

        logger.info("--> starting node B with default settings");
        final String nodeB = internalCluster().startNode();

        long chunkSize = Math.max(1, shardSize.getBytes() / 10);
        logger.info(
            "--> restarting node A with recovery throttling settings. Index shard size is `{}`. Throttling down to a "
                + "chunk of size `{}` per second. This will slow recovery of the shard to 10 seconds.",
            shardSize,
            ByteSizeValue.ofBytes(chunkSize)
        );
        internalCluster().restartNode(nodeA, new InternalTestCluster.RestartCallback() {
            // This callback returns node Settings that are ultimately passed into the restarted node.
            @Override
            public Settings onNodeStopped(String nodeName) {
                return createRecoverySettingsChunkPerSecond(chunkSize).build();
            }
        });

        logger.info("--> waiting for the cluster to stabilize after restarting the source node (Node A)");
        ensureGreen();

        // --- Shard recovery.

        startShardRecovery(nodeA, nodeB);

        logger.info("--> checking throttling increases on Node A (source node), while Node B (target node) reports no throttling");
        assertBusy(() -> {
            NodesStatsResponse nodeStatsResponse = clusterAdmin().prepareNodesStats()
                .clear()
                .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
                .get();
            assertThat(nodeStatsResponse.getNodes(), hasSize(2));
            for (NodeStats nodeStats : nodeStatsResponse.getNodes()) {
                final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
                if (nodeStats.getNode().getName().equals(nodeA)) {
                    assertThat("node A throttling should  increase", recoveryStats.throttleTime().millis(), greaterThan(0L));
                }
                if (nodeStats.getNode().getName().equals(nodeB)) {
                    assertThat("node B throttling should _not_ increase", recoveryStats.throttleTime().millis(), equalTo(0L));
                }
            }
        });

        logger.info("--> increasing the recovery throttle limit so that the shard recovery completes quickly");
        unthrottleRecovery();

        logger.info("--> waiting for the shard recovery to complete");
        ensureGreen();

        // --- Shard recovery complete. Verify throttling millis remain reflected in node stats.

        logger.info("--> checking that both nodes A and B no longer have recoveries in progress, but that they do retain throttling stats");
        // We must use assertBusy because recovery counters are decremented only when the last reference to
        // the RecoveryTarget is decremented, which may happen after the recovery finishes.
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries(nodeA, true));
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries(nodeB, false));
    }

    /**
     * Tests shard recovery throttling on the target node. Node statistics should show throttling time on the target node, while no
     * throttling should be shown on the source node because the target will accept data more slowly than the source's throttling threshold.
     */
    public void testTargetThrottling() throws Exception {
        logger.info("--> starting node A with default settings");
        final String nodeA = internalCluster().startNode(
            Settings.builder()
                // Use a high value so that when unthrottling recoveries we do not cause accidental throttling on the source node.
                .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "200mb")
        );

        logger.info("--> creating index on node A");
        ByteSizeValue shardSize = createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT_1, REPLICA_COUNT_0).getShards()[0].getStats()
            .getStore()
            .size();

        long chunkSize = Math.max(1, shardSize.getBytes() / 10);
        logger.info(
            "--> starting node B with recovery throttling settings. Index shard size is `{}`. Throttling down to a "
                + "chunk of size `{}` per second. This will slow recovery of the existing shard to 10 seconds.",
            shardSize,
            ByteSizeValue.ofBytes(chunkSize)
        );
        final String nodeB = internalCluster().startNode(createRecoverySettingsChunkPerSecond(chunkSize));

        logger.info("--> waiting for the cluster to stabilize after restarting the target node (Node B)");
        ensureGreen();

        // --- Shard recovery.

        startShardRecovery(nodeA, nodeB);

        logger.info("--> checking throttling increases on Node B (target node), while Node A (source node) reports no throttling");
        assertBusy(() -> {
            NodesStatsResponse statsResponse1 = clusterAdmin().prepareNodesStats()
                .clear()
                .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Recovery))
                .get();
            assertThat(statsResponse1.getNodes(), hasSize(2));
            for (NodeStats nodeStats : statsResponse1.getNodes()) {
                final RecoveryStats recoveryStats = nodeStats.getIndices().getRecoveryStats();
                if (nodeStats.getNode().getName().equals(nodeA)) {
                    assertThat("node A throttling should _not_ increase", recoveryStats.throttleTime().millis(), equalTo(0L));
                }
                if (nodeStats.getNode().getName().equals(nodeB)) {
                    assertThat("node B throttling should increase", recoveryStats.throttleTime().millis(), greaterThan(0L));
                }
            }
        });

        logger.info("--> increasing the recovery throttle limit so that the shard recovery completes quickly");
        unthrottleRecovery();

        logger.info("--> waiting for the shard recovery to complete");
        ensureGreen();

        // --- Shard recovery complete. Verify throttling millis remain reflected in node stats.

        logger.info("--> checking that both nodes A and B no longer have recoveries in progress, but that they do retain throttling stats");
        // we have to use assertBusy as recovery counters are decremented only when the last reference to the RecoveryTarget
        // is decremented, which may happen after the recovery was done.
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries(nodeA, false));
        assertBusy(() -> assertNodeHasThrottleTimeAndNoRecoveries(nodeB, true));
    }

    public void testSnapshotRecovery() throws Exception {
        logger.info("--> start node A");
        String nodeA = internalCluster().startNode();

        logger.info("--> create repository");
        createRepository(randomBoolean());

        ensureGreen();

        logger.info("--> create index on node: {}", nodeA);
        createAndPopulateIndex(INDEX_NAME, 1, SHARD_COUNT_1, REPLICA_COUNT_0);

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = createSnapshot(INDEX_NAME);

        indicesAdmin().prepareClose(INDEX_NAME).get();

        logger.info("--> restore");
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, REPO_NAME, SNAP_NAME)
            .setWaitForCompletion(true)
            .get();
        int totalShards = restoreSnapshotResponse.getRestoreInfo().totalShards();
        assertThat(totalShards, greaterThan(0));

        ensureGreen();

        logger.info("--> request recoveries");
        RecoveryResponse response = indicesAdmin().prepareRecoveries(INDEX_NAME).get();

        Repository repository = internalCluster().getAnyMasterNodeInstance(RepositoriesService.class).repository(REPO_NAME);
        final RepositoryData repositoryData = AbstractSnapshotIntegTestCase.getRepositoryData(repository);
        for (Map.Entry<String, List<RecoveryState>> indexRecoveryStates : response.shardRecoveryStates().entrySet()) {

            assertThat(indexRecoveryStates.getKey(), equalTo(INDEX_NAME));
            List<RecoveryState> recoveryStates = indexRecoveryStates.getValue();
            assertThat(recoveryStates.size(), equalTo(totalShards));

            for (RecoveryState recoveryState : recoveryStates) {
                SnapshotRecoverySource recoverySource = new SnapshotRecoverySource(
                    ((SnapshotRecoverySource) recoveryState.getRecoverySource()).restoreUUID(),
                    new Snapshot(REPO_NAME, createSnapshotResponse.getSnapshotInfo().snapshotId()),
                    IndexVersion.current(),
                    repositoryData.resolveIndexId(INDEX_NAME)
                );
                assertRecoveryState(recoveryState, 0, recoverySource, true, Stage.DONE, null, nodeA);
                validateIndexRecoveryState(recoveryState.getIndex());
            }
        }
    }

    private List<RecoveryState> findRecoveriesForTargetNode(String nodeName, List<RecoveryState> recoveryStates) {
        List<RecoveryState> nodeResponses = new ArrayList<>();
        for (RecoveryState recoveryState : recoveryStates) {
            if (recoveryState.getTargetNode().getName().equals(nodeName)) {
                nodeResponses.add(recoveryState);
            }
        }
        return nodeResponses;
    }

    private IndicesStatsResponse createAndPopulateIndex(String name, int nodeCount, int shardCount, int replicaCount)
        throws ExecutionException, InterruptedException {

        logger.info("--> creating test index: {}", name);
        assertAcked(
            prepareCreate(
                name,
                nodeCount,
                indexSettings(shardCount, replicaCount).put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), 0)
            )
        );
        ensureGreen();

        logger.info("--> indexing sample data");
        final int numDocs = between(MIN_DOC_COUNT, MAX_DOC_COUNT);
        final IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];

        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex(name).setSource(
                "foo-int",
                randomInt(),
                "foo-string",
                randomAlphaOfLength(32),
                "foo-float",
                randomFloat()
            );
        }

        indexRandom(true, docs);
        flush();
        assertHitCount(prepareSearch(name).setSize(0), numDocs);
        return indicesAdmin().prepareStats(name).get();
    }

    private void validateIndexRecoveryState(RecoveryState.Index indexState) {
        assertThat(indexState.time(), greaterThanOrEqualTo(0L));
        assertThat(indexState.recoveredFilesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredFilesPercent(), lessThanOrEqualTo(100.0f));
        assertThat(indexState.recoveredBytesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredBytesPercent(), lessThanOrEqualTo(100.0f));
    }

    public void testTransientErrorsDuringRecoveryAreRetried() throws Exception {
        final String recoveryActionToBlock = randomFrom(
            PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
            PeerRecoveryTargetService.Actions.TRANSLOG_OPS,
            PeerRecoveryTargetService.Actions.FILES_INFO,
            // PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT, --> recovery from snapshot is a x-pack plugin
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            PeerRecoveryTargetService.Actions.CLEAN_FILES,
            PeerRecoveryTargetService.Actions.FINALIZE
        );
        checkTransientErrorsDuringRecoveryAreRetried(recoveryActionToBlock);
    }

    public void testDisconnectsWhileRecovering() throws Exception {
        final String recoveryActionToBlock = randomFrom(
            PeerRecoverySourceService.Actions.START_RECOVERY,
            PeerRecoveryTargetService.Actions.FILES_INFO,
            // PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT, --> recovery from snapshot is a x-pack plugin
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            PeerRecoveryTargetService.Actions.CLEAN_FILES,
            // RecoveryTarget.Actions.TRANSLOG_OPS, <-- may not be sent if already flushed
            PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
            PeerRecoveryTargetService.Actions.FINALIZE
        );
        checkDisconnectsWhileRecovering(recoveryActionToBlock);
    }

    /**
     * Tests scenario where recovery target successfully sends recovery request to source but then the channel gets closed while
     * the source is working on the recovery process.
     */
    public void testDisconnectsDuringRecovery() throws Exception {
        checkDisconnectsDuringRecovery(false);
    }

    public void testHistoryRetention() throws Exception {
        internalCluster().startNodes(3);

        final String indexName = "test";
        indicesAdmin().prepareCreate(indexName)
            .setSettings(indexSettings(1, 2).put(IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.getKey(), 1.0))
            .get();
        ensureGreen(indexName);

        // Perform some replicated operations so the replica isn't simply empty, because ops-based recovery isn't better in that case
        final List<IndexRequestBuilder> requests = new ArrayList<>();
        final int replicatedDocCount = scaledRandomIntBetween(25, 250);
        while (requests.size() < replicatedDocCount) {
            requests.add(prepareIndex(indexName).setSource("{}", XContentType.JSON));
        }
        indexRandom(true, requests);
        if (randomBoolean()) {
            flush(indexName);
        }

        String firstNodeToStop = randomFrom(internalCluster().getNodeNames());
        Settings firstNodeToStopDataPathSettings = internalCluster().dataPathSettings(firstNodeToStop);
        internalCluster().stopNode(firstNodeToStop);
        String secondNodeToStop = randomFrom(internalCluster().getNodeNames());
        Settings secondNodeToStopDataPathSettings = internalCluster().dataPathSettings(secondNodeToStop);
        internalCluster().stopNode(secondNodeToStop);

        final long desyncNanoTime = System.nanoTime();
        // noinspection StatementWithEmptyBody
        while (System.nanoTime() <= desyncNanoTime) {
            // time passes
        }

        final int numNewDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numNewDocs; i++) {
            prepareIndex(indexName).setSource("{}", XContentType.JSON).setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        }
        // Flush twice to update the safe commit's local checkpoint
        assertThat(indicesAdmin().prepareFlush(indexName).setForce(true).execute().get().getFailedShards(), equalTo(0));
        assertThat(indicesAdmin().prepareFlush(indexName).setForce(true).execute().get().getFailedShards(), equalTo(0));

        setReplicaCount(1, indexName);
        internalCluster().startNode(randomFrom(firstNodeToStopDataPathSettings, secondNodeToStopDataPathSettings));
        ensureGreen(indexName);

        final RecoveryResponse recoveryResponse = indicesAdmin().recoveries(new RecoveryRequest(indexName)).get();
        final List<RecoveryState> recoveryStates = recoveryResponse.shardRecoveryStates().get(indexName);
        recoveryStates.removeIf(r -> r.getTimer().getStartNanoTime() <= desyncNanoTime);

        assertThat(recoveryStates, hasSize(1));
        final RecoveryState recoveryState = recoveryStates.get(0);
        assertThat(Strings.toString(recoveryState), recoveryState.getIndex().totalFileCount(), is(0));
        assertThat(recoveryState.getTranslog().recoveredOperations(), greaterThan(0));
    }

    public void testDoNotInfinitelyWaitForMapping() {
        internalCluster().ensureAtLeastNumDataNodes(3);
        createIndex(
            "test",
            indexSettings(1, 0).put("index.analysis.analyzer.test_analyzer.type", "custom")
                .put("index.analysis.analyzer.test_analyzer.tokenizer", "standard")
                .putList("index.analysis.analyzer.test_analyzer.filter", "test_token_filter")
                .build()
        );
        indicesAdmin().preparePutMapping("test").setSource("test_field", "type=text,analyzer=test_analyzer").get();
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setId("u" + i).setSource(singletonMap("test_field", Integer.toString(i)), XContentType.JSON).get();
        }
        Semaphore recoveryBlocked = new Semaphore(1);
        for (DiscoveryNode node : clusterService().state().nodes()) {
            MockTransportService.getInstance(node.getName()).addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(PeerRecoverySourceService.Actions.START_RECOVERY)) {
                    if (recoveryBlocked.tryAcquire()) {
                        PluginsService pluginService = internalCluster().getInstance(PluginsService.class, node.getName());
                        pluginService.filterPlugins(TestAnalysisPlugin.class).forEach(p -> p.throwParsingError.set(true));
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        setReplicaCount(1, "test");

        var allocator = internalCluster().getInstance(ShardsAllocator.class);
        if (allocator instanceof BalancedShardsAllocator) {
            // BalancedShardsAllocator will try other node once retries are exhausted
            ensureGreen("test");
        } else if (allocator instanceof DesiredBalanceShardsAllocator) {
            // DesiredBalanceShardsAllocator will keep shard in the error state if it could not be allocated on the desired node
            ensureYellow("test");
        } else {
            fail("Unknown allocator used");
        }

        indicesAdmin().prepareRefresh("test").get();
        assertHitCount(prepareSearch(), numDocs);
    }

    /** Makes sure the new master does not repeatedly fetch index metadata from recovering replicas */
    public void testOngoingRecoveryAndMasterFailOver() throws Exception {
        String indexName = "test";
        internalCluster().startNodes(2);
        String nodeWithPrimary = internalCluster().startDataOnlyNode();
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.include._name", nodeWithPrimary))
        );
        CountDownLatch phase1ReadyBlocked = new CountDownLatch(1);
        CountDownLatch allowToCompletePhase1Latch = new CountDownLatch(1);
        Semaphore blockRecovery = new Semaphore(1);
        MockTransportService.getInstance(nodeWithPrimary).addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.CLEAN_FILES.equals(action) && blockRecovery.tryAcquire()) {
                phase1ReadyBlocked.countDown();
                safeAwait(allowToCompletePhase1Latch);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        try {
            String nodeWithReplica = internalCluster().startDataOnlyNode();
            updateIndexSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put("index.routing.allocation.include._name", nodeWithPrimary + "," + nodeWithReplica),
                indexName
            );
            safeAwait(phase1ReadyBlocked);
            internalCluster().restartNode(
                clusterService().state().nodes().getMasterNode().getName(),
                new InternalTestCluster.RestartCallback()
            );
            internalCluster().ensureAtLeastNumDataNodes(3);
            updateIndexSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).putNull("index.routing.allocation.include._name"),
                indexName
            );
            assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).setWaitForActiveShards(2).get().isTimedOut());
        } finally {
            allowToCompletePhase1Latch.countDown();
        }
        ensureGreen(indexName);
    }

    @TestLogging(
        reason = "https://github.com/elastic/elasticsearch/issues/89235",
        value = "org.elasticsearch.indices.recovery:TRACE, org.elasticsearch.index.shard:TRACE, org.elasticsearch.index.engine:TRACE"
    )
    public void testRecoverLocallyUpToGlobalCheckpoint() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        List<String> nodes = randomSubsetOf(
            2,
            clusterService().state().nodes().getDataNodes().values().stream().map(DiscoveryNode::getName).collect(Collectors.toSet())
        );
        String indexName = "test-index";
        createIndex(
            indexName,
            indexSettings(1, 1)
                // disable global checkpoint background sync so we can verify the start recovery request
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "12h")
                .put("index.routing.allocation.include._name", String.join(",", nodes))
                .build()
        );
        ensureGreen(indexName);
        int numDocs = randomIntBetween(0, 100);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, numDocs).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );
        indicesAdmin().prepareRefresh(indexName).get(); // avoid refresh when we are failing a shard
        String failingNode = randomFrom(nodes);
        PlainActionFuture<StartRecoveryRequest> startRecoveryRequestFuture = new PlainActionFuture<>();
        // Peer recovery fails if the primary does not see the recovering replica in the replication group (when the cluster state
        // update on the primary is delayed). To verify the local recovery stats, we have to manually remember this value in the
        // first try because the local recovery happens once and its stats is reset when the recovery fails.
        SetOnce<Integer> localRecoveredOps = new SetOnce<>();
        for (String node : nodes) {
            MockTransportService.getInstance(node).addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(PeerRecoverySourceService.Actions.START_RECOVERY)) {
                    final RecoveryState recoveryState = internalCluster().getInstance(IndicesService.class, failingNode)
                        .getShardOrNull(new ShardId(resolveIndex(indexName), 0))
                        .recoveryState();
                    assertThat(recoveryState.getTranslog().recoveredOperations(), equalTo(recoveryState.getTranslog().totalLocal()));
                    if (startRecoveryRequestFuture.isDone()) {
                        assertThat(recoveryState.getTranslog().totalLocal(), equalTo(0));
                        recoveryState.getTranslog().totalLocal(localRecoveredOps.get());
                        recoveryState.getTranslog().incrementRecoveredOperations(localRecoveredOps.get());
                    } else {
                        localRecoveredOps.set(recoveryState.getTranslog().totalLocal());
                        startRecoveryRequestFuture.onResponse((StartRecoveryRequest) request);
                    }
                }
                if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                    RetentionLeases retentionLeases = internalCluster().getInstance(IndicesService.class, node)
                        .indexServiceSafe(resolveIndex(indexName))
                        .getShard(0)
                        .getRetentionLeases();
                    throw new AssertionError(
                        "expect an operation-based recovery:retention leases" + Strings.toString(retentionLeases) + "]"
                    );
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }
        assertGlobalCheckpointIsStableAndSyncedInAllNodes(indexName, nodes, 0);

        IndexShard shard = internalCluster().getInstance(IndicesService.class, failingNode)
            .getShardOrNull(new ShardId(resolveIndex(indexName), 0));
        final long lastSyncedGlobalCheckpoint = shard.getLastSyncedGlobalCheckpoint();
        final long localCheckpointOfSafeCommit;
        try (Engine.IndexCommitRef safeCommitRef = shard.acquireSafeIndexCommit()) {
            localCheckpointOfSafeCommit = getLocalCheckpointOfSafeCommit(safeCommitRef.getIndexCommit());
        }
        final long maxSeqNo = shard.seqNoStats().getMaxSeqNo();
        shard.failShard("test", new IOException("simulated"));
        StartRecoveryRequest startRecoveryRequest = startRecoveryRequestFuture.actionGet();
        logger.info(
            "--> start recovery request: starting seq_no {}, commit {}",
            startRecoveryRequest.startingSeqNo(),
            startRecoveryRequest.metadataSnapshot().commitUserData()
        );
        SequenceNumbers.CommitInfo commitInfoAfterLocalRecovery = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
            startRecoveryRequest.metadataSnapshot().commitUserData().entrySet()
        );
        assertThat(commitInfoAfterLocalRecovery.localCheckpoint(), equalTo(lastSyncedGlobalCheckpoint));
        assertThat(commitInfoAfterLocalRecovery.maxSeqNo(), equalTo(lastSyncedGlobalCheckpoint));
        assertThat(startRecoveryRequest.startingSeqNo(), equalTo(lastSyncedGlobalCheckpoint + 1));
        ensureGreen(indexName);
        assertThat((long) localRecoveredOps.get(), equalTo(lastSyncedGlobalCheckpoint - localCheckpointOfSafeCommit));
        for (RecoveryState recoveryState : indicesAdmin().prepareRecoveries().get().shardRecoveryStates().get(indexName)) {
            if (startRecoveryRequest.targetNode().equals(recoveryState.getTargetNode())) {
                assertThat("expect an operation-based recovery", recoveryState.getIndex().fileDetails(), empty());
                assertThat(
                    "total recovered translog operations must include both local and remote recovery",
                    recoveryState.getTranslog().recoveredOperations(),
                    greaterThanOrEqualTo(Math.toIntExact(maxSeqNo - localCheckpointOfSafeCommit))
                );
            }
        }
        for (String node : nodes) {
            MockTransportService.getInstance(node).clearAllRules();
        }
    }

    public void testUsesFileBasedRecoveryIfRetentionLeaseMissing() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        String indexName = "test-index";
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "12h")
                .build()
        );
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(0, 100)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );
        ensureGreen(indexName);

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final DiscoveryNodes discoveryNodes = clusterService().state().nodes();
        final IndexShardRoutingTable indexShardRoutingTable = clusterService().state().routingTable().shardRoutingTable(shardId);

        final IndexShard primary = internalCluster().getInstance(
            IndicesService.class,
            discoveryNodes.get(indexShardRoutingTable.primaryShard().currentNodeId()).getName()
        ).getShardOrNull(shardId);

        final ShardRouting replicaShardRouting = indexShardRoutingTable.replicaShards().get(0);
        internalCluster().restartNode(
            discoveryNodes.get(replicaShardRouting.currentNodeId()).getName(),
            new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    assertFalse(
                        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                            .setWaitForNodes(Integer.toString(discoveryNodes.getSize() - 1))
                            .setWaitForEvents(Priority.LANGUID)
                            .get()
                            .isTimedOut()
                    );

                    final PlainActionFuture<ReplicationResponse> future = new PlainActionFuture<>();
                    primary.removeRetentionLease(ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaShardRouting), future);
                    future.get();

                    return super.onNodeStopped(nodeName);
                }
            }
        );

        ensureGreen(indexName);

        // noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final RecoveryState recoveryState = indicesAdmin().prepareRecoveries(indexName)
            .get()
            .shardRecoveryStates()
            .get(indexName)
            .stream()
            .filter(rs -> rs.getPrimary() == false)
            .findFirst()
            .get();
        assertThat(recoveryState.getIndex().totalFileCount(), greaterThan(0));
    }

    public void testUsesFileBasedRecoveryIfRetentionLeaseAheadOfGlobalCheckpoint() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        String indexName = "test-index";
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "12h")
                .build()
        );
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(0, 100)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );
        ensureGreen(indexName);

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final DiscoveryNodes discoveryNodes = clusterService().state().nodes();
        final IndexShardRoutingTable indexShardRoutingTable = clusterService().state().routingTable().shardRoutingTable(shardId);

        final IndexShard primary = internalCluster().getInstance(
            IndicesService.class,
            discoveryNodes.get(indexShardRoutingTable.primaryShard().currentNodeId()).getName()
        ).getShardOrNull(shardId);

        final ShardRouting replicaShardRouting = indexShardRoutingTable.replicaShards().get(0);
        internalCluster().restartNode(
            discoveryNodes.get(replicaShardRouting.currentNodeId()).getName(),
            new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    assertFalse(
                        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                            .setWaitForNodes(Integer.toString(discoveryNodes.getSize() - 1))
                            .setWaitForEvents(Priority.LANGUID)
                            .get()
                            .isTimedOut()
                    );

                    indexRandom(
                        randomBoolean(),
                        randomBoolean(),
                        randomBoolean(),
                        IntStream.range(0, between(1, 100)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
                    );

                    // We do not guarantee that the replica can recover locally all the way to its own global checkpoint before starting
                    // to recover from the primary, so we must be careful not to perform an operations-based recovery if this would require
                    // some operations that are not being retained. Emulate this by advancing the lease ahead of the replica's GCP:
                    primary.renewRetentionLease(
                        ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaShardRouting),
                        primary.seqNoStats().getMaxSeqNo() + 1,
                        ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE
                    );

                    return super.onNodeStopped(nodeName);
                }
            }
        );

        ensureGreen(indexName);

        // noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final RecoveryState recoveryState = indicesAdmin().prepareRecoveries(indexName)
            .get()
            .shardRecoveryStates()
            .get(indexName)
            .stream()
            .filter(rs -> rs.getPrimary() == false)
            .findFirst()
            .get();
        assertThat(recoveryState.getIndex().totalFileCount(), greaterThan(0));
    }

    public void testUsesFileBasedRecoveryIfOperationsBasedRecoveryWouldBeUnreasonable() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        String indexName = "test-index";
        final Settings.Builder settings = indexSettings(1, 1).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "12h")
            .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms");

        final double reasonableOperationsBasedRecoveryProportion;
        if (randomBoolean()) {
            reasonableOperationsBasedRecoveryProportion = randomDoubleBetween(0.05, 0.99, true);
            settings.put(IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.getKey(), reasonableOperationsBasedRecoveryProportion);
        } else {
            reasonableOperationsBasedRecoveryProportion = IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.get(Settings.EMPTY);
        }
        logger.info("--> performing ops-based recoveries up to [{}%] of docs", reasonableOperationsBasedRecoveryProportion * 100.0);

        createIndex(indexName, settings.build());
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, between(0, 100)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );
        ensureGreen(indexName);

        flush(indexName);
        // wait for all history to be discarded
        assertBusy(() -> {
            for (ShardStats shardStats : indicesAdmin().prepareStats(indexName).get().getShards()) {
                final long maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
                assertTrue(
                    shardStats.getRetentionLeaseStats().retentionLeases() + " should discard history up to " + maxSeqNo,
                    shardStats.getRetentionLeaseStats()
                        .retentionLeases()
                        .leases()
                        .stream()
                        .allMatch(l -> l.retainingSequenceNumber() == maxSeqNo + 1)
                );
            }
        });
        flush(indexName); // ensure that all operations are in the safe commit

        final ShardStats shardStats = indicesAdmin().prepareStats(indexName).get().getShards()[0];
        final long docCount = shardStats.getStats().docs.getCount();
        assertThat(shardStats.getStats().docs.getDeleted(), equalTo(0L));
        assertThat(shardStats.getSeqNoStats().getMaxSeqNo() + 1, equalTo(docCount));

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final DiscoveryNodes discoveryNodes = clusterService().state().nodes();
        final IndexShardRoutingTable indexShardRoutingTable = clusterService().state().routingTable().shardRoutingTable(shardId);

        final ShardRouting replicaShardRouting = indexShardRoutingTable.replicaShards().get(0);
        assertTrue(
            "should have lease for " + replicaShardRouting,
            indicesAdmin().prepareStats(indexName).get().getShards()[0].getRetentionLeaseStats()
                .retentionLeases()
                .contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaShardRouting))
        );
        internalCluster().restartNode(
            discoveryNodes.get(replicaShardRouting.currentNodeId()).getName(),
            new InternalTestCluster.RestartCallback() {
                @Override
                public Settings onNodeStopped(String nodeName) throws Exception {
                    assertFalse(
                        clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                            .setWaitForNodes(Integer.toString(discoveryNodes.getSize() - 1))
                            .setWaitForEvents(Priority.LANGUID)
                            .get()
                            .isTimedOut()
                    );

                    final int newDocCount = Math.toIntExact(
                        Math.round(
                            Math.ceil(
                                (1 + Math.ceil(docCount * reasonableOperationsBasedRecoveryProportion)) / (1
                                    - reasonableOperationsBasedRecoveryProportion)
                            )
                        )
                    );

                    /*
                     *     newDocCount >= (ceil(docCount * p) + 1) / (1-p)
                     *
                     * ==> 0 <= newDocCount * (1-p) - ceil(docCount * p) - 1
                     *       =  newDocCount - (newDocCount * p + ceil(docCount * p) + 1)
                     *       <  newDocCount - (ceil(newDocCount * p) + ceil(docCount * p))
                     *       <= newDocCount -  ceil(newDocCount * p + docCount * p)
                     *
                     * ==> docCount <  newDocCount + docCount - ceil((newDocCount + docCount) * p)
                     *              == localCheckpoint + 1    - ceil((newDocCount + docCount) * p)
                     *              == firstReasonableSeqNo
                     *
                     * The replica has docCount docs, i.e. has operations with seqnos [0..docCount-1], so a seqno-based recovery will start
                     * from docCount < firstReasonableSeqNo
                     *
                     * ==> it is unreasonable to recover the replica using a seqno-based recovery
                     */

                    indexRandom(
                        randomBoolean(),
                        randomBoolean(),
                        randomBoolean(),
                        IntStream.range(0, newDocCount).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
                    );

                    flush(indexName);

                    assertBusy(
                        () -> assertFalse(
                            "should no longer have lease for " + replicaShardRouting,
                            indicesAdmin().prepareStats(indexName).get().getShards()[0].getRetentionLeaseStats()
                                .retentionLeases()
                                .contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(replicaShardRouting))
                        )
                    );

                    return super.onNodeStopped(nodeName);
                }
            }
        );

        ensureGreen(indexName);

        // noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final RecoveryState recoveryState = indicesAdmin().prepareRecoveries(indexName)
            .get()
            .shardRecoveryStates()
            .get(indexName)
            .stream()
            .filter(rs -> rs.getPrimary() == false)
            .findFirst()
            .get();
        assertThat(recoveryState.getIndex().totalFileCount(), greaterThan(0));
    }

    public void testDoesNotCopyOperationsInSafeCommit() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build());
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(0, 100)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final DiscoveryNodes discoveryNodes = clusterService().state().nodes();
        final IndexShardRoutingTable indexShardRoutingTable = clusterService().state().routingTable().shardRoutingTable(shardId);

        final IndexShard primary = internalCluster().getInstance(
            IndicesService.class,
            discoveryNodes.get(indexShardRoutingTable.primaryShard().currentNodeId()).getName()
        ).getShardOrNull(shardId);
        final long maxSeqNoBeforeRecovery = primary.seqNoStats().getMaxSeqNo();
        assertBusy(() -> assertThat(primary.getLastSyncedGlobalCheckpoint(), equalTo(maxSeqNoBeforeRecovery)));
        assertThat(indicesAdmin().prepareFlush(indexName).get().getFailedShards(), is(0)); // makes a safe commit

        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(0, 100)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );

        setReplicaCount(1, indexName);
        ensureGreen(indexName);
        final long maxSeqNoAfterRecovery = primary.seqNoStats().getMaxSeqNo();

        // noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final RecoveryState recoveryState = indicesAdmin().prepareRecoveries(indexName)
            .get()
            .shardRecoveryStates()
            .get(indexName)
            .stream()
            .filter(rs -> rs.getPrimary() == false)
            .findFirst()
            .get();
        assertThat(
            (long) recoveryState.getTranslog().recoveredOperations(),
            lessThanOrEqualTo(maxSeqNoAfterRecovery - maxSeqNoBeforeRecovery)
        );
    }

    public static final class TestAnalysisPlugin extends Plugin implements AnalysisPlugin {
        final AtomicBoolean throwParsingError = new AtomicBoolean();

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
            return singletonMap("test_token_filter", (indexSettings, environment, name, settings) -> new AbstractTokenFilterFactory(name) {
                @Override
                public TokenStream create(TokenStream tokenStream) {
                    if (throwParsingError.get()) {
                        throw new MapperParsingException("simulate mapping parsing error");
                    }
                    return tokenStream;
                }
            });
        }
    }

    public void testRepeatedRecovery() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        // Ensures that you can remove a replica and then add it back again without any ill effects, even if it's allocated back to the
        // node that held it previously, in case that node hasn't completely cleared it up.

        final String indexName = "test-index";
        createIndex(
            indexName,
            indexSettings(randomIntBetween(1, 6), 1).put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "200ms").build()
        );
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, randomIntBetween(0, 10)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );

        assertThat(indicesAdmin().prepareFlush(indexName).get().getFailedShards(), equalTo(0));

        assertBusy(() -> {
            final ShardStats[] shardsStats = indicesAdmin().prepareStats(indexName).get().getIndex(indexName).getShards();
            for (final ShardStats shardStats : shardsStats) {
                final long maxSeqNo = shardStats.getSeqNoStats().getMaxSeqNo();
                assertTrue(
                    shardStats.getRetentionLeaseStats()
                        .retentionLeases()
                        .leases()
                        .stream()
                        .allMatch(retentionLease -> retentionLease.retainingSequenceNumber() == maxSeqNo + 1)
                );
            }
        });

        logger.info("--> remove replicas");
        setReplicaCount(0, indexName);
        ensureGreen(indexName);

        logger.info("--> index more documents");
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, randomIntBetween(0, 10)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );

        logger.info("--> add replicas again");
        setReplicaCount(1, indexName);
        ensureGreen(indexName);
    }

    public void testAllocateEmptyPrimaryResetsGlobalCheckpoint() throws Exception {
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final Settings randomNodeDataPathSettings = internalCluster().dataPathSettings(randomFrom(dataNodes));
        final String indexName = "test";
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(indexSettings(1, 1).put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), randomBoolean()))
        );
        final List<IndexRequestBuilder> indexRequests = IntStream.range(0, between(10, 500))
            .mapToObj(n -> prepareIndex(indexName).setSource("foo", "bar"))
            .toList();
        indexRandom(randomBoolean(), true, true, indexRequests);
        ensureGreen();
        internalCluster().stopRandomDataNode();
        internalCluster().stopRandomDataNode();
        final String nodeWithoutData = internalCluster().startDataOnlyNode();
        ClusterRerouteUtils.reroute(client(), new AllocateEmptyPrimaryAllocationCommand(indexName, 0, nodeWithoutData, true));
        internalCluster().startDataOnlyNode(randomNodeDataPathSettings);
        ensureGreen();
        for (ShardStats shardStats : indicesAdmin().prepareStats(indexName).get().getIndex(indexName).getShards()) {
            assertThat(shardStats.getSeqNoStats().getMaxSeqNo(), equalTo(NO_OPS_PERFORMED));
            assertThat(shardStats.getSeqNoStats().getLocalCheckpoint(), equalTo(NO_OPS_PERFORMED));
            assertThat(shardStats.getSeqNoStats().getGlobalCheckpoint(), equalTo(NO_OPS_PERFORMED));
        }
    }

    public void testPeerRecoveryTrimsLocalTranslog() throws Exception {
        internalCluster().startNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 1).put("index.routing.allocation.include._name", String.join(",", dataNodes)).build());
        ensureGreen(indexName);
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        DiscoveryNode nodeWithOldPrimary = clusterState.nodes()
            .get(clusterState.routingTable().index(indexName).shard(0).primaryShard().currentNodeId());
        final var transportService = MockTransportService.getInstance(nodeWithOldPrimary.getName());
        CountDownLatch readyToRestartNode = new CountDownLatch(1);
        AtomicBoolean stopped = new AtomicBoolean();
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals("indices:data/write/bulk[s][r]") && randomInt(100) < 5) {
                throw new NodeClosedException(nodeWithOldPrimary);
            }
            // prevent the primary from marking the replica as stale so the replica can get promoted.
            if (action.equals("internal:cluster/shard/failure")) {
                stopped.set(true);
                readyToRestartNode.countDown();
                throw new NodeClosedException(nodeWithOldPrimary);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        Thread[] indexers = new Thread[randomIntBetween(1, 8)];
        for (int i = 0; i < indexers.length; i++) {
            indexers[i] = new Thread(() -> {
                while (stopped.get() == false) {
                    try {
                        DocWriteResponse response = prepareIndex(indexName).setSource(
                            Map.of("f" + randomIntBetween(1, 10), randomNonNegativeLong()),
                            XContentType.JSON
                        ).get();
                        assertThat(response.getResult(), is(oneOf(CREATED, UPDATED)));
                    } catch (ElasticsearchException ignored) {}
                }
            });
        }
        for (Thread indexer : indexers) {
            indexer.start();
        }
        readyToRestartNode.await();
        transportService.clearAllRules();
        internalCluster().restartNode(nodeWithOldPrimary.getName(), new InternalTestCluster.RestartCallback());
        for (Thread indexer : indexers) {
            indexer.join();
        }
        ensureGreen(indexName);
    }

    public void testCancelRecoveryWithAutoExpandReplicas() throws Exception {
        internalCluster().startMasterOnlyNode();
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all"))
                .setWaitForActiveShards(ActiveShardCount.NONE)
        );
        internalCluster().startNode();
        internalCluster().startNode();
        ClusterRerouteUtils.rerouteRetryFailed(client());
        assertAcked(indicesAdmin().prepareDelete("test")); // cancel recoveries
        assertBusy(() -> {
            for (PeerRecoverySourceService recoveryService : internalCluster().getDataNodeInstances(PeerRecoverySourceService.class)) {
                assertThat(recoveryService.numberOfOngoingRecoveries(), equalTo(0));
            }
        });
    }

    public void testReservesBytesDuringPeerRecoveryPhaseOne() throws Exception {
        internalCluster().startNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.include._name", String.join(",", dataNodes)).build());
        ensureGreen(indexName);
        final List<IndexRequestBuilder> indexRequests = IntStream.range(0, between(10, 500))
            .mapToObj(n -> prepareIndex(indexName).setSource("foo", "bar"))
            .toList();
        indexRandom(randomBoolean(), true, true, indexRequests);
        assertThat(indicesAdmin().prepareFlush(indexName).get().getFailedShards(), equalTo(0));

        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        DiscoveryNode nodeWithPrimary = clusterState.nodes()
            .get(clusterState.routingTable().index(indexName).shard(0).primaryShard().currentNodeId());

        final AtomicBoolean fileInfoIntercepted = new AtomicBoolean();
        final AtomicBoolean fileChunkIntercepted = new AtomicBoolean();
        MockTransportService.getInstance(nodeWithPrimary.getName()).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(PeerRecoveryTargetService.Actions.FILES_INFO)) {
                if (fileInfoIntercepted.compareAndSet(false, true)) {
                    final NodeIndicesStats nodeIndicesStats = clusterAdmin().prepareNodesStats(connection.getNode().getId())
                        .clear()
                        .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Store))
                        .get()
                        .getNodes()
                        .get(0)
                        .getIndices();
                    assertThat(nodeIndicesStats.getStore().reservedSizeInBytes(), equalTo(0L));
                    assertThat(
                        nodeIndicesStats.getShardStats(clusterState.metadata().getProject().index(indexName).getIndex())
                            .stream()
                            .flatMap(s -> Arrays.stream(s.getShards()))
                            .map(s -> s.getStats().getStore().reservedSizeInBytes())
                            .toList(),
                        everyItem(equalTo(StoreStats.UNKNOWN_RESERVED_BYTES))
                    );
                }
            } else if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                if (fileChunkIntercepted.compareAndSet(false, true)) {
                    assertThat(
                        clusterAdmin().prepareNodesStats(connection.getNode().getId())
                            .clear()
                            .setIndices(new CommonStatsFlags(CommonStatsFlags.Flag.Store))
                            .get()
                            .getNodes()
                            .get(0)
                            .getIndices()
                            .getStore()
                            .reservedSizeInBytes(),
                        greaterThan(0L)
                    );
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        setReplicaCount(1, indexName);
        ensureGreen();
        assertTrue(fileInfoIntercepted.get());
        assertTrue(fileChunkIntercepted.get());

        assertThat(
            clusterAdmin().prepareNodesStats()
                .get()
                .getNodes()
                .stream()
                .mapToLong(n -> n.getIndices().getStore().reservedSizeInBytes())
                .sum(),
            equalTo(0L)
        );
    }

    public void testWaitForClusterStateToBeAppliedOnSourceNode() throws Exception {
        internalCluster().startMasterOnlyNode();
        final var primaryNode = internalCluster().startDataOnlyNode();
        String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        final List<IndexRequestBuilder> indexRequests = IntStream.range(0, between(10, 500))
            .mapToObj(n -> prepareIndex(indexName).setSource("foo", "bar"))
            .toList();
        indexRandom(randomBoolean(), true, true, indexRequests);
        assertThat(indicesAdmin().prepareFlush(indexName).get().getFailedShards(), equalTo(0));

        final var replicaNode = internalCluster().startDataOnlyNode();

        final long initialClusterStateVersion = clusterService().state().version();

        try (var recoveryClusterStateDelayListeners = new RecoveryClusterStateDelayListeners(initialClusterStateVersion)) {
            final var primaryNodeTransportService = MockTransportService.getInstance(primaryNode);
            primaryNodeTransportService.addRequestHandlingBehavior(
                Coordinator.COMMIT_STATE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    assertThat(request, instanceOf(ApplyCommitRequest.class));
                    recoveryClusterStateDelayListeners.getClusterStateDelayListener(((ApplyCommitRequest) request).getVersion())
                        .addListener(
                            ActionListener.wrap(ignored -> handler.messageReceived(request, channel, task), e -> fail(e, "unexpected"))
                        );
                }
            );
            primaryNodeTransportService.addRequestHandlingBehavior(
                PeerRecoverySourceService.Actions.START_RECOVERY,
                (handler, request, channel, task) -> {
                    assertThat(request, instanceOf(StartRecoveryRequest.class));
                    assertThat(((StartRecoveryRequest) request).clusterStateVersion(), greaterThan(initialClusterStateVersion));
                    handler.messageReceived(
                        request,
                        new TestTransportChannel(
                            new ChannelActionListener<>(channel).delegateResponse(
                                (l, e) -> fail(e, "recovery should succeed on first attempt")
                            )
                        ),
                        task
                    );
                    recoveryClusterStateDelayListeners.onStartRecovery();
                }
            );
            recoveryClusterStateDelayListeners.addCleanup(primaryNodeTransportService::clearInboundRules);

            final var replicaClusterService = internalCluster().getInstance(ClusterService.class, replicaNode);
            final ClusterStateListener clusterStateListener = event -> {
                final var primaryProceedListener = recoveryClusterStateDelayListeners.getClusterStateDelayListener(event.state().version());
                final var indexRoutingTable = event.state().routingTable().index(indexName);
                assertNotNull(indexRoutingTable);
                final var indexShardRoutingTable = indexRoutingTable.shard(0);
                if (indexShardRoutingTable.size() == 2 && indexShardRoutingTable.getAllInitializingShards().isEmpty() == false) {
                    // this is the cluster state update which starts the recovery, so delay the primary node application until recovery
                    // has started
                    recoveryClusterStateDelayListeners.delayUntilRecoveryStart(primaryProceedListener);
                } else {
                    // this is some other cluster state update, so we must let it proceed now
                    primaryProceedListener.onResponse(null);
                }
            };
            replicaClusterService.addListener(clusterStateListener);
            recoveryClusterStateDelayListeners.addCleanup(() -> replicaClusterService.removeListener(clusterStateListener));

            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
            ensureGreen(indexName);
        }
    }

    public void testDeleteIndexDuringFinalization() throws Exception {
        internalCluster().startMasterOnlyNode();
        final var primaryNode = internalCluster().startDataOnlyNode();
        String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        final List<IndexRequestBuilder> indexRequests = IntStream.range(0, between(10, 500))
            .mapToObj(n -> prepareIndex(indexName).setSource("foo", "bar"))
            .toList();
        indexRandom(randomBoolean(), true, true, indexRequests);
        assertThat(indicesAdmin().prepareFlush(indexName).get().getFailedShards(), equalTo(0));

        final var replicaNode = internalCluster().startDataOnlyNode();

        final SubscribableListener<Void> recoveryCompleteListener = new SubscribableListener<>();
        final PlainActionFuture<AcknowledgedResponse> deleteListener = new PlainActionFuture<>();

        final var threadPool = internalCluster().clusterService().threadPool();

        final var indexId = internalCluster().clusterService().state().routingTable().index(indexName).getIndex();
        final var primaryIndexShard = internalCluster().getInstance(IndicesService.class, primaryNode)
            .indexServiceSafe(indexId)
            .getShard(0);
        final var globalCheckpointBeforeRecovery = primaryIndexShard.getLastSyncedGlobalCheckpoint();

        final var replicaNodeTransportService = asInstanceOf(
            MockTransportService.class,
            internalCluster().getInstance(TransportService.class, replicaNode)
        );
        replicaNodeTransportService.addRequestHandlingBehavior(
            PeerRecoveryTargetService.Actions.TRANSLOG_OPS,
            (handler, request, channel, task) -> handler.messageReceived(
                request,
                new TestTransportChannel(ActionTestUtils.assertNoFailureListener(response -> {
                    // Process the TRANSLOG_OPS response on the replica (avoiding failing it due to a concurrent delete) but
                    // before sending the response back send another document to the primary, advancing the GCP to prevent the replica
                    // being marked as in-sync (NB below we delay the replica write until after the index is deleted)
                    prepareIndex(indexName).setSource("foo", "baz").execute(ActionListener.noop());

                    primaryIndexShard.addGlobalCheckpointListener(
                        globalCheckpointBeforeRecovery + 1,
                        new GlobalCheckpointListeners.GlobalCheckpointListener() {
                            @Override
                            public Executor executor() {
                                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
                            }

                            @Override
                            public void accept(long globalCheckpoint, Exception e) {
                                assertNull(e);

                                // Now the GCP has advanced the replica won't be marked in-sync so respond to the TRANSLOG_OPS request
                                // to start recovery finalization
                                channel.sendResponse(response);

                                // Wait a short while for finalization to block on advancing the replica's GCP and then delete the index
                                threadPool.schedule(
                                    () -> client().admin().indices().prepareDelete(indexName).execute(deleteListener),
                                    TimeValue.timeValueMillis(100),
                                    EsExecutors.DIRECT_EXECUTOR_SERVICE
                                );
                            }
                        },
                        TimeValue.timeValueSeconds(10)
                    );
                })),
                task
            )
        );

        // delay the delivery of the replica write until the end of the test so the replica never becomes in-sync
        replicaNodeTransportService.addRequestHandlingBehavior(
            TransportBulkAction.NAME + "[s][r]",
            (handler, request, channel, task) -> recoveryCompleteListener.addListener(
                assertNoFailureListener(ignored -> handler.messageReceived(request, channel, task))
            )
        );

        // Create the replica to trigger the whole process
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );

        // Wait for the index to be deleted
        assertTrue(deleteListener.get(20, TimeUnit.SECONDS).isAcknowledged());

        final var peerRecoverySourceService = internalCluster().getInstance(PeerRecoverySourceService.class, primaryNode);
        assertBusy(() -> assertEquals(0, peerRecoverySourceService.numberOfOngoingRecoveries()));
        recoveryCompleteListener.onResponse(null);
    }

    public void testPostRecoveryMerge() throws Exception {
        internalCluster().startMasterOnlyNode();
        final var dataNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        final var indexSettingsBuilder = indexSettings(1, 0).put(INDEX_MERGE_ENABLED, false);
        if (randomBoolean()) {
            indexSettingsBuilder.put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.UPGRADE_TO_LUCENE_10_0_0, IndexVersion.current())
            );
        }
        createIndex(indexName, indexSettingsBuilder.build());

        final var initialSegmentCount = 20;
        for (int i = 0; i < initialSegmentCount; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName); // force a one-doc segment
        }
        flush(indexName); // commit all the one-doc segments

        final LongSupplier searchableSegmentCountSupplier = () -> indicesAdmin().prepareSegments(indexName)
            .get(SAFE_AWAIT_TIMEOUT)
            .getIndices()
            .get(indexName)
            .getShards()
            .get(0)
            .shards()[0].getSegments()
            .stream()
            .filter(Segment::isSearch)
            .count();

        assertEquals(initialSegmentCount, searchableSegmentCountSupplier.getAsLong());

        // force a recovery by restarting the node, re-enabling merges while the node is down, but configure the node not to be in the hot
        // or content tiers so that it does not do any post-recovery merge
        internalCluster().restartNode(dataNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                final var request = new UpdateSettingsRequest(Settings.builder().putNull(INDEX_MERGE_ENABLED).build(), indexName);
                request.reopen(true);
                safeGet(indicesAdmin().updateSettings(request));
                return Settings.builder()
                    .putList(NODE_ROLES_SETTING.getKey(), randomNonEmptySubsetOf(List.of("data_warm", "data_cold")))
                    .build();
            }
        });

        ensureGreen(indexName);
        final var mergeStats = indicesAdmin().prepareStats(indexName).clear().setMerge(true).get().getIndex(indexName).getShards()[0]
            .getStats()
            .getMerge();
        assertEquals(0, mergeStats.getCurrent());
        assertEquals(0, mergeStats.getTotal());
        assertEquals(initialSegmentCount, searchableSegmentCountSupplier.getAsLong());

        // force a recovery by restarting the node again, but this time putting it into the hot or content tiers to enable post-recovery
        // merges
        internalCluster().restartNode(dataNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder()
                    .putList(
                        NODE_ROLES_SETTING.getKey(),
                        Stream.concat(
                            Stream.of(randomFrom("data", "data_content", "data_hot")),
                            Stream.of("data", "data_content", "data_hot", "data_warm", "data_cold").filter(p -> randomBoolean())
                        ).distinct().toList()
                    )
                    // set the inactive time to zero so that we flush immediately after every merge, rather than having the test wait 5min
                    .put(SHARD_INACTIVE_TIME_SETTING.getKey(), TimeValue.ZERO)
                    .build();
            }
        });

        ensureGreen(indexName);
        assertBusy(() -> assertThat(searchableSegmentCountSupplier.getAsLong(), lessThan((long) initialSegmentCount)));
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false; // need to set index.version.created to test difference in behaviour on older indices
    }

    public void testPostRecoveryMergeDisabledOnOlderIndices() throws Exception {
        internalCluster().startMasterOnlyNode();
        final var dataNode = internalCluster().startDataOnlyNode();
        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(INDEX_MERGE_ENABLED, false)
                .put(
                    IndexMetadata.SETTING_VERSION_CREATED,
                    IndexVersionUtils.randomVersionBetween(
                        random(),
                        IndexVersionUtils.getLowestWriteCompatibleVersion(),
                        IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
                    )
                )
                .build()
        );

        final var initialSegmentCount = 20;
        for (int i = 0; i < initialSegmentCount; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName); // force a one-doc segment
        }
        flush(indexName); // commit all the one-doc segments

        final LongSupplier searchableSegmentCountSupplier = () -> indicesAdmin().prepareSegments(indexName)
            .get(SAFE_AWAIT_TIMEOUT)
            .getIndices()
            .get(indexName)
            .getShards()
            .get(0)
            .shards()[0].getSegments()
            .stream()
            .filter(Segment::isSearch)
            .count();

        assertEquals(initialSegmentCount, searchableSegmentCountSupplier.getAsLong());

        // force a recovery by restarting the node, re-enabling merges while the node is down
        internalCluster().restartNode(dataNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                final var request = new UpdateSettingsRequest(Settings.builder().putNull(INDEX_MERGE_ENABLED).build(), indexName);
                request.reopen(true);
                safeGet(indicesAdmin().updateSettings(request));
                return Settings.EMPTY;
            }
        });

        ensureGreen(indexName);
        final var mergeStats = indicesAdmin().prepareStats(indexName).clear().setMerge(true).get().getIndex(indexName).getShards()[0]
            .getStats()
            .getMerge();
        assertEquals(0, mergeStats.getCurrent());
        assertEquals(0, mergeStats.getTotal());
        assertEquals(initialSegmentCount, searchableSegmentCountSupplier.getAsLong());
    }

    private void assertGlobalCheckpointIsStableAndSyncedInAllNodes(String indexName, List<String> nodes, int shard) throws Exception {
        assertThat(nodes, is(not(empty())));

        ShardId shardId = new ShardId(resolveIndex(indexName), shard);
        IndexShard indexShard = internalCluster().getInstance(IndicesService.class, nodes.get(0)).getShardOrNull(shardId);
        assertThat(indexShard, is(notNullValue()));
        long maxSeqNo = indexShard.seqNoStats().getMaxSeqNo();

        for (String node : nodes) {
            IndexShard nodeIndexShard = internalCluster().getInstance(IndicesService.class, node).getShardOrNull(shardId);
            assertThat(nodeIndexShard, is(notNullValue()));

            assertThat(nodeIndexShard.seqNoStats().getMaxSeqNo(), is(equalTo(maxSeqNo)));
            assertBusy(() -> assertThat(nodeIndexShard.getLastSyncedGlobalCheckpoint(), equalTo(maxSeqNo)));
        }
    }

    private void createRepository(boolean enableSnapshotPeerRecoveries) {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPO_NAME)
                .setType("fs")
                .setSettings(
                    Settings.builder()
                        .put("location", randomRepoPath())
                        .put(BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.getKey(), enableSnapshotPeerRecoveries)
                        .put("compress", false)
                )
        );
    }

    private CreateSnapshotResponse createSnapshot(String indexName) {
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, REPO_NAME, SNAP_NAME)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertThat(
            clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, REPO_NAME).setSnapshots(SNAP_NAME).get().getSnapshots().get(0).state(),
            equalTo(SnapshotState.SUCCESS)
        );
        return createSnapshotResponse;
    }

    private long getLocalCheckpointOfSafeCommit(IndexCommit safeIndexCommit) throws IOException {
        final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
            safeIndexCommit.getUserData().entrySet()
        );
        final long commitLocalCheckpoint = commitInfo.localCheckpoint();
        final long maxSeqNo = commitInfo.maxSeqNo();
        final LocalCheckpointTracker localCheckpointTracker = new LocalCheckpointTracker(maxSeqNo, commitLocalCheckpoint);

        // In certain scenarios it is possible that the local checkpoint captured during commit lags behind,
        // meaning that it's possible that there are operations stored in the safe commit that have > seqNo
        // than the captured local checkpoint. When a shard is recovered locally, the local checkpoint can
        // be > than the safe commit local checkpoint, since that's checked and updated in
        // InternalEngine#restoreVersionMapAndCheckpointTracker
        try (DirectoryReader directoryReader = DirectoryReader.open(safeIndexCommit)) {
            final IndexSearcher searcher = newSearcher(directoryReader);
            searcher.setQueryCache(null);
            final Query query = new BooleanQuery.Builder().add(
                LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, commitLocalCheckpoint + 1, Long.MAX_VALUE),
                BooleanClause.Occur.MUST
            ).add(Queries.newNonNestedFilter(IndexVersion.current()), BooleanClause.Occur.MUST).build();
            final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            for (LeafReaderContext leaf : directoryReader.leaves()) {
                final Scorer scorer = weight.scorer(leaf);
                if (scorer == null) {
                    continue;
                }
                final NumericDocValues seqNoDocValue = leaf.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
                final DocIdSetIterator iterator = scorer.iterator();
                int docId;
                while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (seqNoDocValue.advanceExact(docId) == false) {
                        throw new IllegalStateException();
                    }
                    final long docSeqNo = seqNoDocValue.longValue();
                    assertThat(docSeqNo, is(greaterThan(commitLocalCheckpoint)));
                    localCheckpointTracker.markSeqNoAsProcessed(docSeqNo);
                    localCheckpointTracker.markSeqNoAsPersisted(docSeqNo);
                }
            }
            assertThat(localCheckpointTracker.getPersistedCheckpoint(), is(greaterThanOrEqualTo(commitLocalCheckpoint)));
            return localCheckpointTracker.getPersistedCheckpoint();
        }
    }
}
