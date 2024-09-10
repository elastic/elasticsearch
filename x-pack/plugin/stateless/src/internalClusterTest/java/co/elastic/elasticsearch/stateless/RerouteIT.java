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

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class RerouteIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testRerouteRecoveryOfIndexShard() throws Exception {
        startMasterOnlyNode();
        final String nodeA = startIndexNode();
        logger.info("--> started index node A [{}]", nodeA);

        logger.info("--> create index on node: {}", nodeA);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        indexDocs(indexName, between(1000, 1500));
        refresh(indexName);

        final String nodeB = startIndexNode();
        ensureGreen();
        logger.info("--> started index node B [{}]", nodeB);

        logger.info("--> blocking recoveries on " + nodeB);
        ObjectStoreService objectStoreService = getObjectStoreService(nodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setBlockOnAnyFiles();

        logger.info("--> move shard from: {} to: {}", nodeA, nodeB);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, nodeA, nodeB));

        logger.info("--> waiting for recovery to start both on source and target");
        final Index index = resolveIndex(indexName);
        assertBusy(() -> {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeA);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsSource(), equalTo(1));
            indicesService = internalCluster().getInstance(IndicesService.class, nodeB);
            assertThat(indicesService.indexServiceSafe(index).getShard(0).recoveryStats().currentAsTarget(), equalTo(1));
        });

        logger.info("--> request recoveries");
        RecoveryResponse response = indicesAdmin().prepareRecoveries(indexName).execute().actionGet();

        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(indexName);
        List<RecoveryState> nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        List<RecoveryState> nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(1));

        assertRecoveryState(
            nodeARecoveryStates.get(0),
            0,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            true,
            RecoveryState.Stage.DONE,
            null,
            nodeA
        );
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertOnGoingRecoveryState(nodeBRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, true, nodeA, nodeB);
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

        logger.info("--> unblocking recoveries on " + nodeB);
        repository.unblock();

        // wait for it to be finished
        ensureGreen();

        response = indicesAdmin().prepareRecoveries(indexName).execute().actionGet();

        recoveryStates = response.shardRecoveryStates().get(indexName);
        assertThat(recoveryStates.size(), equalTo(1));

        assertRecoveryState(
            recoveryStates.get(0),
            0,
            RecoverySource.PeerRecoverySource.INSTANCE,
            true,
            RecoveryState.Stage.DONE,
            nodeA,
            nodeB
        );
        validateIndexRecoveryState(recoveryStates.get(0).getIndex());
        assertBusy(() -> assertNodeHasNoCurrentRecoveries(nodeA));
        assertBusy(() -> assertNodeHasNoCurrentRecoveries(nodeB));
    }

    public void testRerouteRecoveryOfSearchShard() throws Exception {
        startMasterOnlyNode();
        final String nodeA = startIndexNode();
        logger.info("--> started index node A [{}]", nodeA);

        logger.info("--> create index on node: {}", nodeA);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        indexDocs(indexName, between(1000, 1500));
        refresh(indexName);

        String nodeB = startSearchNode();
        ensureGreen();
        logger.info("--> started search node B [{}]", nodeB);
        logger.info("--> bump replica count");
        setReplicaCount(1, indexName);
        ensureGreen();

        String nodeC = startSearchNode();
        ensureGreen();
        logger.info("--> started search node C [{}]", nodeC);

        assertBusy(() -> assertNodeHasNoCurrentRecoveries(nodeA));
        assertBusy(() -> assertNodeHasNoCurrentRecoveries(nodeB));
        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForNodes("4").get().isTimedOut()); // including master node

        ObjectStoreService objectStoreService = getObjectStoreService(nodeC);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        logger.info("--> block recoveries on " + nodeC);
        repository.setBlockOnAnyFiles();

        logger.info("--> move replica shard from: {} to: {}", nodeB, nodeC);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, nodeB, nodeC));

        RecoveryResponse response = indicesAdmin().prepareRecoveries(indexName).execute().actionGet();
        List<RecoveryState> recoveryStates = response.shardRecoveryStates().get(indexName);
        List<RecoveryState> nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        List<RecoveryState> nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        List<RecoveryState> nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);

        assertThat(nodeARecoveryStates.size(), equalTo(1));
        assertThat(nodeBRecoveryStates.size(), equalTo(1));
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(
            nodeARecoveryStates.get(0),
            0,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            true,
            RecoveryState.Stage.DONE,
            null,
            nodeA
        );
        validateIndexRecoveryState(nodeARecoveryStates.get(0).getIndex());

        assertRecoveryState(
            nodeBRecoveryStates.get(0),
            0,
            RecoverySource.PeerRecoverySource.INSTANCE,
            false,
            RecoveryState.Stage.DONE,
            nodeA,
            nodeB
        );
        validateIndexRecoveryState(nodeBRecoveryStates.get(0).getIndex());

        assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, nodeA, nodeC);
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());

        if (randomBoolean()) {
            // shutdown nodeB and check if recovery continues
            internalCluster().stopNode(nodeB);
            ensureStableCluster(3);

            response = indicesAdmin().prepareRecoveries(indexName).execute().actionGet();
            recoveryStates = response.shardRecoveryStates().get(indexName);

            nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
            assertThat(nodeARecoveryStates.size(), equalTo(1));
            nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
            assertThat(nodeBRecoveryStates.size(), equalTo(0));
            nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
            assertThat(nodeCRecoveryStates.size(), equalTo(1));

            assertOnGoingRecoveryState(nodeCRecoveryStates.get(0), 0, RecoverySource.PeerRecoverySource.INSTANCE, false, nodeA, nodeC);
            validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());
        }

        logger.info("--> unblocking recoveries on " + nodeC);
        repository.unblock();
        ensureGreen();

        response = indicesAdmin().prepareRecoveries(indexName).execute().actionGet();
        recoveryStates = response.shardRecoveryStates().get(indexName);

        nodeARecoveryStates = findRecoveriesForTargetNode(nodeA, recoveryStates);
        assertThat(nodeARecoveryStates.size(), equalTo(1));
        nodeBRecoveryStates = findRecoveriesForTargetNode(nodeB, recoveryStates);
        assertThat(nodeBRecoveryStates.size(), equalTo(0));
        nodeCRecoveryStates = findRecoveriesForTargetNode(nodeC, recoveryStates);
        assertThat(nodeCRecoveryStates.size(), equalTo(1));

        assertRecoveryState(
            nodeCRecoveryStates.get(0),
            0,
            RecoverySource.PeerRecoverySource.INSTANCE,
            false,
            RecoveryState.Stage.DONE,
            nodeA,
            nodeC
        );
        validateIndexRecoveryState(nodeCRecoveryStates.get(0).getIndex());
    }

    private void assertRecoveryState(
        RecoveryState state,
        int shardId,
        RecoverySource type,
        boolean primary,
        RecoveryState.Stage stage,
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
        assertThat(state.getStage(), not(equalTo(RecoveryState.Stage.DONE)));
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

    private void validateIndexRecoveryState(RecoveryState.Index indexState) {
        assertThat(indexState.time(), greaterThanOrEqualTo(0L));
        assertThat(indexState.recoveredFilesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredFilesPercent(), lessThanOrEqualTo(100.0f));
        assertThat(indexState.recoveredBytesPercent(), greaterThanOrEqualTo(0.0f));
        assertThat(indexState.recoveredBytesPercent(), lessThanOrEqualTo(100.0f));
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
}
