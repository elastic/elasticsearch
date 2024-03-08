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

package co.elastic.elasticsearch.stateless.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class StatelessAllocationDeciderTests extends ESAllocationTestCase {

    public void testAllocateAccordingToNodeRoles() {

        var indexName = UUIDs.randomBase64UUID();
        var indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        var state = createClusterState(1, 1, indexMetadata);

        var service = createAllocationService(new StatelessAllocationDecider());
        state = applyStartedShardsUntilNoChange(state, service);

        assertThat(state.getRoutingNodes().hasUnassignedShards(), equalTo(false));
        assertThat(findShard(state, indexName, 0, ShardRouting.Role.INDEX_ONLY).currentNodeId(), equalTo("index-node-0"));
        assertThat(findShard(state, indexName, 0, ShardRouting.Role.SEARCH_ONLY).currentNodeId(), equalTo("search-node-0"));
    }

    public void testAllocationExplain() {

        var indexName = UUIDs.randomBase64UUID();
        var indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();

        var state = createClusterState(1, 0, indexMetadata);

        var service = createAllocationService(new StatelessAllocationDecider());
        var allocation = createRoutingAllocation(state, service);
        allocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        assertContainsDecision(
            service.explainShardAllocation(findShard(state, indexName, 0, ShardRouting.Role.INDEX_ONLY), allocation),
            Decision.Type.YES,
            "shard role matches stateless node role"
        );
        assertContainsDecision(
            service.explainShardAllocation(findShard(state, indexName, 0, ShardRouting.Role.SEARCH_ONLY), allocation),
            Decision.Type.NO,
            "shard role [SEARCH_ONLY] does not match stateless node role [index]"
        );
    }

    private static void assertContainsDecision(ShardAllocationDecision explain, Decision.Type type, String explanation) {
        assertTrue(
            explain.getAllocateDecision()
                .getNodeDecisions()
                .stream()
                .flatMap(node -> node.getCanAllocateDecision().getDecisions().stream())
                .anyMatch(decision -> decision.type() == type && Objects.equals(decision.getExplanation(), explanation))
        );
    }

    private static ClusterState createClusterState(int indexNodes, int searchNodes, IndexMetadata indexMetadata) {
        var discoveryNodesBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < indexNodes; i++) {
            discoveryNodesBuilder.add(newNode("index-node-" + i, Set.of(DiscoveryNodeRole.INDEX_ROLE)));
        }
        for (int i = 0; i < searchNodes; i++) {
            discoveryNodesBuilder.add(newNode("search-node-" + i, Set.of(DiscoveryNodeRole.SEARCH_ROLE)));
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder(new StatelessShardRoutingRoleStrategy()).addAsNew(indexMetadata))
            .nodes(discoveryNodesBuilder)
            .build();
    }

    private static ShardRouting findShard(ClusterState state, String indexName, int shardId, ShardRouting.Role role) {
        var indexShardRouting = state.routingTable().index(indexName).shard(shardId);
        for (int i = 0; i < indexShardRouting.size(); i++) {
            if (indexShardRouting.shard(i).role() == role) {
                return indexShardRouting.shard(i);
            }
        }
        throw new AssertionError("Shard [" + indexName + "][" + shardId + "] with role [" + role + "] is not found");
    }

    private static AllocationService createAllocationService(AllocationDecider... deciders) {
        return new AllocationService(
            new AllocationDeciders(Sets.addToCopy(DEFAULT_DECIDERS, deciders)),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            new StatelessShardRoutingRoleStrategy()
        );
    }

    private static RoutingAllocation createRoutingAllocation(ClusterState state, AllocationService service) {
        return new RoutingAllocation(
            service.getAllocationDeciders(),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private static final Set<AllocationDecider> DEFAULT_DECIDERS = Set.of(
        new ReplicaAfterPrimaryActiveAllocationDecider(),
        createSameShardAllocationDecider(Settings.EMPTY)
    );

    private static SameShardAllocationDecider createSameShardAllocationDecider(Settings settings) {
        return new SameShardAllocationDecider(new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }
}
