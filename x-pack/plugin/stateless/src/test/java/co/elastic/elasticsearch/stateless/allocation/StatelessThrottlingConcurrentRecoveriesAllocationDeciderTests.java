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
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static co.elastic.elasticsearch.stateless.allocation.StatelessThrottlingConcurrentRecoveriesAllocationDecider.CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB;
import static co.elastic.elasticsearch.stateless.allocation.StatelessThrottlingConcurrentRecoveriesAllocationDecider.MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class StatelessThrottlingConcurrentRecoveriesAllocationDeciderTests extends ESAllocationTestCase {

    private static final String NODE_0 = "node-0";
    private static final String NODE_1 = "node-1";
    private static final String INDEX_NAME = "test";

    // *** min_heap_required_for_concurrent_primary_recoveries ***

    public void testShouldNotThrottleSingleRelocationOnMinHeap() {
        // nodeHeap is smaller than required min heap for concurrent relocation
        ByteSizeValue requiredMinHeap = ByteSizeValue.ofMb(1028);
        double concurrentRecoveriesPerHeapGb = Integer.MAX_VALUE;
        ByteSizeValue nodeHeap = ByteSizeValue.ofMb(512);
        int initialRelocatingShards = 0;
        Decision.Type expected = Decision.Type.YES;

        doTestCanAllocate(requiredMinHeap, concurrentRecoveriesPerHeapGb, nodeHeap, initialRelocatingShards, expected);
    }

    public void testThrottleWhenHeapIsBelowMin() {
        // Required min heap for concurrent relocation is larger than node heap
        ByteSizeValue requiredMinHeap = ByteSizeValue.ofMb(1028);
        double concurrentRecoveriesPerHeapGb = Integer.MAX_VALUE;
        ByteSizeValue nodeHeap = ByteSizeValue.ofMb(512);
        int initialRelocatingShards = 1;
        Decision.Type expected = Decision.Type.THROTTLE;

        doTestCanAllocate(requiredMinHeap, concurrentRecoveriesPerHeapGb, nodeHeap, initialRelocatingShards, expected);
    }

    public void testNoThrottleWhenHeapIsAboveMin() {
        // Required min heap for concurrent relocation is smaller than node heap
        ByteSizeValue requiredMinHeap = ByteSizeValue.ofMb(256);
        double concurrentRecoveriesPerHeapGb = Integer.MAX_VALUE;
        ByteSizeValue nodeHeap = ByteSizeValue.ofMb(512);
        int initialRelocatingShards = 1;
        Decision.Type expected = Decision.Type.YES;

        doTestCanAllocate(requiredMinHeap, concurrentRecoveriesPerHeapGb, nodeHeap, initialRelocatingShards, expected);
    }

    public void testReasonMessageWhenThrottleMinRequiredHeap() {
        // Required min heap for concurrent relocation is larger than node heap
        ByteSizeValue requiredMinHeap = ByteSizeValue.ofMb(1028);
        double concurrentRecoveriesPerHeapGb = Integer.MAX_VALUE;
        ByteSizeValue nodeHeap = ByteSizeValue.ofMb(512);
        int initialRelocatingShards = 1;
        Decision.Type expected = Decision.Type.THROTTLE;

        var reason = doTestCanAllocate(requiredMinHeap, concurrentRecoveriesPerHeapGb, nodeHeap, initialRelocatingShards, expected);
        assertThat(
            reason,
            equalTo(
                "node is not large enough (node max heap size: 512mb) "
                    + "to do concurrent recoveries [incoming shard recoveries: 1], cluster setting "
                    + "[cluster.routing.allocation.min_heap_required_for_concurrent_primary_recoveries=1gb]"
            )
        );
    }

    // *** concurrent_primary_recoveries_per_heap_gb ***

    public void testShouldNotThrottleSingleRelocationOnPerGbHeap() {
        // Low value for concurrentRecoveriesPerHeapGb should not prevent single relocation
        ByteSizeValue requiredMinHeap = ByteSizeValue.ofMb(0);
        double concurrentRecoveriesPerHeapGb = 0;
        ByteSizeValue nodeHeap = ByteSizeValue.ofMb(512);
        int initialRelocatingShards = 0;
        Decision.Type expected = Decision.Type.YES;

        doTestCanAllocate(requiredMinHeap, concurrentRecoveriesPerHeapGb, nodeHeap, initialRelocatingShards, expected);
    }

    public void testNoThrottleWhenNumberOfRelocatingShardsIsWithinLimit() {
        // Number of concurrent relocations is within the limit for given heap size
        ByteSizeValue requiredMinHeap = ByteSizeValue.ofMb(0);
        double concurrentRecoveriesPerHeapGb = 4; // Allow 2 concurrentRecoveries for nodes with 0.5GB heap
        ByteSizeValue nodeHeap = ByteSizeValue.ofMb(512);
        int initialRelocatingShards = 1;
        Decision.Type expected = Decision.Type.YES;

        doTestCanAllocate(requiredMinHeap, concurrentRecoveriesPerHeapGb, nodeHeap, initialRelocatingShards, expected);
    }

    public void testThrottleWhenNumberOfRelocatingShardsIsAboveLimit() {
        // Number of concurrent relocations is above the limit for given heap size
        ByteSizeValue requiredMinHeap = ByteSizeValue.ofMb(0);
        double concurrentRecoveriesPerHeapGb = 4; // Allow 2 concurrentRecoveries for nodes with 0.5GB heap
        ByteSizeValue nodeHeap = ByteSizeValue.ofMb(512);
        int initialRelocatingShards = 2;
        Decision.Type expected = Decision.Type.THROTTLE;

        doTestCanAllocate(requiredMinHeap, concurrentRecoveriesPerHeapGb, nodeHeap, initialRelocatingShards, expected);
    }

    public void testNoThrottleWhenNumberOfRelocatingShardsIsWithinLimitLargeHeap() {
        // Number of concurrent relocations is within the limit for given heap size
        ByteSizeValue requiredMinHeap = ByteSizeValue.ofMb(0);
        double concurrentRecoveriesPerHeapGb = 1;
        int heapInGb = 64;
        ByteSizeValue nodeHeap = ByteSizeValue.ofGb(heapInGb);
        int initialRelocatingShards = heapInGb - 1;
        Decision.Type expected = Decision.Type.YES;

        doTestCanAllocate(requiredMinHeap, concurrentRecoveriesPerHeapGb, nodeHeap, initialRelocatingShards, expected);
    }

    public void testThrottleWhenNumberOfRelocatingShardsIsAboveLimitLargeHeap() {
        // Number of concurrent relocations is above the limit for given heap size
        ByteSizeValue requiredMinHeap = ByteSizeValue.ofMb(0);
        double concurrentRecoveriesPerHeapGb = 1;
        int heapInGb = 64;
        ByteSizeValue nodeHeap = ByteSizeValue.ofGb(heapInGb);
        Decision.Type expected = Decision.Type.THROTTLE;

        doTestCanAllocate(requiredMinHeap, concurrentRecoveriesPerHeapGb, nodeHeap, heapInGb, expected);
    }

    public void testReasonMessageWhenThrottleOnAllowedConcurrentRecoveriesPerHeapGB() {
        // Number of concurrent relocations is above the limit for given heap size
        ByteSizeValue requiredMinHeap = ByteSizeValue.ofMb(0);
        double concurrentRecoveriesPerHeapGb = 4;
        ByteSizeValue nodeHeap = ByteSizeValue.ofMb(512);
        int initialRelocatingShards = 2;
        Decision.Type expected = Decision.Type.THROTTLE;

        var reason = doTestCanAllocate(requiredMinHeap, concurrentRecoveriesPerHeapGb, nodeHeap, initialRelocatingShards, expected);
        assertThat(
            reason,
            equalTo(
                "Node is not allowed to do more concurrent recoveries. "
                    + "Incoming shard recoveries: [2]. Allowed recoveries: [2], "
                    + "based on node max heap size [512mb] and setting "
                    + "[cluster.routing.allocation.concurrent_primary_recoveries_per_heap_gb=4.000000]. "
                    + "Need larger node or update setting."
            )
        );
    }

    private static ShardRouting startedShard(int shardId) {
        return TestShardRouting.newShardRouting(INDEX_NAME, shardId, NODE_0, null, true, ShardRoutingState.STARTED);
    }

    private static ShardRouting relocatingShard(int shardId) {
        return TestShardRouting.newShardRouting(INDEX_NAME, shardId, NODE_0, NODE_1, true, ShardRoutingState.RELOCATING);
    }

    /**
     * Setup cluster state with two nodes (NODE_0 and NODE_1) with given heap size and provided initial shards.
     * Test canAllocate one additional relocation with given settings and verify expected result
     */
    private static String doTestCanAllocate(
        ByteSizeValue requiredMinHeapSetting,
        double concurrentRecoveriesPerHeapGbSetting,
        ByteSizeValue nodeHeap,
        int numberOfRelocatingShards,
        Decision.Type expected
    ) {
        Settings settings = Settings.builder()
            .put(MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey(), requiredMinHeapSetting.toString())
            .put(CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB.getKey(), concurrentRecoveriesPerHeapGbSetting)
            .build();

        var heapSizes = new HashMap<String, ByteSizeValue>();
        heapSizes.put(NODE_0, nodeHeap);
        heapSizes.put(NODE_1, nodeHeap);
        ClusterInfo clusterInfo = ClusterInfo.builder().maxHeapSizePerNode(heapSizes).build();

        HashSet<Setting<?>> settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING);
        settingsSet.add(CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB);
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        StatelessThrottlingConcurrentRecoveriesAllocationDecider decider = new StatelessThrottlingConcurrentRecoveriesAllocationDecider(
            clusterSettings
        );

        DiscoveryNode discoveryNode0 = newNode(NODE_0, Set.of(DiscoveryNodeRole.INDEX_ROLE));
        DiscoveryNode discoveryNode1 = newNode(NODE_1, Set.of(DiscoveryNodeRole.INDEX_ROLE));
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(discoveryNode0).add(discoveryNode1).build();

        List<ShardRouting> shardRoutings = new ArrayList<>();
        int i = 0;
        for (; i < numberOfRelocatingShards; i++) {
            shardRoutings.add(relocatingShard(i));
        }
        // Add one more shard that we will test relocation for
        shardRoutings.add(startedShard(i));

        var indexMetadata = IndexMetadata.builder(INDEX_NAME)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(shardRoutings.size())
            .numberOfReplicas(0)
            .build();
        ProjectMetadata.Builder projectMetadataBuilder = ProjectMetadata.builder(ProjectId.DEFAULT).put(indexMetadata, false);
        Metadata metadata = Metadata.builder().put(projectMetadataBuilder).build();

        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
        shardRoutings.forEach(indexRoutingTableBuilder::addShard);
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .add(indexRoutingTableBuilder.build())
            .build();
        GlobalRoutingTable globalRoutingTable = GlobalRoutingTable.builder().put(ProjectId.DEFAULT, routingTable).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(globalRoutingTable)
            .nodes(discoveryNodes)
            .build();

        RoutingAllocation routingAllocation = new RoutingAllocation(
            null,
            RoutingNodes.immutable(clusterState.globalRoutingTable(), clusterState.nodes()),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        routingAllocation.debugDecision(true);

        // When one additional routing to node-1
        RoutingNode node1 = clusterState.getRoutingNodes().node(NODE_1);
        ShardRouting shardRouting = relocatingShard(shardRoutings.size() - 1);
        Decision decision = decider.canAllocate(shardRouting, node1, routingAllocation);

        // Then
        assertThat(decision.type(), equalTo(expected));
        return decision.getExplanation();
    }
}
