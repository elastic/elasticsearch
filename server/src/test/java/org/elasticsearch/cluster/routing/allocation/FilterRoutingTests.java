/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class FilterRoutingTests extends ESAllocationTestCase {

    public void testClusterIncludeFiltersSingleAttribute() {
        testClusterFilters(
            Settings.builder().put(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag1", "value1,value2"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "value1")))
                .add(newNode("node2", attrMap("tag1", "value2")))
                .add(newNode("node3", attrMap("tag1", "value3")))
                .add(newNode("node4", attrMap("tag1", "value4")))
        );
    }

    public void testClusterIncludeFiltersMultipleAttributes() {
        testClusterFilters(
            Settings.builder()
                .put(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag1", "value1")
                .put(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag2", "value2"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "value1")))
                .add(newNode("node2", attrMap("tag2", "value2")))
                .add(newNode("node3", attrMap("tag1", "value3")))
                .add(newNode("node4", attrMap("tag2", "value4")))
        );
    }

    public void testClusterIncludeFiltersOptionalAttribute() {
        testClusterFilters(
            Settings.builder().put(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag1", "value1,value2"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "value1")))
                .add(newNode("node2", attrMap("tag1", "value2")))
                .add(newNode("node3", attrMap()))
                .add(newNode("node4", attrMap()))
        );
    }

    public void testClusterIncludeFiltersWildcards() {
        testClusterFilters(
            Settings.builder()
                .put(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag1", "*incl*")
                .put(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag2", "*incl*"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "do_include_this")))
                .add(newNode("node2", attrMap("tag2", "also_include_this")))
                .add(newNode("node3", attrMap("tag1", "exclude_this")))
                .add(newNode("node4", attrMap("tag2", "also_exclude_this")))
        );
    }

    public void testClusterExcludeFiltersSingleAttribute() {
        testClusterFilters(
            Settings.builder().put(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag1", "value3,value4"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "value1")))
                .add(newNode("node2", attrMap("tag1", "value2")))
                .add(newNode("node3", attrMap("tag1", "value3")))
                .add(newNode("node4", attrMap("tag1", "value4")))
        );
    }

    public void testClusterExcludeFiltersMultipleAttributes() {
        testClusterFilters(
            Settings.builder()
                .put(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag1", "value3")
                .put(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag2", "value4"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "value1")))
                .add(newNode("node2", attrMap("tag2", "value2")))
                .add(newNode("node3", attrMap("tag1", "value3")))
                .add(newNode("node4", attrMap("tag2", "value4")))
        );
    }

    public void testClusterExcludeFiltersOptionalAttribute() {
        testClusterFilters(
            Settings.builder().put(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag1", "value3,value4"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap()))
                .add(newNode("node2", attrMap()))
                .add(newNode("node3", attrMap("tag1", "value3")))
                .add(newNode("node4", attrMap("tag1", "value4")))
        );
    }

    public void testClusterExcludeFiltersWildcards() {
        testClusterFilters(
            Settings.builder()
                .put(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag1", "*excl*")
                .put(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag2", "*excl*"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "do_include_this")))
                .add(newNode("node2", attrMap("tag2", "also_include_this")))
                .add(newNode("node3", attrMap("tag1", "exclude_this")))
                .add(newNode("node4", attrMap("tag2", "also_exclude_this")))
        );
    }

    public void testClusterIncludeAndExcludeFilters() {
        testClusterFilters(
            Settings.builder()
                .put(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag1", "*incl*")
                .put(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag2", "*excl*"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "do_include_this")))
                .add(newNode("node2", attrMap("tag1", "also_include_this", "tag2", "ok_by_tag2")))
                .add(newNode("node3", attrMap("tag1", "included_by_tag1", "tag2", "excluded_by_tag2")))
                .add(newNode("node4", attrMap("tag1", "excluded_by_tag1", "tag2", "included_by_tag2")))
        );
    }

    public void testClusterRequireFilters() {
        testClusterFilters(
            Settings.builder()
                .put(CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "tag1", "req1")
                .put(CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "tag2", "req2"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "req1", "tag2", "req2")))
                .add(newNode("node2", attrMap("tag1", "req1", "tag2", "req2")))
                .add(newNode("node3", attrMap("tag1", "req1")))
                .add(newNode("node4", attrMap("tag1", "other", "tag2", "req2")))
        );
    }

    private static Map<String, String> attrMap(String... keysValues) {
        if (keysValues.length == 0) {
            return emptyMap();
        }
        if (keysValues.length == 2) {
            return singletonMap(keysValues[0], keysValues[1]);
        }
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < keysValues.length; i += 2) {
            result.put(keysValues[i], keysValues[i + 1]);
        }
        return result;
    }

    /**
     * A test that creates a 2p1r index and which expects the given allocation service's settings only to allocate the shards of this index
     * to `node1` and `node2`.
     */
    private void testClusterFilters(Settings.Builder allocationServiceSettings, DiscoveryNodes.Builder nodes) {
        final AllocationService strategy = createAllocationService(allocationServiceSettings.build());

        logger.info("Building initial routing table");

        final Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(2).numberOfReplicas(1))
            .build();

        final RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .nodes(nodes)
            .build();

        logger.info("--> rerouting");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(2));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> make sure shards are only allocated on tag1 with value1 and value2");
        final List<ShardRouting> startedShards = shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED);
        assertThat(startedShards.size(), equalTo(4));
        for (ShardRouting startedShard : startedShards) {
            assertThat(startedShard.currentNodeId(), Matchers.anyOf(equalTo("node1"), equalTo("node2")));
        }
    }

    public void testIndexIncludeFilters() {
        testIndexFilters(
            Settings.builder().put(INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag1", "value1,value2"),
            Settings.builder().put(INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag1", "value1,value4"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "value1")))
                .add(newNode("node2", attrMap("tag1", "value2")))
                .add(newNode("node3", attrMap("tag1", "value3")))
                .add(newNode("node4", attrMap("tag1", "value4")))
                .add(newNode("node5", attrMap()))
        );
    }

    public void testIndexExcludeFilters() {
        testIndexFilters(
            Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag1", "value3,value4"),
            Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag1", "value2,value3"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap()))
                .add(newNode("node2", attrMap("tag1", "value2")))
                .add(newNode("node3", attrMap("tag1", "value3")))
                .add(newNode("node4", attrMap("tag1", "value4")))
        );
    }

    public void testIndexIncludeThenExcludeFilters() {
        testIndexFilters(
            Settings.builder().put(INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag1", "value1,value2"),
            Settings.builder()
                .put(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag1", "value2,value3")
                .putNull(INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag1"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "value1")))
                .add(newNode("node2", attrMap("tag1", "value2")))
                .add(newNode("node3", attrMap("tag1", "value3")))
                .add(newNode("node4", attrMap()))
        );
    }

    public void testIndexExcludeThenIncludeFilters() {
        testIndexFilters(
            Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag1", "value3,value4"),
            Settings.builder()
                .put(INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag1", "value1,value4")
                .putNull(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag1"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "value1")))
                .add(newNode("node2", attrMap()))
                .add(newNode("node3", attrMap("tag1", "value3")))
                .add(newNode("node4", attrMap("tag1", "value4")))
        );
    }

    public void testIndexRequireFilters() {
        testIndexFilters(
            Settings.builder()
                .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "tag1", "value1")
                .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "tag2", "value2"),
            Settings.builder()
                .putNull(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "tag2")
                .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "tag3", "value3"),
            DiscoveryNodes.builder()
                .add(newNode("node1", attrMap("tag1", "value1", "tag2", "value2", "tag3", "value3")))
                .add(newNode("node2", attrMap("tag1", "value1", "tag2", "value2", "tag3", "other")))
                .add(newNode("node3", attrMap("tag1", "other", "tag2", "value2", "tag3", "other")))
                .add(newNode("node4", attrMap("tag1", "value1", "tag2", "other", "tag3", "value3")))
                .add(newNode("node5", attrMap("tag2", "value2", "tag3", "value3")))
                .add(newNode("node6", attrMap()))
        );
    }

    /**
     * A test that creates a 2p1r index and expects the given index allocation settings only to allocate the shards to `node1` and `node2`;
     * on updating the index allocation settings the shards should be relocated to nodes `node1` and `node4`.
     */
    private void testIndexFilters(Settings.Builder initialIndexSettings, Settings.Builder updatedIndexSettings, Builder nodesBuilder) {
        AllocationService strategy = createAllocationService(Settings.builder().build());

        logger.info("Building initial routing table");

        final Metadata initialMetadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(indexSettings(IndexVersion.current(), 2, 1).put(initialIndexSettings.build())))
            .build();

        final RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(initialMetadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(initialMetadata)
            .routingTable(initialRoutingTable)
            .nodes(nodesBuilder)
            .build();

        logger.info("--> rerouting");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(2));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> make sure shards are only allocated on tag1 with value1 and value2");
        List<ShardRouting> startedShards = shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED);
        assertThat(startedShards.size(), equalTo(4));
        for (ShardRouting startedShard : startedShards) {
            assertThat(startedShard.currentNodeId(), Matchers.anyOf(equalTo("node1"), equalTo("node2")));
        }

        logger.info("--> switch between value2 and value4, shards should be relocating");

        final IndexMetadata existingMetadata = clusterState.metadata().getProject().index("test");
        final Metadata updatedMetadata = Metadata.builder()
            .put(
                IndexMetadata.builder(existingMetadata)
                    .settings(Settings.builder().put(existingMetadata.getSettings()).put(updatedIndexSettings.build()).build())
            )
            .build();

        clusterState = ClusterState.builder(clusterState).metadata(updatedMetadata).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(2));

        logger.info("--> finish relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        startedShards = shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED);
        assertThat(startedShards.size(), equalTo(4));
        for (ShardRouting startedShard : startedShards) {
            assertThat(startedShard.currentNodeId(), Matchers.anyOf(equalTo("node1"), equalTo("node4")));
        }
    }

    public void testConcurrentRecoveriesAfterShardsCannotRemainOnNode() {
        AllocationService strategy = createAllocationService(Settings.builder().build());

        logger.info("Building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(IndexVersion.current())).numberOfShards(2).numberOfReplicas(0))
            .put(IndexMetadata.builder("test2").settings(settings(IndexVersion.current())).numberOfShards(2).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test1"))
            .addAsNew(metadata.getProject().index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes and performing rerouting");
        DiscoveryNode node1 = newNode("node1", singletonMap("tag1", "value1"));
        DiscoveryNode node2 = newNode("node2", singletonMap("tag1", "value2"));
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(node1).add(node2)).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(clusterState.getRoutingNodes().node(node1.getId()).numberOfShardsWithState(INITIALIZING), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node(node2.getId()).numberOfShardsWithState(INITIALIZING), equalTo(2));

        logger.info("--> start the shards (only primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> make sure all shards are started");
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(4));

        logger.info("--> disable allocation for node1 and reroute");
        strategy = createAllocationService(
            Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), "1")
                .put(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "tag1", "value1")
                .build()
        );

        logger.info("--> move shards from node1 to node2");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        logger.info("--> check that concurrent recoveries only allows 1 shard to move");
        assertThat(clusterState.getRoutingNodes().node(node1.getId()).numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node(node2.getId()).numberOfShardsWithState(INITIALIZING), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node(node2.getId()).numberOfShardsWithState(STARTED), equalTo(2));

        logger.info("--> start the shards (only primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> move second shard from node1 to node2");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(clusterState.getRoutingNodes().node(node2.getId()).numberOfShardsWithState(INITIALIZING), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node(node2.getId()).numberOfShardsWithState(STARTED), equalTo(3));

        logger.info("--> start the shards (only primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(clusterState.getRoutingNodes().node(node2.getId()).numberOfShardsWithState(STARTED), equalTo(4));
    }
}
