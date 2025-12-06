/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.allocation;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class FilteringAllocationIT extends ESIntegTestCase {

    public void testDecommissionNodeNoReplicas() {
        logger.info("--> starting 2 nodes");
        List<String> nodesIds = internalCluster().startNodes(2);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        assertThat(cluster().size(), equalTo(2));

        logger.info("--> creating an index with no replicas");
        createIndex("test", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen("test");
        logger.info("--> index some data");
        for (int i = 0; i < 100; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        indicesAdmin().prepareRefresh().get();
        assertHitCount(prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()), 100);

        final boolean closed = randomBoolean();
        if (closed) {
            assertAcked(indicesAdmin().prepareClose("test"));
            ensureGreen("test");
        }

        logger.info("--> decommission the second node");
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", node_1));
        ensureGreen("test");

        logger.info("--> verify all are allocated on node1 now");
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                final IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardId);
                for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                    assertThat(clusterState.nodes().get(indexShardRoutingTable.shard(copy).currentNodeId()).getName(), equalTo(node_0));
                }
            }
        }

        if (closed) {
            assertAcked(indicesAdmin().prepareOpen("test"));
        }

        indicesAdmin().prepareRefresh().get();
        assertHitCount(prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()), 100);
    }

    public void testAutoExpandReplicasToFilteredNodes() {
        logger.info("--> starting 2 nodes");
        List<String> nodesIds = internalCluster().startNodes(2);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        assertThat(cluster().size(), equalTo(2));

        logger.info("--> creating an index with auto-expand replicas");
        createIndex("test", Settings.builder().put(AutoExpandReplicas.SETTING.getKey(), "0-all").build());
        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        assertThat(clusterState.metadata().getProject().index("test").getNumberOfReplicas(), equalTo(1));
        ensureGreen("test");

        logger.info("--> filter out the second node");
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", node_1));
        } else {
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node_1), "test");
        }
        ensureGreen("test");

        logger.info("--> verify all are allocated on node1 now");
        clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        assertThat(clusterState.metadata().getProject().index("test").getNumberOfReplicas(), equalTo(0));
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                final IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardId);
                for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                    assertThat(clusterState.nodes().get(indexShardRoutingTable.shard(copy).currentNodeId()).getName(), equalTo(node_0));
                }
            }
        }
    }

    public void testDisablingAllocationFiltering() {
        logger.info("--> starting 2 nodes");
        List<String> nodesIds = internalCluster().startNodes(2);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        assertThat(cluster().size(), equalTo(2));

        logger.info("--> creating an index with no replicas");
        createIndex("test", 2, 0);
        ensureGreen("test");

        logger.info("--> index some data");
        for (int i = 0; i < 100; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        indicesAdmin().prepareRefresh().get();
        assertHitCount(prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()), 100);

        final boolean closed = randomBoolean();
        if (closed) {
            assertAcked(indicesAdmin().prepareClose("test"));
            ensureGreen("test");
        }

        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index("test");
        int numShardsOnNode1 = 0;
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            final IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                if ("node1".equals(clusterState.nodes().get(indexShardRoutingTable.shard(copy).currentNodeId()).getName())) {
                    numShardsOnNode1++;
                }
            }
        }

        if (numShardsOnNode1 > ThrottlingAllocationDecider.DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES) {
            updateClusterSettings(Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", numShardsOnNode1));
            // make sure we can recover all the nodes at once otherwise we might run into a state where
            // one of the shards has not yet started relocating but we already fired up the request to wait for 0 relocating shards.
        }
        logger.info("--> remove index from the first node");
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", node_0), "test");
        ClusterRerouteUtils.reroute(client());
        ensureGreen("test");

        logger.info("--> verify all shards are allocated on node_1 now");
        clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        indexRoutingTable = clusterState.routingTable().index("test");
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            final IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                assertThat(clusterState.nodes().get(indexShardRoutingTable.shard(copy).currentNodeId()).getName(), equalTo(node_1));
            }
        }

        logger.info("--> disable allocation filtering ");
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", ""), "test");
        ClusterRerouteUtils.reroute(client());
        ensureGreen("test");

        logger.info("--> verify that there are shards allocated on both nodes now");
        clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        assertThat(clusterState.routingTable().index("test").numberOfNodesShardsAreAllocatedOn(), equalTo(2));
    }

    public void testInvalidIPFilterClusterSettings() {
        logger.info("--> starting 2 nodes");
        internalCluster().startNodes(2);
        String ipKey = randomFrom("_ip", "_host_ip", "_publish_ip");
        var filterSetting = randomFrom(
            FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING,
            FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING,
            FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Settings.builder().put(filterSetting.getKey() + ipKey, "192.168.1.1."))
        );
        assertEquals("invalid IP address [192.168.1.1.] for [" + filterSetting.getKey() + ipKey + "]", e.getMessage());
    }

    public void testTransientSettingsStillApplied() {
        List<String> nodes = internalCluster().startNodes(6);
        Set<String> excludeNodes = new HashSet<>(nodes.subList(0, 3));
        Set<String> includeNodes = new HashSet<>(nodes.subList(3, 6));
        logger.info(
            "--> exclude: [{}], include: [{}]",
            Strings.collectionToCommaDelimitedString(excludeNodes),
            Strings.collectionToCommaDelimitedString(includeNodes)
        );
        ensureStableCluster(6);
        indicesAdmin().prepareCreate("test").get();
        ensureGreen("test");

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareClose("test"));
        }

        Settings exclude = Settings.builder()
            .put("cluster.routing.allocation.exclude._name", Strings.collectionToCommaDelimitedString(excludeNodes))
            .build();

        logger.info("--> updating settings");
        clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setTransientSettings(exclude).get();

        logger.info("--> waiting for relocation");
        waitForRelocation(ClusterHealthStatus.GREEN);

        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();

        for (ShardRouting shard : RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED)) {
            String node = state.getRoutingNodes().node(shard.currentNodeId()).node().getName();
            logger.info("--> shard on {} - {}", node, shard);
            assertTrue(
                "shard on "
                    + node
                    + " but should only be on the include node list: "
                    + Strings.collectionToCommaDelimitedString(includeNodes),
                includeNodes.contains(node)
            );
        }

        Settings other = Settings.builder().put("cluster.info.update.interval", "45s").build();

        logger.info("--> updating settings with random persistent setting");
        clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(other)
            .setTransientSettings(exclude)
            .get();

        logger.info("--> waiting for relocation");
        waitForRelocation(ClusterHealthStatus.GREEN);

        state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();

        // The transient settings still exist in the state
        assertThat(state.metadata().transientSettings(), equalTo(exclude));

        for (ShardRouting shard : RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED)) {
            String node = state.getRoutingNodes().node(shard.currentNodeId()).node().getName();
            logger.info("--> shard on {} - {}", node, shard);
            assertTrue(
                "shard on "
                    + node
                    + " but should only be on the include node list: "
                    + Strings.collectionToCommaDelimitedString(includeNodes),
                includeNodes.contains(node)
            );
        }
    }
}
