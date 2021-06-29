/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AutoExpandReplicas;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope= Scope.TEST, numDataNodes =0)
public class FilteringAllocationIT extends ESIntegTestCase {

    public void testDecommissionNodeNoReplicas() {
        logger.info("--> starting 2 nodes");
        List<String> nodesIds = internalCluster().startNodes(2);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        assertThat(cluster().size(), equalTo(2));

        logger.info("--> creating an index with no replicas");
        createIndex("test", Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());
        ensureGreen("test");
        logger.info("--> index some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet()
            .getHits().getTotalHits().value, equalTo(100L));

        final boolean closed = randomBoolean();
        if (closed) {
            assertAcked(client().admin().indices().prepareClose("test"));
            ensureGreen("test");
        }

        logger.info("--> decommission the second node");
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", node_1))
                .execute().actionGet();
        ensureGreen("test");

        logger.info("--> verify all are allocated on node1 now");
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    assertThat(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(node_0));
                }
            }
        }

        if (closed) {
            assertAcked(client().admin().indices().prepareOpen("test"));
        }

        client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery())
            .execute().actionGet().getHits().getTotalHits().value, equalTo(100L));
    }

    public void testAutoExpandReplicasToFilteredNodes() {
        logger.info("--> starting 2 nodes");
        List<String> nodesIds = internalCluster().startNodes(2);
        final String node_0 = nodesIds.get(0);
        final String node_1 = nodesIds.get(1);
        assertThat(cluster().size(), equalTo(2));

        logger.info("--> creating an index with auto-expand replicas");
        createIndex("test", Settings.builder()
            .put(AutoExpandReplicas.SETTING.getKey(), "0-all")
            .build());
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(clusterState.metadata().index("test").getNumberOfReplicas(), equalTo(1));
        ensureGreen("test");

        logger.info("--> filter out the second node");
        if (randomBoolean()) {
            client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", node_1))
                .execute().actionGet();
        } else {
            client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", node_1))
                .execute().actionGet();
        }
        ensureGreen("test");

        logger.info("--> verify all are allocated on node1 now");
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(clusterState.metadata().index("test").getNumberOfReplicas(), equalTo(0));
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    assertThat(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(node_0));
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
        createIndex("test", Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());
        ensureGreen("test");

        logger.info("--> index some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery())
            .execute().actionGet().getHits().getTotalHits().value, equalTo(100L));

        final boolean closed = randomBoolean();
        if (closed) {
            assertAcked(client().admin().indices().prepareClose("test"));
            ensureGreen("test");
        }

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index("test");
        int numShardsOnNode1 = 0;
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                if ("node1".equals(clusterState.nodes().get(shardRouting.currentNodeId()).getName())) {
                    numShardsOnNode1++;
                }
            }
        }

        if (numShardsOnNode1 > ThrottlingAllocationDecider.DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES) {
            client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", numShardsOnNode1)).execute().actionGet();
            // make sure we can recover all the nodes at once otherwise we might run into a state where
            // one of the shards has not yet started relocating but we already fired up the request to wait for 0 relocating shards.
        }
        logger.info("--> remove index from the first node");
        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", node_0))
                .execute().actionGet();
        client().admin().cluster().prepareReroute().get();
        ensureGreen("test");

        logger.info("--> verify all shards are allocated on node_1 now");
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        indexRoutingTable = clusterState.routingTable().index("test");
        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                assertThat(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), equalTo(node_1));
            }
        }

        logger.info("--> disable allocation filtering ");
        client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", ""))
                .execute().actionGet();
        client().admin().cluster().prepareReroute().get();
        ensureGreen("test");

        logger.info("--> verify that there are shards allocated on both nodes now");
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(clusterState.routingTable().index("test").numberOfNodesShardsAreAllocatedOn(), equalTo(2));
    }

    public void testInvalidIPFilterClusterSettings() {
        String ipKey = randomFrom("_ip", "_host_ip", "_publish_ip");
        Setting<String> filterSetting = randomFrom(FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING,
            FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING, FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(filterSetting.getKey() + ipKey, "192.168.1.1."))
            .execute().actionGet());
        assertEquals("invalid IP address [192.168.1.1.] for [" + filterSetting.getKey() + ipKey + "]", e.getMessage());
    }

    public void testTransientSettingsStillApplied() {
        List<String> nodes = internalCluster().startNodes(6);
        Set<String> excludeNodes = new HashSet<>(nodes.subList(0, 3));
        Set<String> includeNodes = new HashSet<>(nodes.subList(3, 6));
        logger.info("--> exclude: [{}], include: [{}]",
            Strings.collectionToCommaDelimitedString(excludeNodes),
            Strings.collectionToCommaDelimitedString(includeNodes));
        ensureStableCluster(6);
        client().admin().indices().prepareCreate("test").get();
        ensureGreen("test");

        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test"));
        }

        Settings exclude = Settings.builder().put("cluster.routing.allocation.exclude._name",
            Strings.collectionToCommaDelimitedString(excludeNodes)).build();

        logger.info("--> updating settings");
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(exclude).get();

        logger.info("--> waiting for relocation");
        waitForRelocation(ClusterHealthStatus.GREEN);

        ClusterState state = client().admin().cluster().prepareState().get().getState();

        for (ShardRouting shard : state.getRoutingTable().shardsWithState(ShardRoutingState.STARTED)) {
            String node = state.getRoutingNodes().node(shard.currentNodeId()).node().getName();
            logger.info("--> shard on {} - {}", node, shard);
            assertTrue("shard on " + node + " but should only be on the include node list: " +
                    Strings.collectionToCommaDelimitedString(includeNodes),
                includeNodes.contains(node));
        }

        Settings other = Settings.builder().put("cluster.info.update.interval", "45s").build();

        logger.info("--> updating settings with random persistent setting");
        client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(other).setTransientSettings(exclude).get();

        logger.info("--> waiting for relocation");
        waitForRelocation(ClusterHealthStatus.GREEN);

        state = client().admin().cluster().prepareState().get().getState();

        // The transient settings still exist in the state
        assertThat(state.metadata().transientSettings(), equalTo(exclude));

        for (ShardRouting shard : state.getRoutingTable().shardsWithState(ShardRoutingState.STARTED)) {
            String node = state.getRoutingNodes().node(shard.currentNodeId()).node().getName();
            logger.info("--> shard on {} - {}", node, shard);
            assertTrue("shard on " + node + " but should only be on the include node list: " +
                    Strings.collectionToCommaDelimitedString(includeNodes),
                includeNodes.contains(node));
        }
    }
}

