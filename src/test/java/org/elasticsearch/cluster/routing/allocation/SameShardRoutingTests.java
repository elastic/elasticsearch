/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.allocation.RoutingNodesUtils.numberOfShardsOfType;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class SameShardRoutingTests extends ElasticsearchAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(SameShardRoutingTests.class);

    @Test
    @TestLogging("cluster.routing.allocation:TRACE")
    public void sameHost() {
        AllocationService strategy = createAllocationService(settingsBuilder().put(SameShardAllocationDecider.SAME_HOST_SETTING, true).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").numberOfShards(2).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding two nodes with the same host");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(new DiscoveryNode("node1", "node1", "test1", "test1", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of(), Version.CURRENT))
                .put(new DiscoveryNode("node2", "node2", "test1", "test1", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of(), Version.CURRENT))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(numberOfShardsOfType(clusterState.readOnlyRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(2));

        logger.info("--> start all primary shards, no replica will be started since its on the same host");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.readOnlyRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(numberOfShardsOfType(clusterState.readOnlyRoutingNodes(), ShardRoutingState.STARTED), equalTo(2));
        assertThat(numberOfShardsOfType(clusterState.readOnlyRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(0));

        logger.info("--> add another node, with a different host, replicas will be allocating");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(new DiscoveryNode("node3", "node3", "test2", "test2", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of(), Version.CURRENT))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(numberOfShardsOfType(clusterState.readOnlyRoutingNodes(), ShardRoutingState.STARTED), equalTo(2));
        assertThat(numberOfShardsOfType(clusterState.readOnlyRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(2));
        for (MutableShardRouting shardRouting : clusterState.readOnlyRoutingNodes().shardsWithState(INITIALIZING)) {
            assertThat(shardRouting.currentNodeId(), equalTo("node3"));
        }
    }
}
