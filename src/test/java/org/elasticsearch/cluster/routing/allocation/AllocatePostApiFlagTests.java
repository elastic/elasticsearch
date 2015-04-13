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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Test;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class AllocatePostApiFlagTests extends ElasticsearchAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(AllocatePostApiFlagTests.class);

    @Test
    public void simpleFlagTests() {
        AllocationService allocation = createAllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).build());

        logger.info("creating an index with 1 shard, no replica");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        assertThat(clusterState.routingTable().index("test").shard(0).primaryAllocatedPostApi(), equalTo(false));

        logger.info("adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingTable().index("test").shard(0).primaryAllocatedPostApi(), equalTo(false));

        logger.info("start primary shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingTable().index("test").shard(0).primaryAllocatedPostApi(), equalTo(true));
    }
}
