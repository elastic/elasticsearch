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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;

import static java.util.Collections.emptyMap;

public class NonDataNodeShardRoutingTests extends ESAllocationTestCase {
    private final Logger logger = Loggers.getLogger(NonDataNodeShardRoutingTests.class);

    public void testMoveShardToNonDataNode() {
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metaData.index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData)
            .routingTable(routingTable).build();

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(
            DiscoveryNodes.builder()
                .add(new DiscoveryNode("node1", "node1", "node1", "test1", "test1", buildNewFakeTransportAddress(), emptyMap(),
                    MASTER_DATA_ROLES, Version.CURRENT))
                .add(new DiscoveryNode("node2", "node2", "node2", "test2", "test2", buildNewFakeTransportAddress(), emptyMap(),
                    Collections.unmodifiableSet(EnumSet.of(DiscoveryNode.Role.MASTER)), Version.CURRENT))).build();

        Index index = clusterState.getMetaData().index("test").getIndex();
        MoveAllocationCommand command = new MoveAllocationCommand(index.getName(), 0, "node1", "node2");
        RoutingAllocation routingAllocation = new RoutingAllocation(new AllocationDeciders(Settings.EMPTY, Collections.emptyList()),
            new RoutingNodes(clusterState, false), clusterState, ClusterInfo.EMPTY, System.nanoTime());
        logger.info("--> executing move allocation command to non-data node");
        expectThrows(IllegalArgumentException.class, () -> command.execute(routingAllocation, false));
    }

    public void testMoveShardFromNonDataNode() {
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metaData.index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData)
            .routingTable(routingTable).build();

        logger.info("--> adding two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(
            DiscoveryNodes.builder()
                .add(new DiscoveryNode("node1", "node1", "node1", "test1", "test1", buildNewFakeTransportAddress(), emptyMap(),
                    MASTER_DATA_ROLES, Version.CURRENT))
                .add(new DiscoveryNode("node2", "node2", "node2", "test2", "test2", buildNewFakeTransportAddress(), emptyMap(),
                    Collections.unmodifiableSet(EnumSet.of(DiscoveryNode.Role.MASTER)), Version.CURRENT))).build();

        Index index = clusterState.getMetaData().index("test").getIndex();
        MoveAllocationCommand command = new MoveAllocationCommand(index.getName(), 0, "node2", "node1");
        RoutingAllocation routingAllocation = new RoutingAllocation(new AllocationDeciders(Settings.EMPTY, Collections.emptyList()),
            new RoutingNodes(clusterState, false), clusterState, ClusterInfo.EMPTY, System.nanoTime());
        logger.info("--> executing move allocation command from non-data node");
        expectThrows(IllegalArgumentException.class, () -> command.execute(routingAllocation, false));
    }
}
