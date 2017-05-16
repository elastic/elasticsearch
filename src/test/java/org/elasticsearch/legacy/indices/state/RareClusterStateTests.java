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

package org.elasticsearch.legacy.indices.state;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.legacy.cluster.ClusterInfo;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.DiskUsage;
import org.elasticsearch.legacy.cluster.node.DiscoveryNodes;
import org.elasticsearch.legacy.cluster.routing.RoutingNodes;
import org.elasticsearch.legacy.cluster.routing.RoutingTable;
import org.elasticsearch.legacy.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.legacy.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.legacy.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.legacy.common.settings.ImmutableSettings;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.gateway.local.LocalGatewayAllocator;
import org.elasticsearch.legacy.test.ElasticsearchIntegrationTest;
import org.junit.Test;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, transportClientRatio = 0)
public class RareClusterStateTests extends ElasticsearchIntegrationTest {

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("gateway.type", "local")
                .build();
    }

    @Test
    public void testUnassignedShardAndEmptyNodesInRoutingTable() throws Exception {
        createIndex("a");
        ensureSearchable("a");
        ClusterState current = clusterService().state();
        LocalGatewayAllocator allocator = internalCluster().getInstance(LocalGatewayAllocator.class);

        AllocationDeciders allocationDeciders = new AllocationDeciders(ImmutableSettings.EMPTY, new AllocationDecider[0]);
        RoutingNodes routingNodes = new RoutingNodes(
                ClusterState.builder(current)
                        .routingTable(RoutingTable.builder(current.routingTable()).remove("a").addAsRecovery(current.metaData().index("a")))
                        .nodes(DiscoveryNodes.EMPTY_NODES)
                        .build()
        );
        ClusterInfo clusterInfo = new ClusterInfo(ImmutableMap.<String, DiskUsage>of(), ImmutableMap.<String, Long>of());

        RoutingAllocation routingAllocation =  new RoutingAllocation(allocationDeciders, routingNodes, current.nodes(), clusterInfo);
        allocator.allocateUnassigned(routingAllocation);
    }

}
