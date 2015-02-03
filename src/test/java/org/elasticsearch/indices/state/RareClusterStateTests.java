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

package org.elasticsearch.indices.state;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.gateway.local.LocalGatewayAllocator;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0)
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
        internalCluster().startNode();
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

        RoutingAllocation routingAllocation = new RoutingAllocation(allocationDeciders, routingNodes, current.nodes(), clusterInfo);
        allocator.allocateUnassigned(routingAllocation);
    }


    @Test
    @TestLogging(value = "cluster.service:TRACE")
    public void testDeleteCreateInOneBulk() throws Exception {
        internalCluster().startNodesAsync(2, ImmutableSettings.builder()
                .put(DiscoveryModule.DISCOVERY_TYPE_KEY, "zen")
                .build()).get();
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut());
        prepareCreate("test").setSettings(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, true).addMapping("type").get();
        ensureGreen("test");

        // now that the cluster is stable, remove publishing timeout
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(ImmutableSettings.builder().put(DiscoverySettings.PUBLISH_TIMEOUT, "0")));

        Set<String> nodes = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
        nodes.remove(internalCluster().getMasterName());

        // block none master node.
        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(nodes.iterator().next(), getRandom());
        internalCluster().setDisruptionScheme(disruption);
        logger.info("--> indexing a doc");
        index("test", "type", "1");
        refresh();
        disruption.startDisrupting();
        logger.info("--> delete index and recreate it");
        assertFalse(client().admin().indices().prepareDelete("test").setTimeout("200ms").get().isAcknowledged());
        assertFalse(prepareCreate("test").setTimeout("200ms").setSettings(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, true).get().isAcknowledged());
        logger.info("--> letting cluster proceed");
        disruption.stopDisrupting();
        ensureGreen(TimeValue.timeValueMinutes(30), "test");
        assertHitCount(client().prepareSearch("test").get(), 0);
    }


}
