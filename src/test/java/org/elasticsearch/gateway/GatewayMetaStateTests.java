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

package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Test;

import java.util.Iterator;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 * Test IndexMetaState for master and data only nodes return correct list of indices to write
 * There are many parameters:
 * - meta state is on disk with old version
 * - meta state is on disk with new version
 * - meta state is not on disk
 * - meta state is in memory with old version
 * - meta state is in memory with new version
 * - version changed in cluster state event
 * - no change
 * - node is data only node
 * - node is master eligible
 * for data only nodes: shard initializing on shard
 */
public class GatewayMetaStateTests extends ElasticsearchAllocationTestCase {

    ClusterChangedEvent generateEvent(boolean initializing, boolean versionChanged) {
        //ridiculous settings to make sure we don't run into uninitialized because fo default
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 100)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", 100)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 100)
                .build());
        ClusterState newClusterState, previousClusterState;
        MetaData metaDataOldClusterState = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(2))
                .build();

        RoutingTable routingTableOldClusterState = RoutingTable.builder()
                .addAsNew(metaDataOldClusterState.index("test"))
                .build();

        // assign all shards to node1
        ClusterState init = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
                .metaData(metaDataOldClusterState)
                .routingTable(routingTableOldClusterState)
                .nodes(DiscoveryNodes.builder().put(newNode("node1")).localNodeId("node1"))
                .build();
        // new cluster state will have initializing shards on node 1
        RoutingTable routingTableNewClusterState = strategy.reroute(init).routingTable();
        if (initializing == false) {
            // pretend all initialized, nothing happened
            ClusterState temp = ClusterState.builder(init).routingTable(routingTableNewClusterState).metaData(metaDataOldClusterState).build();
            routingTableNewClusterState = strategy.applyStartedShards(temp, temp.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
            routingTableOldClusterState = routingTableNewClusterState;

        } else {
            // nothing to do, we have one routing table with unassigned and one with initializing
        }

        // create new meta data either with version changed or not
        MetaData metaDataNewClusterState = MetaData.builder()
                .put(init.metaData().index("test"), versionChanged)
                .build();

        // create the cluster states with meta data and routing tables as computed before
        previousClusterState = ClusterState.builder(init)
                .metaData(metaDataOldClusterState)
                .routingTable(routingTableOldClusterState)
                .nodes(DiscoveryNodes.builder().put(newNode("node1")).localNodeId("node1"))
                .build();
        newClusterState = ClusterState.builder(previousClusterState).routingTable(routingTableNewClusterState).metaData(metaDataNewClusterState).build();

        return new ClusterChangedEvent("test", newClusterState, previousClusterState);
    }

    public void assertState(boolean initializing,
                            boolean versionChanged,
                            boolean stateOnDisk,
                            boolean stateOnDiskOutdated,
                            boolean stateInMemory,
                            boolean stateInMemoryOutdated,
                            boolean masterEligible,
                            boolean expectMetaData) throws Exception {
        ClusterChangedEvent event = generateEvent(initializing, versionChanged);
        final NodeEnvironment env = newNodeEnvironment();
        MetaStateService metaStateService = new MetaStateService(ImmutableSettings.EMPTY, env);
        IndexMetaState indexMetaState = new IndexMetaState(ImmutableSettings.EMPTY, env, metaStateService);

        MetaData inMemoryMetaData = null;
        if (stateInMemory) {
            if (stateInMemoryOutdated) {
                inMemoryMetaData = event.previousState().metaData();
            } else {
                inMemoryMetaData = event.state().metaData();
            }
        }
        if (stateOnDisk) {
            if (stateOnDiskOutdated) {
                metaStateService.writeIndex("test", event.previousState().metaData().index("test"), null);
            } else {
                metaStateService.writeIndex("test", event.state().metaData().index("test"), null);
            }
        }
        Iterator<GatewayMetaState.IndexMetaWriteInfo> indices;
        if (masterEligible) {
            indices = indexMetaState.filterStatesOnMaster(event, inMemoryMetaData).iterator();
        } else {
            indices = indexMetaState.filterStateOnDataNode(event, inMemoryMetaData).iterator();
        }
        if (expectMetaData) {
            assertThat(indices.hasNext(), equalTo(true));
            assertThat(indices.next().getNewMetaData().index(), equalTo("test"));
            assertThat(indices.hasNext(), equalTo(false));
        } else {
            assertThat(indices.hasNext(), equalTo(false));
        }
    }

    @Test
    public void testAllCombinations() throws Exception {
        assertState(/*initializing*/true, /*versionChanged*/true,
        /*stateOnDisk*/true, /*stateOnDiskOutdated*/true,
        /*stateInMemory*/true, /*stateInMemoryOutdated*/true,
        /*masterEligible*/true,/*expectMetaData*/true);

        assertState(/*initializing*/true, /*versionChanged*/true,
        /*stateOnDisk*/true, /*stateOnDiskOutdated*/true,
        /*stateInMemory*/true, /*stateInMemoryOutdated*/true,
        /*masterEligible*/false,/*expectMetaData*/true);

        assertState(/*initializing*/false, /*versionChanged*/false,
        /*stateOnDisk*/true, /*stateOnDiskOutdated*/true,
        /*stateInMemory*/true, /*stateInMemoryOutdated*/true,
        /*masterEligible*/false,/*expectMetaData*/false);

        assertState(/*initializing*/false, /*versionChanged*/false,
        /*stateOnDisk*/true, /*stateOnDiskOutdated*/true,
        /*stateInMemory*/false, /*stateInMemoryOutdated*/true,
        /*masterEligible*/false,/*expectMetaData*/false);

        assertState(/*initializing*/true, /*versionChanged*/false,
        /*stateOnDisk*/true, /*stateOnDiskOutdated*/false,
        /*stateInMemory*/false, /*stateInMemoryOutdated*/true,
        /*masterEligible*/false,/*expectMetaData*/true);
    }
}
