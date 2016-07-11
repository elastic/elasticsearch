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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESAllocationTestCase;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test IndexMetaState for master and data only nodes return correct list of indices to write
 * There are many parameters:
 * - meta state is not in memory
 * - meta state is in memory with old version/ new version
 * - meta state is in memory with new version
 * - version changed in cluster state event/ no change
 * - node is data only node
 * - node is master eligible
 * for data only nodes: shard initializing on shard
 */
public class GatewayMetaStateTests extends ESAllocationTestCase {

    ClusterChangedEvent generateEvent(boolean initializing, boolean versionChanged, boolean masterEligible) {
        //ridiculous settings to make sure we don't run into uninitialized because fo default
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 100)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
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

        // assign all shards
        ClusterState init = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaDataOldClusterState)
                .routingTable(routingTableOldClusterState)
                .nodes(generateDiscoveryNodes(masterEligible))
                .build();
        // new cluster state will have initializing shards on node 1
        RoutingTable routingTableNewClusterState = strategy.reroute(init, "reroute").routingTable();
        if (initializing == false) {
            // pretend all initialized, nothing happened
            ClusterState temp = ClusterState.builder(init).routingTable(routingTableNewClusterState)
                    .metaData(metaDataOldClusterState).build();
            routingTableNewClusterState = strategy.applyStartedShards(temp, temp.getRoutingNodes().shardsWithState(INITIALIZING))
                    .routingTable();
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
                .nodes(generateDiscoveryNodes(masterEligible))
                .build();
        newClusterState = ClusterState.builder(previousClusterState).routingTable(routingTableNewClusterState)
                .metaData(metaDataNewClusterState).version(previousClusterState.getVersion() + 1).build();

        ClusterChangedEvent event = new ClusterChangedEvent("test", newClusterState, previousClusterState);
        assertThat(event.state().version(), equalTo(event.previousState().version() + 1));
        return event;
    }

    ClusterChangedEvent generateCloseEvent(boolean masterEligible) {
        //ridiculous settings to make sure we don't run into uninitialized because fo default
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 100)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", 100)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 100)
                .build());
        ClusterState newClusterState, previousClusterState;
        MetaData metaDataIndexCreated = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(2))
                .build();

        RoutingTable routingTableIndexCreated = RoutingTable.builder()
                .addAsNew(metaDataIndexCreated.index("test"))
                .build();

        // assign all shards
        ClusterState init = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaDataIndexCreated)
                .routingTable(routingTableIndexCreated)
                .nodes(generateDiscoveryNodes(masterEligible))
                .build();
        RoutingTable routingTableInitializing = strategy.reroute(init, "reroute").routingTable();
        ClusterState temp = ClusterState.builder(init).routingTable(routingTableInitializing).build();
        RoutingTable routingTableStarted = strategy.applyStartedShards(temp, temp.getRoutingNodes().shardsWithState(INITIALIZING))
                .routingTable();

        // create new meta data either with version changed or not
        MetaData metaDataStarted = MetaData.builder()
                .put(init.metaData().index("test"), true)
                .build();

        // create the cluster states with meta data and routing tables as computed before
        MetaData metaDataClosed = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).state(IndexMetaData.State.CLOSE)
                        .numberOfShards(5).numberOfReplicas(2)).version(metaDataStarted.version() + 1)
                .build();
        previousClusterState = ClusterState.builder(init)
                .metaData(metaDataStarted)
                .routingTable(routingTableStarted)
                .nodes(generateDiscoveryNodes(masterEligible))
                .build();
        newClusterState = ClusterState.builder(previousClusterState)
                .routingTable(routingTableIndexCreated)
                .metaData(metaDataClosed)
                .version(previousClusterState.getVersion() + 1).build();

        ClusterChangedEvent event = new ClusterChangedEvent("test", newClusterState, previousClusterState);
        assertThat(event.state().version(), equalTo(event.previousState().version() + 1));
        return event;
    }

    private DiscoveryNodes.Builder generateDiscoveryNodes(boolean masterEligible) {
        Set<DiscoveryNode.Role> dataOnlyRoles = Collections.singleton(DiscoveryNode.Role.DATA);
        return DiscoveryNodes.builder().put(newNode("node1", masterEligible ? MASTER_DATA_ROLES : dataOnlyRoles))
                .put(newNode("master_node", MASTER_DATA_ROLES)).localNodeId("node1").masterNodeId(masterEligible ? "node1" : "master_node");
    }

    public void assertState(ClusterChangedEvent event,
                            boolean stateInMemory,
                            boolean expectMetaData) throws Exception {
        MetaData inMemoryMetaData = null;
        Set<Index> oldIndicesList = emptySet();
        if (stateInMemory) {
            inMemoryMetaData = event.previousState().metaData();
            oldIndicesList = GatewayMetaState.getRelevantIndices(event.previousState(), event.previousState(), oldIndicesList);
        }
        Set<Index> newIndicesList = GatewayMetaState.getRelevantIndices(event.state(),event.previousState(), oldIndicesList);
        // third, get the actual write info
        Iterator<GatewayMetaState.IndexMetaWriteInfo> indices = GatewayMetaState.resolveStatesToBeWritten(oldIndicesList, newIndicesList,
                inMemoryMetaData, event.state().metaData()).iterator();

        if (expectMetaData) {
            assertThat(indices.hasNext(), equalTo(true));
            assertThat(indices.next().getNewMetaData().getIndex().getName(), equalTo("test"));
            assertThat(indices.hasNext(), equalTo(false));
        } else {
            assertThat(indices.hasNext(), equalTo(false));
        }
    }

    public void testVersionChangeIsAlwaysWritten() throws Exception {
        // test that version changes are always written
        boolean initializing = randomBoolean();
        boolean versionChanged = true;
        boolean stateInMemory = randomBoolean();
        boolean masterEligible = randomBoolean();
        boolean expectMetaData = true;
        ClusterChangedEvent event = generateEvent(initializing, versionChanged, masterEligible);
        assertState(event, stateInMemory, expectMetaData);
    }

    public void testNewShardsAlwaysWritten() throws Exception {
        // make sure new shards on data only node always written
        boolean initializing = true;
        boolean versionChanged = randomBoolean();
        boolean stateInMemory = randomBoolean();
        boolean masterEligible = false;
        boolean expectMetaData = true;
        ClusterChangedEvent event = generateEvent(initializing, versionChanged, masterEligible);
        assertState(event, stateInMemory, expectMetaData);
    }

    public void testAllUpToDateNothingWritten() throws Exception {
        // make sure state is not written again if we wrote already
        boolean initializing = false;
        boolean versionChanged = false;
        boolean stateInMemory = true;
        boolean masterEligible = randomBoolean();
        boolean expectMetaData = false;
        ClusterChangedEvent event = generateEvent(initializing, versionChanged, masterEligible);
        assertState(event, stateInMemory, expectMetaData);
    }

    public void testNoWriteIfNothingChanged() throws Exception {
        boolean initializing = false;
        boolean versionChanged = false;
        boolean stateInMemory = true;
        boolean masterEligible = randomBoolean();
        boolean expectMetaData = false;
        ClusterChangedEvent event = generateEvent(initializing, versionChanged, masterEligible);
        ClusterChangedEvent newEventWithNothingChanged = new ClusterChangedEvent("test cluster state", event.state(), event.state());
        assertState(newEventWithNothingChanged, stateInMemory, expectMetaData);
    }

    public void testWriteClosedIndex() throws Exception {
        // test that the closing of an index is written also on data only node
        boolean masterEligible = randomBoolean();
        boolean expectMetaData = true;
        boolean stateInMemory = true;
        ClusterChangedEvent event = generateCloseEvent(masterEligible);
        assertState(event, stateInMemory, expectMetaData);
    }
}
