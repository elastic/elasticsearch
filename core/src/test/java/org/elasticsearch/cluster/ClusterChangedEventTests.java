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

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link ClusterChangedEvent} class.
 */
public class ClusterChangedEventTests extends ESTestCase {

    private static final ClusterName TEST_CLUSTER_NAME = new ClusterName("test");
    private static final int INDICES_CHANGE_NUM_TESTS = 5;
    private static final String NODE_ID_PREFIX = "node_";
    private static final String INITIAL_CLUSTER_ID = Strings.randomBase64UUID();
    // the initial indices which every cluster state test starts out with
    private static final List<String> initialIndices = Arrays.asList("idx1", "idx2", "idx3");
    // index settings
    private static final Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();

    /**
     * Test basic properties of the ClusterChangedEvent class:
     *   (1) make sure there are no null values for any of its properties
     *   (2) make sure you can't create a ClusterChangedEvent with any null values
     */
    public void testBasicProperties() {
        ClusterState newState = createSimpleClusterState();
        ClusterState previousState = createSimpleClusterState();
        ClusterChangedEvent event = new ClusterChangedEvent("_na_", newState, previousState);
        assertThat(event.source(), equalTo("_na_"));
        assertThat(event.state(), equalTo(newState));
        assertThat(event.previousState(), equalTo(previousState));
        assertNotNull("nodesDelta should not be null", event.nodesDelta());

        // should not be able to create a ClusterChangedEvent with null values for any of the constructor args
        try {
            event = new ClusterChangedEvent(null, newState, previousState);
            fail("should not have created a ClusterChangedEvent from a null source: " + event.source());
        } catch (NullPointerException e) {
        }
        try {
            event = new ClusterChangedEvent("_na_", null, previousState);
            fail("should not have created a ClusterChangedEvent from a null state: " + event.state());
        } catch (NullPointerException e) {
        }
        try {
            event = new ClusterChangedEvent("_na_", newState, null);
            fail("should not have created a ClusterChangedEvent from a null previousState: " + event.previousState());
        } catch (NullPointerException e) {
        }
    }

    /**
     * Test whether the ClusterChangedEvent returns the correct value for whether the local node is master,
     * based on what was set on the cluster state.
     */
    public void testLocalNodeIsMaster() {
        final int numNodesInCluster = 3;
        ClusterState previousState = createSimpleClusterState();
        ClusterState newState = createState(numNodesInCluster, true, initialIndices);
        ClusterChangedEvent event = new ClusterChangedEvent("_na_", newState, previousState);
        assertTrue("local node should be master", event.localNodeMaster());

        newState = createState(numNodesInCluster, false, initialIndices);
        event = new ClusterChangedEvent("_na_", newState, previousState);
        assertFalse("local node should not be master", event.localNodeMaster());
    }

    /**
     * Test that the indices created and indices deleted lists between two cluster states
     * are correct when there is no change in the cluster UUID.  Also tests metadata equality
     * between cluster states.
     */
    public void testMetaDataChangesOnNoMasterChange() {
        metaDataChangesCheck(false);
    }

    /**
     * Test that the indices created and indices deleted lists between two cluster states
     * are correct when there is a change in the cluster UUID.  Also tests metadata equality
     * between cluster states.
     */
    public void testMetaDataChangesOnNewClusterUUID() {
        metaDataChangesCheck(true);
    }

    /**
     * Test the index metadata change check.
     */
    public void testIndexMetaDataChange() {
        final int numNodesInCluster = 3;
        final ClusterState originalState = createState(numNodesInCluster, randomBoolean(), initialIndices);
        final ClusterState newState = originalState; // doesn't matter for this test, just need a non-null value
        final ClusterChangedEvent event = new ClusterChangedEvent("_na_", originalState, newState);

        // test when its not the same IndexMetaData
        final String indexId = initialIndices.get(0);
        final IndexMetaData originalIndexMeta = originalState.metaData().index(indexId);
        // make sure the metadata is actually on the cluster state
        assertNotNull("IndexMetaData for " + indexId + " should exist on the cluster state", originalIndexMeta);
        IndexMetaData newIndexMeta = createIndexMetadata(indexId, originalIndexMeta.getVersion() + 1);
        assertTrue("IndexMetaData with different version numbers must be considered changed", event.indexMetaDataChanged(newIndexMeta));

        // test when it doesn't exist
        newIndexMeta = createIndexMetadata("doesntexist");
        assertTrue("IndexMetaData that didn't previously exist should be considered changed", event.indexMetaDataChanged(newIndexMeta));

        // test when its the same IndexMetaData
        assertFalse("IndexMetaData should be the same", event.indexMetaDataChanged(originalIndexMeta));
    }

    /**
     * Test nodes added/removed/changed checks.
     */
    public void testNodesAddedAndRemovedAndChanged() {
        final int numNodesInCluster = 4;
        final ClusterState originalState = createState(numNodesInCluster, randomBoolean(), initialIndices);

        // test when nodes have not been added or removed between cluster states
        ClusterState newState = createState(numNodesInCluster, randomBoolean(), initialIndices);
        ClusterChangedEvent event = new ClusterChangedEvent("_na_", newState, originalState);
        assertFalse("Nodes should not have been added between cluster states", event.nodesAdded());
        assertFalse("Nodes should not have been removed between cluster states", event.nodesRemoved());
        assertFalse("Nodes should not have been changed between cluster states", event.nodesChanged());

        // test when nodes have been removed between cluster states
        newState = createState(numNodesInCluster - 1, randomBoolean(), initialIndices);
        event = new ClusterChangedEvent("_na_", newState, originalState);
        assertTrue("Nodes should have been removed between cluster states", event.nodesRemoved());
        assertFalse("Nodes should not have been added between cluster states", event.nodesAdded());
        assertTrue("Nodes should have been changed between cluster states", event.nodesChanged());

        // test when nodes have been added between cluster states
        newState = createState(numNodesInCluster + 1, randomBoolean(), initialIndices);
        event = new ClusterChangedEvent("_na_", newState, originalState);
        assertFalse("Nodes should not have been removed between cluster states", event.nodesRemoved());
        assertTrue("Nodes should have been added between cluster states", event.nodesAdded());
        assertTrue("Nodes should have been changed between cluster states", event.nodesChanged());

        // test when nodes both added and removed between cluster states
        // here we reuse the newState from the previous run which already added extra nodes
        newState = nextState(newState, randomBoolean(), Collections.<String>emptyList(), Collections.<String>emptyList(), 1);
        event = new ClusterChangedEvent("_na_", newState, originalState);
        assertTrue("Nodes should have been removed between cluster states", event.nodesRemoved());
        assertTrue("Nodes should have been added between cluster states", event.nodesAdded());
        assertTrue("Nodes should have been changed between cluster states", event.nodesChanged());
    }

    /**
     * Test the routing table changes checks.
     */
    public void testRoutingTableChanges() {
        final int numNodesInCluster = 3;
        final ClusterState originalState = createState(numNodesInCluster, randomBoolean(), initialIndices);

        // routing tables and index routing tables are same object
        ClusterState newState = ClusterState.builder(originalState).build();
        ClusterChangedEvent event = new ClusterChangedEvent("_na_", originalState, newState);
        assertFalse("routing tables should be the same object", event.routingTableChanged());
        assertFalse("index routing table should be the same object", event.indexRoutingTableChanged(initialIndices.get(0)));

        // routing tables and index routing tables aren't same object
        newState = createState(numNodesInCluster, randomBoolean(), initialIndices);
        event = new ClusterChangedEvent("_na_", originalState, newState);
        assertTrue("routing tables should not be the same object", event.routingTableChanged());
        assertTrue("index routing table should not be the same object", event.indexRoutingTableChanged(initialIndices.get(0)));

        // index routing tables are different because they don't exist
        newState = createState(numNodesInCluster, randomBoolean(), initialIndices.subList(1, initialIndices.size()));
        event = new ClusterChangedEvent("_na_", originalState, newState);
        assertTrue("routing tables should not be the same object", event.routingTableChanged());
        assertTrue("index routing table should not be the same object", event.indexRoutingTableChanged(initialIndices.get(0)));
    }

    // Tests that the indices change list is correct as well as metadata equality when the metadata has changed.
    private static void metaDataChangesCheck(final boolean changeClusterUUID) {
        final int numNodesInCluster = 3;
        for (int i = 0; i < INDICES_CHANGE_NUM_TESTS; i++) {
            final ClusterState previousState = createState(numNodesInCluster, randomBoolean(), initialIndices);
            final int numAdd = randomIntBetween(0, 5); // add random # of indices to the next cluster state
            final int numDel = randomIntBetween(0, initialIndices.size()); // delete random # of indices from the next cluster state
            final List<String> addedIndices = addIndices(numAdd);
            final List<String> delIndices = delIndices(numDel, initialIndices);
            final ClusterState newState = nextState(previousState, changeClusterUUID, addedIndices, delIndices, 0);
            final ClusterChangedEvent event = new ClusterChangedEvent("_na_", newState, previousState);
            final List<String> addsFromEvent = event.indicesCreated();
            final List<String> delsFromEvent = event.indicesDeleted();
            Collections.sort(addsFromEvent);
            Collections.sort(delsFromEvent);
            assertThat(addsFromEvent, equalTo(addedIndices));
            assertThat(delsFromEvent, changeClusterUUID ? equalTo(Collections.<String>emptyList()) : equalTo(delIndices));
            assertThat(event.metaDataChanged(), equalTo(changeClusterUUID || addedIndices.size() > 0 || delIndices.size() > 0));
        }
    }

    private static ClusterState createSimpleClusterState() {
        return ClusterState.builder(TEST_CLUSTER_NAME).build();
    }

    // Create a basic cluster state with a given set of indices
    private static ClusterState createState(final int numNodes, final boolean isLocalMaster, final List<String> indices) {
        final MetaData metaData = createMetaData(indices);
        return ClusterState.builder(TEST_CLUSTER_NAME)
                           .nodes(createDiscoveryNodes(numNodes, isLocalMaster))
                           .metaData(metaData)
                           .routingTable(createRoutingTable(1, metaData))
                           .build();
    }

    // Create a modified cluster state from another one, but with some number of indices added and deleted.
    private static ClusterState nextState(final ClusterState previousState, final boolean changeClusterUUID,
                                          final List<String> addedIndices, final List<String> deletedIndices,
                                          final int numNodesToRemove) {
        final ClusterState.Builder builder = ClusterState.builder(previousState);
        builder.stateUUID(Strings.randomBase64UUID());
        final MetaData.Builder metaBuilder = MetaData.builder(previousState.metaData());
        if (changeClusterUUID || addedIndices.size() > 0 || deletedIndices.size() > 0) {
            // there is some change in metadata cluster state
            if (changeClusterUUID) {
                metaBuilder.clusterUUID(Strings.randomBase64UUID());
            }
            for (String index : addedIndices) {
                metaBuilder.put(createIndexMetadata(index), true);
            }
            for (String index : deletedIndices) {
                metaBuilder.remove(index);
            }
            builder.metaData(metaBuilder);
        }
        if (numNodesToRemove > 0) {
            final int discoveryNodesSize = previousState.getNodes().size();
            final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(previousState.getNodes());
            for (int i = 0; i < numNodesToRemove && i < discoveryNodesSize; i++) {
                nodesBuilder.remove(NODE_ID_PREFIX + i);
            }
            builder.nodes(nodesBuilder);
        }
        return builder.build();
    }

    // Create the discovery nodes for a cluster state.  For our testing purposes, we want
    // the first to be master, the second to be master eligible, the third to be a data node,
    // and the remainder can be any kinds of nodes (master eligible, data, or both).
    private static DiscoveryNodes createDiscoveryNodes(final int numNodes, final boolean isLocalMaster) {
        assert (numNodes >= 3) : "the initial cluster state for event change tests should have a minimum of 3 nodes " +
                                 "so there are a minimum of 2 master nodes for testing master change events.";
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int localNodeIndex = isLocalMaster ? 0 : randomIntBetween(1, numNodes - 1); // randomly assign the local node if not master
        for (int i = 0; i < numNodes; i++) {
            final String nodeId = NODE_ID_PREFIX + i;
            boolean isMasterEligible = false;
            boolean isData = false;
            if (i == 0) {
                // the master node
                builder.masterNodeId(nodeId);
                isMasterEligible = true;
            } else if (i == 1) {
                // the alternate master node
                isMasterEligible = true;
            } else if (i == 2) {
                // we need at least one data node
                isData = true;
            } else {
                // remaining nodes can be anything (except for master)
                isMasterEligible = randomBoolean();
                isData = randomBoolean();
            }
            final DiscoveryNode node = newNode(nodeId, isMasterEligible, isData);
            builder.put(node);
            if (i == localNodeIndex) {
                builder.localNodeId(nodeId);
            }
        }
        return builder.build();
    }

    // Create a new DiscoveryNode
    private static DiscoveryNode newNode(final String nodeId, boolean isMasterEligible, boolean isData) {
        final Map<String, String> attributes = MapBuilder.<String, String>newMapBuilder()
                                                   .put(DiscoveryNode.MASTER_ATTR, isMasterEligible ? "true" : "false")
                                                   .put(DiscoveryNode.DATA_ATTR, isData ? "true": "false")
                                                   .immutableMap();
        return new DiscoveryNode(nodeId, nodeId, DummyTransportAddress.INSTANCE, attributes, Version.CURRENT);
    }

    // Create the metadata for a cluster state.
    private static MetaData createMetaData(final List<String> indices) {
        final MetaData.Builder builder = MetaData.builder();
        builder.clusterUUID(INITIAL_CLUSTER_ID);
        for (String index : indices) {
            builder.put(createIndexMetadata(index), true);
        }
        return builder.build();
    }

    // Create the index metadata for a given index.
    private static IndexMetaData createIndexMetadata(final String index) {
        return createIndexMetadata(index, 1);
    }

    // Create the index metadata for a given index, with the specified version.
    private static IndexMetaData createIndexMetadata(final String index, final long version) {
        return IndexMetaData.builder(index)
                            .settings(settings)
                            .numberOfShards(1)
                            .numberOfReplicas(0)
                            .creationDate(System.currentTimeMillis())
                            .version(version)
                            .build();
    }

    // Create the routing table for a cluster state.
    private static RoutingTable createRoutingTable(final long version, final MetaData metaData) {
        final RoutingTable.Builder builder = RoutingTable.builder().version(version);
        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            builder.addAsNew(cursor.value);
        }
        return builder.build();
    }

    // Create a list of indices to add
    private static List<String> addIndices(final int numIndices) {
        final List<String> list = new ArrayList<>();
        for (int i = 0; i < numIndices; i++) {
            list.add("newIdx_" + i);
        }
        return list;
    }

    // Create a list of indices to delete from a list that already belongs to a particular cluster state.
    private static List<String> delIndices(final int numIndices, final List<String> currIndices) {
        final List<String> list = new ArrayList<>();
        for (int i = 0; i < numIndices; i++) {
            list.add(currIndices.get(i));
        }
        return list;
    }

}
