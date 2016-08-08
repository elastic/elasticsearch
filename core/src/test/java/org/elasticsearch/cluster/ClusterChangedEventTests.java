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
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link ClusterChangedEvent} class.
 */
public class ClusterChangedEventTests extends ESTestCase {

    private static final ClusterName TEST_CLUSTER_NAME = new ClusterName("test");
    private static final String NODE_ID_PREFIX = "node_";
    private static final String INITIAL_CLUSTER_ID = UUIDs.randomBase64UUID();
    // the initial indices which every cluster state test starts out with
    private static final List<Index> initialIndices = Arrays.asList(new Index("idx1", UUIDs.randomBase64UUID()),
                                                                    new Index("idx2", UUIDs.randomBase64UUID()),
                                                                    new Index("idx3", UUIDs.randomBase64UUID()));

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
     * are correct when there is a change in indices added and deleted.  Also tests metadata
     * equality between cluster states.
     */
    public void testIndicesMetaDataChanges() {
        final int numNodesInCluster = 3;
        ClusterState previousState = createState(numNodesInCluster, randomBoolean(), initialIndices);
        for (TombstoneDeletionQuantity quantity : TombstoneDeletionQuantity.valuesInRandomizedOrder()) {
            final ClusterState newState = executeIndicesChangesTest(previousState, quantity);
            previousState = newState; // serves as the base cluster state for the next iteration
        }
    }

    /**
     * Test that the indices deleted list is correct when the previous cluster state is
     * not initialized/recovered.  This should trigger the use of the index tombstones to
     * determine the deleted indices.
     */
    public void testIndicesDeletionWithNotRecoveredState() {
        // test with all the various tombstone deletion quantities
        for (TombstoneDeletionQuantity quantity : TombstoneDeletionQuantity.valuesInRandomizedOrder()) {
            final ClusterState previousState = createNonInitializedState(randomIntBetween(3, 5), randomBoolean());
            executeIndicesChangesTest(previousState, quantity);
        }
    }

    /**
     * Test the index metadata change check.
     */
    public void testIndexMetaDataChange() {
        final int numNodesInCluster = 3;
        final ClusterState state = createState(numNodesInCluster, randomBoolean(), initialIndices);

        // test when its not the same IndexMetaData
        final Index index = initialIndices.get(0);
        final IndexMetaData originalIndexMeta = state.metaData().index(index);
        // make sure the metadata is actually on the cluster state
        assertNotNull("IndexMetaData for " + index + " should exist on the cluster state", originalIndexMeta);
        IndexMetaData newIndexMeta = createIndexMetadata(index, originalIndexMeta.getVersion() + 1);
        assertTrue("IndexMetaData with different version numbers must be considered changed",
            ClusterChangedEvent.indexMetaDataChanged(originalIndexMeta, newIndexMeta));

        // test when it doesn't exist
        newIndexMeta = createIndexMetadata(new Index("doesntexist", UUIDs.randomBase64UUID()));
        assertTrue("IndexMetaData that didn't previously exist should be considered changed",
            ClusterChangedEvent.indexMetaDataChanged(originalIndexMeta, newIndexMeta));

        // test when its the same IndexMetaData
        assertFalse("IndexMetaData should be the same", ClusterChangedEvent.indexMetaDataChanged(originalIndexMeta, originalIndexMeta));
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
        newState = nextState(newState, randomBoolean(), Collections.emptyList(), Collections.emptyList(), 1);
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
        assertFalse("index routing table should be the same object", event.indexRoutingTableChanged(initialIndices.get(0).getName()));

        // routing tables and index routing tables aren't same object
        newState = createState(numNodesInCluster, randomBoolean(), initialIndices);
        event = new ClusterChangedEvent("_na_", originalState, newState);
        assertTrue("routing tables should not be the same object", event.routingTableChanged());
        assertTrue("index routing table should not be the same object", event.indexRoutingTableChanged(initialIndices.get(0).getName()));

        // index routing tables are different because they don't exist
        newState = createState(numNodesInCluster, randomBoolean(), initialIndices.subList(1, initialIndices.size()));
        event = new ClusterChangedEvent("_na_", originalState, newState);
        assertTrue("routing tables should not be the same object", event.routingTableChanged());
        assertTrue("index routing table should not be the same object", event.indexRoutingTableChanged(initialIndices.get(0).getName()));
    }

    private static ClusterState createSimpleClusterState() {
        return ClusterState.builder(TEST_CLUSTER_NAME).build();
    }

    // Create a basic cluster state with a given set of indices
    private static ClusterState createState(final int numNodes, final boolean isLocalMaster, final List<Index> indices) {
        final MetaData metaData = createMetaData(indices);
        return ClusterState.builder(TEST_CLUSTER_NAME)
                           .nodes(createDiscoveryNodes(numNodes, isLocalMaster))
                           .metaData(metaData)
                           .routingTable(createRoutingTable(1, metaData))
                           .build();
    }

    // Create a non-initialized cluster state
    private static ClusterState createNonInitializedState(final int numNodes, final boolean isLocalMaster) {
        final ClusterState withoutBlock = createState(numNodes, isLocalMaster, Collections.emptyList());
        return ClusterState.builder(withoutBlock)
                           .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                           .build();
    }

    // Create a modified cluster state from another one, but with some number of indices added and deleted.
    private static ClusterState nextState(final ClusterState previousState, final boolean changeClusterUUID,
                                          final List<Index> addedIndices, final List<Index> deletedIndices, final int numNodesToRemove) {
        final ClusterState.Builder builder = ClusterState.builder(previousState);
        builder.stateUUID(UUIDs.randomBase64UUID());
        final MetaData.Builder metaBuilder = MetaData.builder(previousState.metaData());
        if (changeClusterUUID || addedIndices.size() > 0 || deletedIndices.size() > 0) {
            // there is some change in metadata cluster state
            if (changeClusterUUID) {
                metaBuilder.clusterUUID(UUIDs.randomBase64UUID());
            }
            for (Index index : addedIndices) {
                metaBuilder.put(createIndexMetadata(index), true);
            }
            for (Index index : deletedIndices) {
                metaBuilder.remove(index.getName());
                IndexGraveyard.Builder graveyardBuilder = IndexGraveyard.builder(metaBuilder.indexGraveyard());
                graveyardBuilder.addTombstone(index);
                metaBuilder.indexGraveyard(graveyardBuilder.build());
            }
            builder.metaData(metaBuilder);
        }
        if (numNodesToRemove > 0) {
            final int discoveryNodesSize = previousState.getNodes().getSize();
            final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(previousState.getNodes());
            for (int i = 0; i < numNodesToRemove && i < discoveryNodesSize; i++) {
                nodesBuilder.remove(NODE_ID_PREFIX + i);
            }
            builder.nodes(nodesBuilder);
        }
        builder.blocks(ClusterBlocks.builder().build());
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
            Set<DiscoveryNode.Role> roles = new HashSet<>();
            if (i == 0) {
                // the master node
                builder.masterNodeId(nodeId);
                roles.add(DiscoveryNode.Role.MASTER);
            } else if (i == 1) {
                // the alternate master node
                roles.add(DiscoveryNode.Role.MASTER);
            } else if (i == 2) {
                // we need at least one data node
                roles.add(DiscoveryNode.Role.DATA);
            } else {
                // remaining nodes can be anything (except for master)
                if (randomBoolean()) {
                    roles.add(DiscoveryNode.Role.MASTER);
                }
                if (randomBoolean()) {
                    roles.add(DiscoveryNode.Role.DATA);
                }
            }
            final DiscoveryNode node = newNode(nodeId, roles);
            builder.add(node);
            if (i == localNodeIndex) {
                builder.localNodeId(nodeId);
            }
        }
        return builder.build();
    }

    // Create a new DiscoveryNode
    private static DiscoveryNode newNode(final String nodeId, Set<DiscoveryNode.Role> roles) {
        return new DiscoveryNode(nodeId, nodeId, nodeId, "host", "host_address", new LocalTransportAddress("_test_" + nodeId),
            Collections.emptyMap(), roles, Version.CURRENT);
    }

    // Create the metadata for a cluster state.
    private static MetaData createMetaData(final List<Index> indices) {
        final MetaData.Builder builder = MetaData.builder();
        builder.clusterUUID(INITIAL_CLUSTER_ID);
        for (Index index : indices) {
            builder.put(createIndexMetadata(index), true);
        }
        return builder.build();
    }

    // Create the index metadata for a given index.
    private static IndexMetaData createIndexMetadata(final Index index) {
        return createIndexMetadata(index, 1);
    }

    // Create the index metadata for a given index, with the specified version.
    private static IndexMetaData createIndexMetadata(final Index index, final long version) {
        final Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                                                    .put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID())
                                                    .build();
        return IndexMetaData.builder(index.getName())
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
    private static List<Index> addIndices(final int numIndices, final String id) {
        final List<Index> list = new ArrayList<>();
        for (int i = 0; i < numIndices; i++) {
            list.add(new Index("newIdx_" + id + "_" + i, UUIDs.randomBase64UUID()));
        }
        return list;
    }

    // Create a list of indices to delete from a list that already belongs to a particular cluster state.
    private static List<Index> delIndices(final int numIndices, final List<Index> currIndices) {
        final List<Index> list = new ArrayList<>();
        for (int i = 0; i < numIndices; i++) {
            list.add(currIndices.get(i));
        }
        return list;
    }

    // execute the indices changes test by generating random index additions and deletions and
    // checking the values on the cluster changed event.
    private static ClusterState executeIndicesChangesTest(final ClusterState previousState,
                                                          final TombstoneDeletionQuantity deletionQuantity) {
        final int numAdd = randomIntBetween(0, 5); // add random # of indices to the next cluster state
        final List<Index> stateIndices = new ArrayList<>();
        for (Iterator<IndexMetaData> iter = previousState.metaData().indices().valuesIt(); iter.hasNext();) {
            stateIndices.add(iter.next().getIndex());
        }
        final int numDel;
        switch (deletionQuantity) {
            case DELETE_ALL: {
                numDel = stateIndices.size();
                break;
            }
            case DELETE_NONE: {
                numDel = 0;
                break;
            }
            case DELETE_RANDOM: {
                numDel = randomIntBetween(0, Math.max(stateIndices.size() - 1, 0));
                break;
            }
            default: throw new AssertionError("Unhandled mode [" + deletionQuantity + "]");
        }
        final boolean changeClusterUUID = randomBoolean();
        final List<Index> addedIndices = addIndices(numAdd, randomAsciiOfLengthBetween(5, 10));
        List<Index> delIndices;
        if (changeClusterUUID) {
            delIndices = new ArrayList<>();
        } else {
            delIndices = delIndices(numDel, stateIndices);
        }
        final ClusterState newState = nextState(previousState, changeClusterUUID, addedIndices, delIndices, 0);
        ClusterChangedEvent event = new ClusterChangedEvent("_na_", newState, previousState);
        final List<String> addsFromEvent = event.indicesCreated();
        List<Index> delsFromEvent = event.indicesDeleted();
        assertThat(new HashSet<>(addsFromEvent), equalTo(addedIndices.stream().map(Index::getName).collect(Collectors.toSet())));
        assertThat(new HashSet<>(delsFromEvent), equalTo(new HashSet<>(delIndices)));
        assertThat(event.metaDataChanged(), equalTo(changeClusterUUID || addedIndices.size() > 0 || delIndices.size() > 0));
        final IndexGraveyard newGraveyard = event.state().metaData().indexGraveyard();
        final IndexGraveyard oldGraveyard = event.previousState().metaData().indexGraveyard();
        assertThat(((IndexGraveyard.IndexGraveyardDiff)newGraveyard.diff(oldGraveyard)).getAdded().size(), equalTo(delIndices.size()));
        return newState;
    }

    private enum TombstoneDeletionQuantity {
        DELETE_RANDOM, // delete a random number of tombstones from cluster state (not zero and not all)
        DELETE_NONE, // delete none of the tombstones from cluster state
        DELETE_ALL; // delete all tombstones from cluster state

        static List<TombstoneDeletionQuantity> valuesInRandomizedOrder() {
            final List<TombstoneDeletionQuantity> randomOrderQuantities = new ArrayList<>(EnumSet.allOf(TombstoneDeletionQuantity.class));
            Collections.shuffle(randomOrderQuantities, random());
            return randomOrderQuantities;
        }
    }

}
