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

import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ClusterStateUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ClusterStateUtils.createIndexMetadata;
import static org.hamcrest.Matchers.equalTo;
import static org.elasticsearch.test.ClusterStateUtils.nextState;

/**
 * Tests for the {@link ClusterChangedEvent} class.
 */
public class ClusterChangedEventTests extends ESTestCase {

    private static final ClusterName TEST_CLUSTER_NAME = new ClusterName("test");
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
        newIndexMeta = createIndexMetadata(new Index("doesntexist", UUIDs.randomBase64UUID()), 1);
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

    /**
     * Test custom metadata change checks
     */
    public void testChangedCustomMetaDataSet() {
        final int numNodesInCluster = 3;

        final ClusterState originalState = createState(numNodesInCluster, randomBoolean(), initialIndices);
        CustomMetaData1 customMetaData1 = new CustomMetaData1("data");
        final ClusterState stateWithCustomMetaData = nextState(originalState, Collections.singletonList(customMetaData1));

        // no custom metadata present in any state
        ClusterState nextState = ClusterState.builder(originalState).build();
        ClusterChangedEvent event = new ClusterChangedEvent("_na_", originalState, nextState);
        assertTrue(event.changedCustomMetaDataSet().isEmpty());

        // next state has new custom metadata
        nextState = nextState(originalState, Collections.singletonList(customMetaData1));
        event = new ClusterChangedEvent("_na_", originalState, nextState);
        Set<String> changedCustomMetaDataTypeSet = event.changedCustomMetaDataSet();
        assertTrue(changedCustomMetaDataTypeSet.size() == 1);
        assertTrue(changedCustomMetaDataTypeSet.contains(customMetaData1.type()));

        // next state has same custom metadata
        nextState = nextState(originalState, Collections.singletonList(customMetaData1));
        event = new ClusterChangedEvent("_na_", stateWithCustomMetaData, nextState);
        changedCustomMetaDataTypeSet = event.changedCustomMetaDataSet();
        assertTrue(changedCustomMetaDataTypeSet.isEmpty());

        // next state has equivalent custom metadata
        nextState = nextState(originalState, Collections.singletonList(new CustomMetaData1("data")));
        event = new ClusterChangedEvent("_na_", stateWithCustomMetaData, nextState);
        changedCustomMetaDataTypeSet = event.changedCustomMetaDataSet();
        assertTrue(changedCustomMetaDataTypeSet.isEmpty());

        // next state removes custom metadata
        nextState = originalState;
        event = new ClusterChangedEvent("_na_", stateWithCustomMetaData, nextState);
        changedCustomMetaDataTypeSet = event.changedCustomMetaDataSet();
        assertTrue(changedCustomMetaDataTypeSet.size() == 1);
        assertTrue(changedCustomMetaDataTypeSet.contains(customMetaData1.type()));

        // next state updates custom metadata
        nextState = nextState(stateWithCustomMetaData, Collections.singletonList(new CustomMetaData1("data1")));
        event = new ClusterChangedEvent("_na_", stateWithCustomMetaData, nextState);
        changedCustomMetaDataTypeSet = event.changedCustomMetaDataSet();
        assertTrue(changedCustomMetaDataTypeSet.size() == 1);
        assertTrue(changedCustomMetaDataTypeSet.contains(customMetaData1.type()));

        // next state adds new custom metadata type
        CustomMetaData2 customMetaData2 = new CustomMetaData2("data2");
        nextState = nextState(stateWithCustomMetaData, Arrays.asList(customMetaData1, customMetaData2));
        event = new ClusterChangedEvent("_na_", stateWithCustomMetaData, nextState);
        changedCustomMetaDataTypeSet = event.changedCustomMetaDataSet();
        assertTrue(changedCustomMetaDataTypeSet.size() == 1);
        assertTrue(changedCustomMetaDataTypeSet.contains(customMetaData2.type()));

        // next state adds two custom metadata type
        nextState = nextState(originalState, Arrays.asList(customMetaData1, customMetaData2));
        event = new ClusterChangedEvent("_na_", originalState, nextState);
        changedCustomMetaDataTypeSet = event.changedCustomMetaDataSet();
        assertTrue(changedCustomMetaDataTypeSet.size() == 2);
        assertTrue(changedCustomMetaDataTypeSet.contains(customMetaData2.type()));
        assertTrue(changedCustomMetaDataTypeSet.contains(customMetaData1.type()));

        // next state removes two custom metadata type
        nextState = originalState;
        event = new ClusterChangedEvent("_na_",
                nextState(originalState, Arrays.asList(customMetaData1, customMetaData2)), nextState);
        changedCustomMetaDataTypeSet = event.changedCustomMetaDataSet();
        assertTrue(changedCustomMetaDataTypeSet.size() == 2);
        assertTrue(changedCustomMetaDataTypeSet.contains(customMetaData2.type()));
        assertTrue(changedCustomMetaDataTypeSet.contains(customMetaData1.type()));
    }

    private static class CustomMetaData2 extends TestCustomMetaData {
        static {
            MetaData.registerPrototype("2", new CustomMetaData2(""));
        }
        protected CustomMetaData2(String data) {
            super(data);
        }

        @Override
        protected TestCustomMetaData newTestCustomMetaData(String data) {
            return new CustomMetaData2(data);
        }

        @Override
        public String type() {
            return "2";
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetaData1 extends TestCustomMetaData {
        static {
            MetaData.registerPrototype("1", new CustomMetaData1(""));
        }
        protected CustomMetaData1(String data) {
            super(data);
        }

        @Override
        protected TestCustomMetaData newTestCustomMetaData(String data) {
            return new CustomMetaData1(data);
        }

        @Override
        public String type() {
            return "1";
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }
    }

    private static ClusterState createSimpleClusterState() {
        return ClusterState.builder(TEST_CLUSTER_NAME).build();
    }

    // Create a basic cluster state with a given set of indices
    private static ClusterState createState(final int numNodes, final boolean isLocalMaster, final List<Index> indices) {
        return ClusterStateUtils.createState(TEST_CLUSTER_NAME, numNodes, isLocalMaster, indices, random());
    }

    // Create a non-initialized cluster state
    private static ClusterState createNonInitializedState(final int numNodes, final boolean isLocalMaster) {
        final ClusterState withoutBlock = createState(numNodes, isLocalMaster, Collections.emptyList());
        return ClusterState.builder(withoutBlock)
                           .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                           .build();
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
