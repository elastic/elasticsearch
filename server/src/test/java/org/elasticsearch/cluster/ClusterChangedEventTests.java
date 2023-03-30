/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    private static final List<Index> initialIndices = Arrays.asList(
        new Index("idx1", UUIDs.randomBase64UUID()),
        new Index("idx2", UUIDs.randomBase64UUID()),
        new Index("idx3", UUIDs.randomBase64UUID())
    );

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
        } catch (NullPointerException e) {}
        try {
            event = new ClusterChangedEvent("_na_", null, previousState);
            fail("should not have created a ClusterChangedEvent from a null state: " + event.state());
        } catch (NullPointerException e) {}
        try {
            event = new ClusterChangedEvent("_na_", newState, null);
            fail("should not have created a ClusterChangedEvent from a null previousState: " + event.previousState());
        } catch (NullPointerException e) {}
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
    public void testIndicesMetadataChanges() {
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
    public void testIndexMetadataChange() {
        final int numNodesInCluster = 3;
        final ClusterState state = createState(numNodesInCluster, randomBoolean(), initialIndices);

        // test when its not the same IndexMetadata
        final Index index = initialIndices.get(0);
        final IndexMetadata originalIndexMeta = state.metadata().index(index);
        // make sure the metadata is actually on the cluster state
        assertNotNull("IndexMetadata for " + index + " should exist on the cluster state", originalIndexMeta);
        IndexMetadata newIndexMeta = createIndexMetadata(index, originalIndexMeta.getVersion() + 1);
        assertTrue(
            "IndexMetadata with different version numbers must be considered changed",
            ClusterChangedEvent.indexMetadataChanged(originalIndexMeta, newIndexMeta)
        );

        // test when it doesn't exist
        newIndexMeta = createIndexMetadata(new Index("doesntexist", UUIDs.randomBase64UUID()));
        assertTrue(
            "IndexMetadata that didn't previously exist should be considered changed",
            ClusterChangedEvent.indexMetadataChanged(originalIndexMeta, newIndexMeta)
        );

        // test when its the same IndexMetadata
        assertFalse("IndexMetadata should be the same", ClusterChangedEvent.indexMetadataChanged(originalIndexMeta, originalIndexMeta));
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
    public void testChangedCustomMetadataSet() {
        final int numNodesInCluster = 3;

        final ClusterState originalState = createState(numNodesInCluster, randomBoolean(), initialIndices);
        CustomMetadata1 customMetadata1 = new CustomMetadata1("data");
        final ClusterState stateWithCustomMetadata = nextState(originalState, Collections.singletonList(customMetadata1));

        // no custom metadata present in any state
        ClusterState nextState = ClusterState.builder(originalState).build();
        ClusterChangedEvent event = new ClusterChangedEvent("_na_", originalState, nextState);
        assertTrue(event.changedCustomMetadataSet().isEmpty());

        // next state has new custom metadata
        nextState = nextState(originalState, Collections.singletonList(customMetadata1));
        event = new ClusterChangedEvent("_na_", originalState, nextState);
        Set<String> changedCustomMetadataTypeSet = event.changedCustomMetadataSet();
        assertTrue(changedCustomMetadataTypeSet.size() == 1);
        assertTrue(changedCustomMetadataTypeSet.contains(customMetadata1.getWriteableName()));

        // next state has same custom metadata
        nextState = nextState(originalState, Collections.singletonList(customMetadata1));
        event = new ClusterChangedEvent("_na_", stateWithCustomMetadata, nextState);
        changedCustomMetadataTypeSet = event.changedCustomMetadataSet();
        assertTrue(changedCustomMetadataTypeSet.isEmpty());

        // next state has equivalent custom metadata
        nextState = nextState(originalState, Collections.singletonList(new CustomMetadata1("data")));
        event = new ClusterChangedEvent("_na_", stateWithCustomMetadata, nextState);
        changedCustomMetadataTypeSet = event.changedCustomMetadataSet();
        assertTrue(changedCustomMetadataTypeSet.isEmpty());

        // next state removes custom metadata
        nextState = originalState;
        event = new ClusterChangedEvent("_na_", stateWithCustomMetadata, nextState);
        changedCustomMetadataTypeSet = event.changedCustomMetadataSet();
        assertTrue(changedCustomMetadataTypeSet.size() == 1);
        assertTrue(changedCustomMetadataTypeSet.contains(customMetadata1.getWriteableName()));

        // next state updates custom metadata
        nextState = nextState(stateWithCustomMetadata, Collections.singletonList(new CustomMetadata1("data1")));
        event = new ClusterChangedEvent("_na_", stateWithCustomMetadata, nextState);
        changedCustomMetadataTypeSet = event.changedCustomMetadataSet();
        assertTrue(changedCustomMetadataTypeSet.size() == 1);
        assertTrue(changedCustomMetadataTypeSet.contains(customMetadata1.getWriteableName()));

        // next state adds new custom metadata type
        CustomMetadata2 customMetadata2 = new CustomMetadata2("data2");
        nextState = nextState(stateWithCustomMetadata, Arrays.asList(customMetadata1, customMetadata2));
        event = new ClusterChangedEvent("_na_", stateWithCustomMetadata, nextState);
        changedCustomMetadataTypeSet = event.changedCustomMetadataSet();
        assertTrue(changedCustomMetadataTypeSet.size() == 1);
        assertTrue(changedCustomMetadataTypeSet.contains(customMetadata2.getWriteableName()));

        // next state adds two custom metadata type
        nextState = nextState(originalState, Arrays.asList(customMetadata1, customMetadata2));
        event = new ClusterChangedEvent("_na_", originalState, nextState);
        changedCustomMetadataTypeSet = event.changedCustomMetadataSet();
        assertTrue(changedCustomMetadataTypeSet.size() == 2);
        assertTrue(changedCustomMetadataTypeSet.contains(customMetadata2.getWriteableName()));
        assertTrue(changedCustomMetadataTypeSet.contains(customMetadata1.getWriteableName()));

        // next state removes two custom metadata type
        nextState = originalState;
        event = new ClusterChangedEvent("_na_", nextState(originalState, Arrays.asList(customMetadata1, customMetadata2)), nextState);
        changedCustomMetadataTypeSet = event.changedCustomMetadataSet();
        assertTrue(changedCustomMetadataTypeSet.size() == 2);
        assertTrue(changedCustomMetadataTypeSet.contains(customMetadata2.getWriteableName()));
        assertTrue(changedCustomMetadataTypeSet.contains(customMetadata1.getWriteableName()));
    }

    private static class CustomMetadata2 extends TestCustomMetadata {
        protected CustomMetadata2(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return "2";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetadata1 extends TestCustomMetadata {
        protected CustomMetadata1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return "1";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static ClusterState createSimpleClusterState() {
        return ClusterState.builder(TEST_CLUSTER_NAME).build();
    }

    // Create a basic cluster state with a given set of indices
    private static ClusterState createState(final int numNodes, final boolean isLocalMaster, final List<Index> indices) {
        final Metadata metadata = createMetadata(indices);
        return ClusterState.builder(TEST_CLUSTER_NAME)
            .nodes(createDiscoveryNodes(numNodes, isLocalMaster))
            .metadata(metadata)
            .routingTable(createRoutingTable(1, metadata))
            .build();
    }

    // Create a non-initialized cluster state
    private static ClusterState createNonInitializedState(final int numNodes, final boolean isLocalMaster) {
        final ClusterState withoutBlock = createState(numNodes, isLocalMaster, Collections.emptyList());
        return ClusterState.builder(withoutBlock)
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
            .build();
    }

    private static ClusterState nextState(final ClusterState previousState, List<TestCustomMetadata> customMetadataList) {
        final ClusterState.Builder builder = ClusterState.builder(previousState);
        builder.stateUUID(UUIDs.randomBase64UUID());
        Metadata.Builder metadataBuilder = Metadata.builder(previousState.metadata());
        for (Map.Entry<String, Metadata.Custom> customMetadata : previousState.metadata().customs().entrySet()) {
            if (customMetadata.getValue() instanceof TestCustomMetadata) {
                metadataBuilder.removeCustom(customMetadata.getKey());
            }
        }
        for (TestCustomMetadata testCustomMetadata : customMetadataList) {
            metadataBuilder.putCustom(testCustomMetadata.getWriteableName(), testCustomMetadata);
        }
        builder.metadata(metadataBuilder);
        return builder.build();
    }

    // Create a modified cluster state from another one, but with some number of indices added and deleted.
    private static ClusterState nextState(
        final ClusterState previousState,
        final boolean changeClusterUUID,
        final List<Index> addedIndices,
        final List<Index> deletedIndices,
        final int numNodesToRemove
    ) {
        final ClusterState.Builder builder = ClusterState.builder(previousState);
        builder.stateUUID(UUIDs.randomBase64UUID());
        final Metadata.Builder metaBuilder = Metadata.builder(previousState.metadata());
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
            builder.metadata(metaBuilder);
        }
        if (numNodesToRemove > 0) {
            final int discoveryNodesSize = previousState.getNodes().getSize();
            final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(previousState.getNodes());
            for (int i = 0; i < numNodesToRemove && i < discoveryNodesSize; i++) {
                final String nodeId = NODE_ID_PREFIX + i;
                nodesBuilder.remove(NODE_ID_PREFIX + i);
                if (previousState.nodes().getMasterNodeId().equals(nodeId)) {
                    nodesBuilder.masterNodeId(null);
                }
                if (previousState.nodes().getLocalNodeId().equals(nodeId)) {
                    nodesBuilder.localNodeId(null);
                }
            }
            builder.nodes(nodesBuilder);
        }
        builder.blocks(ClusterBlocks.builder().build());
        return builder.build();
    }

    // Create the discovery nodes for a cluster state. For our testing purposes, we want
    // the first to be master, the second to be master eligible, the third to be a data node,
    // and the remainder can be any kinds of nodes (master eligible, data, or both).
    private static DiscoveryNodes createDiscoveryNodes(final int numNodes, final boolean isLocalMaster) {
        assert (numNodes >= 3)
            : "the initial cluster state for event change tests should have a minimum of 3 nodes "
                + "so there are a minimum of 2 master nodes for testing master change events.";
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int localNodeIndex = isLocalMaster ? 0 : randomIntBetween(1, numNodes - 1); // randomly assign the local node if not master
        for (int i = 0; i < numNodes; i++) {
            final String nodeId = NODE_ID_PREFIX + i;
            Set<DiscoveryNodeRole> roles = new HashSet<>();
            if (i == 0) {
                // the master node
                builder.masterNodeId(nodeId);
                roles.add(DiscoveryNodeRole.MASTER_ROLE);
            } else if (i == 1) {
                // the alternate master node
                roles.add(DiscoveryNodeRole.MASTER_ROLE);
            } else if (i == 2) {
                // we need at least one data node
                roles.add(DiscoveryNodeRole.DATA_ROLE);
            } else {
                // remaining nodes can be anything (except for master)
                if (randomBoolean()) {
                    roles.add(DiscoveryNodeRole.MASTER_ROLE);
                }
                if (randomBoolean()) {
                    roles.add(DiscoveryNodeRole.DATA_ROLE);
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
    private static DiscoveryNode newNode(final String nodeId, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode(
            nodeId,
            nodeId,
            nodeId,
            "host",
            "host_address",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            roles,
            Version.CURRENT
        );
    }

    // Create the metadata for a cluster state.
    private static Metadata createMetadata(final List<Index> indices) {
        final Metadata.Builder builder = Metadata.builder();
        builder.clusterUUID(INITIAL_CLUSTER_ID);
        for (Index index : indices) {
            builder.put(createIndexMetadata(index), true);
        }
        return builder.build();
    }

    // Create the index metadata for a given index.
    private static IndexMetadata createIndexMetadata(final Index index) {
        return createIndexMetadata(index, 1);
    }

    // Create the index metadata for a given index, with the specified version.
    private static IndexMetadata createIndexMetadata(final Index index, final long version) {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        return IndexMetadata.builder(index.getName())
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .creationDate(System.currentTimeMillis())
            .version(version)
            .build();
    }

    // Create the routing table for a cluster state.
    private static RoutingTable createRoutingTable(final long version, final Metadata metadata) {
        final RoutingTable.Builder builder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).version(version);
        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            builder.addAsNew(indexMetadata);
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
    private static ClusterState executeIndicesChangesTest(
        final ClusterState previousState,
        final TombstoneDeletionQuantity deletionQuantity
    ) {
        final int numAdd = randomIntBetween(0, 5); // add random # of indices to the next cluster state
        final List<Index> stateIndices = new ArrayList<>();
        for (IndexMetadata indexMetadata : previousState.metadata().indices().values()) {
            stateIndices.add(indexMetadata.getIndex());
        }
        final int numDel = switch (deletionQuantity) {
            case DELETE_ALL -> stateIndices.size();
            case DELETE_NONE -> 0;
            case DELETE_RANDOM -> randomIntBetween(0, Math.max(stateIndices.size() - 1, 0));
        };
        final boolean changeClusterUUID = randomBoolean();
        final List<Index> addedIndices = addIndices(numAdd, randomAlphaOfLengthBetween(5, 10));
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
        assertThat(event.metadataChanged(), equalTo(changeClusterUUID || addedIndices.size() > 0 || delIndices.size() > 0));
        final IndexGraveyard newGraveyard = event.state().metadata().indexGraveyard();
        final IndexGraveyard oldGraveyard = event.previousState().metadata().indexGraveyard();
        assertThat(((IndexGraveyard.IndexGraveyardDiff) newGraveyard.diff(oldGraveyard)).getAdded().size(), equalTo(delIndices.size()));
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
