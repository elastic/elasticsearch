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

package org.elasticsearch.test;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.buildNewFakeTransportAddress;

public class ClusterStateUtils {
    private static final String NODE_ID_PREFIX = "node_";
    private static final String INITIAL_CLUSTER_ID = UUIDs.randomBase64UUID();

    // Create a basic cluster state with a given set of indices
    public static ClusterState createState(ClusterName clusterName, final int numNodes, final boolean isLocalMaster,
                                           final List<Index> indices, Random random) {
        final MetaData metaData = createMetaData(indices);
        return ClusterState.builder(clusterName)
                .nodes(createDiscoveryNodes(numNodes, isLocalMaster, random))
                .metaData(metaData)
                .routingTable(createRoutingTable(1, metaData))
                .build();
    }

    public static ClusterState nextState(final ClusterState previousState, List<TestCustomMetaData> customMetaDataList) {
        final ClusterState.Builder builder = ClusterState.builder(previousState);
        builder.stateUUID(UUIDs.randomBase64UUID());
        MetaData.Builder metaDataBuilder = new MetaData.Builder(previousState.metaData());
        /*for (ObjectObjectCursor<String, MetaData.Custom> customMetaData : previousState.metaData().customs()) {
            if (customMetaData.value instanceof TestCustomMetaData) {
                metaDataBuilder.removeCustom(customMetaData.key);
            }
        }*/
        for (TestCustomMetaData testCustomMetaData : customMetaDataList) {
            metaDataBuilder.putCustom(testCustomMetaData.type(), testCustomMetaData);
        }
        builder.metaData(metaDataBuilder);
        return builder.build();
    }

    // Create a modified cluster state from another one, but with some number of indices added and deleted.
    public static ClusterState nextState(final ClusterState previousState, final boolean changeClusterUUID,
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
    public static IndexMetaData createIndexMetadata(final Index index, final long version) {
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

    // Create the discovery nodes for a cluster state.  For our testing purposes, we want
    // the first to be master, the second to be master eligible, the third to be a data node,
    // and the remainder can be any kinds of nodes (master eligible, data, or both).
    private static DiscoveryNodes createDiscoveryNodes(final int numNodes, final boolean isLocalMaster, final Random random) {
        assert (numNodes >= 3) : "the initial cluster state for event change tests should have a minimum of 3 nodes " +
                "so there are a minimum of 2 master nodes for testing master change events.";
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final int localNodeIndex = isLocalMaster ? 0 :
                RandomNumbers.randomIntBetween(random, 1, numNodes - 1); // randomly assign the local node if not master
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
                if (random.nextBoolean()) {
                    roles.add(DiscoveryNode.Role.MASTER);
                }
                if (random.nextBoolean()) {
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

    private static DiscoveryNode newNode(final String nodeId, Set<DiscoveryNode.Role> roles) {
        return new DiscoveryNode(nodeId, nodeId, nodeId, "host", "host_address", buildNewFakeTransportAddress(),
                Collections.emptyMap(), roles, Version.CURRENT);
    }

    // Create the routing table for a cluster state.
    private static RoutingTable createRoutingTable(final long version, final MetaData metaData) {
        final RoutingTable.Builder builder = RoutingTable.builder().version(version);
        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            builder.addAsNew(cursor.value);
        }
        return builder.build();
    }
}
