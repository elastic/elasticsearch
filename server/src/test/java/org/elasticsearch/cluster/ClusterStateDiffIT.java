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
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexGraveyardTests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfoTests;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.metadata.AliasMetaData.newAliasMetaDataBuilder;
import static org.elasticsearch.cluster.routing.RandomShardRoutingMutator.randomChange;
import static org.elasticsearch.cluster.routing.RandomShardRoutingMutator.randomReason;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.elasticsearch.test.XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;


@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0, numClientNodes = 0)
public class ClusterStateDiffIT extends ESIntegTestCase {
    public void testClusterStateDiffSerialization() throws Exception {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        DiscoveryNode masterNode = randomNode("master");
        DiscoveryNode otherNode = randomNode("other");
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(masterNode).add(otherNode).localNodeId(masterNode.getId()).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        ClusterState clusterStateFromDiffs =
            ClusterState.Builder.fromBytes(ClusterState.Builder.toBytes(clusterState), otherNode, namedWriteableRegistry);

        int iterationCount = randomIntBetween(10, 300);
        for (int iteration = 0; iteration < iterationCount; iteration++) {
            ClusterState previousClusterState = clusterState;
            ClusterState previousClusterStateFromDiffs = clusterStateFromDiffs;
            int changesCount = randomIntBetween(1, 4);
            ClusterState.Builder builder = null;
            for (int i = 0; i < changesCount; i++) {
                if (i > 0) {
                    clusterState = builder.build();
                }
                switch (randomInt(5)) {
                    case 0:
                        builder = randomNodes(clusterState);
                        break;
                    case 1:
                        builder = randomRoutingTable(clusterState);
                        break;
                    case 2:
                        builder = randomBlocks(clusterState);
                        break;
                    case 3:
                        builder = randomClusterStateCustoms(clusterState);
                        break;
                    case 4:
                        builder = randomMetaDataChanges(clusterState);
                        break;
                    case 5:
                        builder = randomCoordinationMetaData(clusterState);
                        break;
                    default:
                        throw new IllegalArgumentException("Shouldn't be here");
                }
            }
            clusterState = builder.incrementVersion().build();

            if (randomIntBetween(0, 10) < 1) {
                // Update cluster state via full serialization from time to time
                clusterStateFromDiffs = ClusterState.Builder.fromBytes(ClusterState.Builder.toBytes(clusterState),
                    previousClusterStateFromDiffs.nodes().getLocalNode(), namedWriteableRegistry);
            } else {
                // Update cluster states using diffs
                Diff<ClusterState> diffBeforeSerialization = clusterState.diff(previousClusterState);
                BytesStreamOutput os = new BytesStreamOutput();
                diffBeforeSerialization.writeTo(os);
                byte[] diffBytes = BytesReference.toBytes(os.bytes());
                Diff<ClusterState> diff;
                try (StreamInput input = StreamInput.wrap(diffBytes)) {
                    StreamInput namedInput = new NamedWriteableAwareStreamInput(input, namedWriteableRegistry);
                    diff = ClusterState.readDiffFrom(namedInput, previousClusterStateFromDiffs.nodes().getLocalNode());
                    clusterStateFromDiffs = diff.apply(previousClusterStateFromDiffs);
                }
            }


            try {
                // Check non-diffable elements
                assertThat(clusterStateFromDiffs.version(), equalTo(clusterState.version()));
                assertThat(clusterStateFromDiffs.coordinationMetaData(), equalTo(clusterState.coordinationMetaData()));

                // Check nodes
                assertThat(clusterStateFromDiffs.nodes().getNodes(), equalTo(clusterState.nodes().getNodes()));
                assertThat(clusterStateFromDiffs.nodes().getLocalNodeId(), equalTo(previousClusterStateFromDiffs.nodes().getLocalNodeId()));
                assertThat(clusterStateFromDiffs.nodes().getNodes(), equalTo(clusterState.nodes().getNodes()));
                for (ObjectCursor<String> node : clusterStateFromDiffs.nodes().getNodes().keys()) {
                    DiscoveryNode node1 = clusterState.nodes().get(node.value);
                    DiscoveryNode node2 = clusterStateFromDiffs.nodes().get(node.value);
                    assertThat(node1.getVersion(), equalTo(node2.getVersion()));
                    assertThat(node1.getAddress(), equalTo(node2.getAddress()));
                    assertThat(node1.getAttributes(), equalTo(node2.getAttributes()));
                }

                // Check routing table
                assertThat(clusterStateFromDiffs.routingTable().version(), equalTo(clusterState.routingTable().version()));
                assertThat(clusterStateFromDiffs.routingTable().indicesRouting(), equalTo(clusterState.routingTable().indicesRouting()));

                // Check cluster blocks
                assertThat(clusterStateFromDiffs.blocks().global(), equalTo(clusterStateFromDiffs.blocks().global()));
                assertThat(clusterStateFromDiffs.blocks().indices(), equalTo(clusterStateFromDiffs.blocks().indices()));
                assertThat(clusterStateFromDiffs.blocks().disableStatePersistence(),
                    equalTo(clusterStateFromDiffs.blocks().disableStatePersistence()));

                // Check metadata
                assertThat(clusterStateFromDiffs.metaData().version(), equalTo(clusterState.metaData().version()));
                assertThat(clusterStateFromDiffs.metaData().clusterUUID(), equalTo(clusterState.metaData().clusterUUID()));
                assertThat(clusterStateFromDiffs.metaData().transientSettings(), equalTo(clusterState.metaData().transientSettings()));
                assertThat(clusterStateFromDiffs.metaData().persistentSettings(), equalTo(clusterState.metaData().persistentSettings()));
                assertThat(clusterStateFromDiffs.metaData().indices(), equalTo(clusterState.metaData().indices()));
                assertThat(clusterStateFromDiffs.metaData().templates(), equalTo(clusterState.metaData().templates()));
                assertThat(clusterStateFromDiffs.metaData().customs(), equalTo(clusterState.metaData().customs()));
                assertThat(clusterStateFromDiffs.metaData().equalsAliases(clusterState.metaData()), is(true));

                // JSON Serialization test - make sure that both states produce similar JSON
                assertNull(differenceBetweenMapsIgnoringArrayOrder(convertToMap(clusterStateFromDiffs), convertToMap(clusterState)));

                // Smoke test - we cannot compare bytes to bytes because some elements might get serialized in different order
                // however, serialized size should remain the same
                assertThat(ClusterState.Builder.toBytes(clusterStateFromDiffs).length,
                    equalTo(ClusterState.Builder.toBytes(clusterState).length));
            } catch (AssertionError error) {
                logger.error("Cluster state:\n{}\nCluster state from diffs:\n{}",
                    clusterState.toString(), clusterStateFromDiffs.toString());
                throw error;
            }
        }

        logger.info("Final cluster state:[{}]", clusterState.toString());

    }

    private ClusterState.Builder randomCoordinationMetaData(ClusterState clusterState) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        CoordinationMetaData.Builder metaBuilder = CoordinationMetaData.builder(clusterState.coordinationMetaData());
        metaBuilder.term(randomNonNegativeLong());
        if (randomBoolean()) {
            metaBuilder.lastCommittedConfiguration(
                new CoordinationMetaData.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false))));
        }
        if (randomBoolean()) {
            metaBuilder.lastAcceptedConfiguration(
                new CoordinationMetaData.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false))));
        }
        if (randomBoolean()) {
            metaBuilder.addVotingConfigExclusion(new VotingConfigExclusion(randomNode("node-" + randomAlphaOfLength(10))));
        }
        return builder;
    }

    private DiscoveryNode randomNode(String nodeId) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), emptySet(), randomVersion(random()));
    }

    /**
     * Randomly updates nodes in the cluster state
     */
    private ClusterState.Builder randomNodes(ClusterState clusterState) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());
        List<String> nodeIds = randomSubsetOf(randomInt(clusterState.nodes().getNodes().size() - 1),
            clusterState.nodes().getNodes().keys().toArray(String.class));
        for (String nodeId : nodeIds) {
            if (nodeId.startsWith("node-")) {
                nodes.remove(nodeId);
                if (randomBoolean()) {
                    nodes.add(randomNode(nodeId));
                }
            }
        }
        int additionalNodeCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalNodeCount; i++) {
            nodes.add(randomNode("node-" + randomAlphaOfLength(10)));
        }
        return ClusterState.builder(clusterState).nodes(nodes);
    }

    /**
     * Randomly updates routing table in the cluster state
     */
    private ClusterState.Builder randomRoutingTable(ClusterState clusterState) {
        RoutingTable.Builder builder = RoutingTable.builder(clusterState.routingTable());
        int numberOfIndices = clusterState.routingTable().indicesRouting().size();
        if (numberOfIndices > 0) {
            List<String> randomIndices = randomSubsetOf(randomInt(numberOfIndices - 1),
                clusterState.routingTable().indicesRouting().keys().toArray(String.class));
            for (String index : randomIndices) {
                if (randomBoolean()) {
                    builder.remove(index);
                } else {
                    builder.add(randomChangeToIndexRoutingTable(clusterState.routingTable().indicesRouting().get(index),
                        clusterState.nodes().getNodes().keys().toArray(String.class)));
                }
            }
        }
        int additionalIndexCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalIndexCount; i++) {
            builder.add(randomIndexRoutingTable("index-" + randomInt(),
                clusterState.nodes().getNodes().keys().toArray(String.class)));
        }
        return ClusterState.builder(clusterState).routingTable(builder.build());
    }

    /**
     * Randomly updates index routing table in the cluster state
     */
    private IndexRoutingTable randomIndexRoutingTable(String index, String[] nodeIds) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(new Index(index, "_na_"));
        int shardCount = randomInt(10);

        for (int i = 0; i < shardCount; i++) {
            IndexShardRoutingTable.Builder indexShard = new IndexShardRoutingTable.Builder(new ShardId(index, "_na_", i));
            int replicaCount = randomIntBetween(1, 10);
            Set<String> availableNodeIds = Sets.newHashSet(nodeIds);
            for (int j = 0; j < replicaCount; j++) {
                UnassignedInfo unassignedInfo = null;
                if (randomInt(5) == 1) {
                    unassignedInfo = new UnassignedInfo(randomReason(), randomAlphaOfLength(10));
                }
                if (availableNodeIds.isEmpty()) {
                    break;
                }
                String nodeId = randomFrom(availableNodeIds);
                availableNodeIds.remove(nodeId);
                indexShard.addShard(
                        TestShardRouting.newShardRouting(index, i, nodeId, null, j == 0,
                                ShardRoutingState.fromValue((byte) randomIntBetween(2, 3)), unassignedInfo));
            }
            builder.addIndexShard(indexShard.build());
        }
        return builder.build();
    }

    /**
     * Randomly updates index routing table in the cluster state
     */
    private IndexRoutingTable randomChangeToIndexRoutingTable(IndexRoutingTable original, String[] nodes) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(original.getIndex());
        for (ObjectCursor<IndexShardRoutingTable> indexShardRoutingTable :  original.shards().values()) {
            Set<String> availableNodes = Sets.newHashSet(nodes);
            for (ShardRouting shardRouting : indexShardRoutingTable.value.shards()) {
                availableNodes.remove(shardRouting.currentNodeId());
                if (shardRouting.relocating()) {
                    availableNodes.remove(shardRouting.relocatingNodeId());
                }
            }

            for (ShardRouting shardRouting : indexShardRoutingTable.value.shards()) {
                final ShardRouting updatedShardRouting = randomChange(shardRouting, availableNodes);
                availableNodes.remove(updatedShardRouting.currentNodeId());
                if (shardRouting.relocating()) {
                    availableNodes.remove(updatedShardRouting.relocatingNodeId());
                }
                builder.addShard(updatedShardRouting);
            }
        }
        return builder.build();
    }

    /**
     * Randomly creates or removes cluster blocks
     */
    private ClusterState.Builder randomBlocks(ClusterState clusterState) {
        ClusterBlocks.Builder builder = ClusterBlocks.builder().blocks(clusterState.blocks());
        int globalBlocksCount = clusterState.blocks().global().size();
        if (globalBlocksCount > 0) {
            List<ClusterBlock> blocks = randomSubsetOf(randomInt(globalBlocksCount - 1),
                clusterState.blocks().global().toArray(new ClusterBlock[globalBlocksCount]));
            for (ClusterBlock block : blocks) {
                builder.removeGlobalBlock(block);
            }
        }
        int additionalGlobalBlocksCount = randomIntBetween(1, 3);
        for (int i = 0; i < additionalGlobalBlocksCount; i++) {
            builder.addGlobalBlock(randomGlobalBlock());
        }
        return ClusterState.builder(clusterState).blocks(builder);
    }

    /**
     * Returns a random global block
     */
    private ClusterBlock randomGlobalBlock() {
        switch (randomInt(2)) {
            case 0:
                return NoMasterBlockService.NO_MASTER_BLOCK_ALL;
            case 1:
                return NoMasterBlockService.NO_MASTER_BLOCK_WRITES;
            default:
                return GatewayService.STATE_NOT_RECOVERED_BLOCK;
        }
    }

    /**
     * Random cluster state part generator interface. Used by {@link #randomClusterStateParts(ClusterState, String, RandomClusterPart)}
     * method to update cluster state with randomly generated parts
     */
    private interface RandomClusterPart<T> {
        /**
         * Returns list of parts from metadata
         */
        ImmutableOpenMap<String, T> parts(ClusterState clusterState);

        /**
         * Puts the part back into metadata
         */
        ClusterState.Builder put(ClusterState.Builder builder, T part);

        /**
         * Remove the part from metadata
         */
        ClusterState.Builder remove(ClusterState.Builder builder, String name);

        /**
         * Returns a random part with the specified name
         */
        T randomCreate(String name);

        /**
         * Makes random modifications to the part
         */
        T randomChange(T part);

    }

    /**
     * Takes an existing cluster state and randomly adds, removes or updates a cluster state part using randomPart generator.
     * If a new part is added the prefix value is used as a prefix of randomly generated part name.
     */
    private <T> ClusterState randomClusterStateParts(ClusterState clusterState, String prefix, RandomClusterPart<T> randomPart) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        ImmutableOpenMap<String, T> parts = randomPart.parts(clusterState);
        int partCount = parts.size();
        if (partCount > 0) {
            List<String> randomParts = randomSubsetOf(randomInt(partCount - 1),
                randomPart.parts(clusterState).keys().toArray(String.class));
            for (String part : randomParts) {
                if (randomBoolean()) {
                    randomPart.remove(builder, part);
                } else {
                    randomPart.put(builder, randomPart.randomChange(parts.get(part)));
                }
            }
        }
        int additionalPartCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalPartCount; i++) {
            String name = randomName(prefix);
            randomPart.put(builder, randomPart.randomCreate(name));
        }
        return builder.build();
    }

    /**
     * Makes random metadata changes
     */
    private ClusterState.Builder randomMetaDataChanges(ClusterState clusterState) {
        MetaData metaData = clusterState.metaData();
        int changesCount = randomIntBetween(1, 10);
        for (int i = 0; i < changesCount; i++) {
            switch (randomInt(3)) {
                case 0:
                    metaData = randomMetaDataSettings(metaData);
                    break;
                case 1:
                    metaData = randomIndices(metaData);
                    break;
                case 2:
                    metaData = randomTemplates(metaData);
                    break;
                case 3:
                    metaData = randomMetaDataCustoms(metaData);
                    break;
                default:
                    throw new IllegalArgumentException("Shouldn't be here");
            }
        }
        return ClusterState.builder(clusterState).metaData(MetaData.builder(metaData).version(metaData.version() + 1).build());
    }

    /**
     * Makes random settings changes
     */
    private Settings randomSettings(Settings settings) {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put(settings);
        }
        int settingsCount = randomInt(10);
        for (int i = 0; i < settingsCount; i++) {
            builder.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        return builder.build();

    }

    /**
     * Randomly updates persistent or transient settings of the given metadata
     */
    private MetaData randomMetaDataSettings(MetaData metaData) {
        if (randomBoolean()) {
            return MetaData.builder(metaData).persistentSettings(randomSettings(metaData.persistentSettings())).build();
        } else {
            return MetaData.builder(metaData).transientSettings(randomSettings(metaData.transientSettings())).build();
        }
    }

    /**
     * Random metadata part generator
     */
    private interface RandomPart<T> {
        /**
         * Returns list of parts from metadata
         */
        ImmutableOpenMap<String, T> parts(MetaData metaData);

        /**
         * Puts the part back into metadata
         */
        MetaData.Builder put(MetaData.Builder builder, T part);

        /**
         * Remove the part from metadata
         */
        MetaData.Builder remove(MetaData.Builder builder, String name);

        /**
         * Returns a random part with the specified name
         */
        T randomCreate(String name);

        /**
         * Makes random modifications to the part
         */
        T randomChange(T part);

    }

    /**
     * Takes an existing cluster state and randomly adds, removes or updates a metadata part using randomPart generator.
     * If a new part is added the prefix value is used as a prefix of randomly generated part name.
     */
    private <T> MetaData randomParts(MetaData metaData, String prefix, RandomPart<T> randomPart) {
        MetaData.Builder builder = MetaData.builder(metaData);
        ImmutableOpenMap<String, T> parts = randomPart.parts(metaData);
        int partCount = parts.size();
        if (partCount > 0) {
            List<String> randomParts = randomSubsetOf(randomInt(partCount - 1),
                randomPart.parts(metaData).keys().toArray(String.class));
            for (String part : randomParts) {
                if (randomBoolean()) {
                    randomPart.remove(builder, part);
                } else {
                    randomPart.put(builder, randomPart.randomChange(parts.get(part)));
                }
            }
        }
        int additionalPartCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalPartCount; i++) {
            String name = randomName(prefix);
            randomPart.put(builder, randomPart.randomCreate(name));
        }
        return builder.build();
    }

    /**
     * Randomly add, deletes or updates indices in the metadata
     */
    private MetaData randomIndices(MetaData metaData) {
        return randomParts(metaData, "index", new RandomPart<IndexMetaData>() {

            @Override
            public ImmutableOpenMap<String, IndexMetaData> parts(MetaData metaData) {
                return metaData.indices();
            }

            @Override
            public MetaData.Builder put(MetaData.Builder builder, IndexMetaData part) {
                return builder.put(part, true);
            }

            @Override
            public MetaData.Builder remove(MetaData.Builder builder, String name) {
                return builder.remove(name);
            }

            @Override
            public IndexMetaData randomCreate(String name) {
                IndexMetaData.Builder builder = IndexMetaData.builder(name);
                Settings.Builder settingsBuilder = Settings.builder();
                setRandomIndexSettings(random(), settingsBuilder);
                settingsBuilder.put(randomSettings(Settings.EMPTY)).put(IndexMetaData.SETTING_VERSION_CREATED, randomVersion(random()));
                builder.settings(settingsBuilder);
                builder.numberOfShards(randomIntBetween(1, 10)).numberOfReplicas(randomInt(10));
                int aliasCount = randomInt(10);
                for (int i = 0; i < aliasCount; i++) {
                    builder.putAlias(randomAlias());
                }
                return builder.build();
            }

            @Override
            public IndexMetaData randomChange(IndexMetaData part) {
                IndexMetaData.Builder builder = IndexMetaData.builder(part);
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        builder.settings(Settings.builder().put(part.getSettings()).put(randomSettings(Settings.EMPTY)));
                        break;
                    case 1:
                        if (randomBoolean() && part.getAliases().isEmpty() == false) {
                            builder.removeAlias(randomFrom(part.getAliases().keys().toArray(String.class)));
                        } else {
                            builder.putAlias(AliasMetaData.builder(randomAlphaOfLength(10)));
                        }
                        break;
                    case 2:
                        builder.settings(Settings.builder().put(part.getSettings())
                            .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()));
                        break;
                    default:
                        throw new IllegalArgumentException("Shouldn't be here");
                }
                return builder.build();
            }
        });
    }

    /**
     * Randomly adds, deletes or updates index templates in the metadata
     */
    private MetaData randomTemplates(MetaData metaData) {
        return randomParts(metaData, "template", new RandomPart<IndexTemplateMetaData>() {
            @Override
            public ImmutableOpenMap<String, IndexTemplateMetaData> parts(MetaData metaData) {
                return metaData.templates();
            }

            @Override
            public MetaData.Builder put(MetaData.Builder builder, IndexTemplateMetaData part) {
                return builder.put(part);
            }

            @Override
            public MetaData.Builder remove(MetaData.Builder builder, String name) {
                return builder.removeTemplate(name);
            }

            @Override
            public IndexTemplateMetaData randomCreate(String name) {
                IndexTemplateMetaData.Builder builder = IndexTemplateMetaData.builder(name);
                builder.order(randomInt(1000))
                        .patterns(Collections.singletonList(randomName("temp")))
                        .settings(randomSettings(Settings.EMPTY));
                int aliasCount = randomIntBetween(0, 10);
                for (int i = 0; i < aliasCount; i++) {
                    builder.putAlias(randomAlias());
                }
                return builder.build();
            }

            @Override
            public IndexTemplateMetaData randomChange(IndexTemplateMetaData part) {
                IndexTemplateMetaData.Builder builder = new IndexTemplateMetaData.Builder(part);
                builder.order(randomInt(1000));
                return builder.build();
            }
        });
    }

    /**
     * Generates random alias
     */
    private AliasMetaData randomAlias() {
        AliasMetaData.Builder builder = newAliasMetaDataBuilder(randomName("alias"));
        if (randomBoolean()) {
            builder.filter(QueryBuilders.termQuery("test", randomRealisticUnicodeOfCodepointLength(10)).toString());
        }
        if (randomBoolean()) {
            builder.routing(randomAlphaOfLength(10));
        }
        return builder.build();
    }

    /**
     * Randomly adds, deletes or updates repositories in the metadata
     */
    private MetaData randomMetaDataCustoms(final MetaData metaData) {
        return randomParts(metaData, "custom", new RandomPart<MetaData.Custom>() {

            @Override
            public ImmutableOpenMap<String, MetaData.Custom> parts(MetaData metaData) {
                return metaData.customs();
            }

            @Override
            public MetaData.Builder put(MetaData.Builder builder, MetaData.Custom part) {
                return builder.putCustom(part.getWriteableName(), part);
            }

            @Override
            public MetaData.Builder remove(MetaData.Builder builder, String name) {
                if (IndexGraveyard.TYPE.equals(name)) {
                    // there must always be at least an empty graveyard
                    return builder.indexGraveyard(IndexGraveyard.builder().build());
                } else {
                    return builder.removeCustom(name);
                }
            }

            @Override
            public MetaData.Custom randomCreate(String name) {
                if (randomBoolean()) {
                    return new RepositoriesMetaData(Collections.emptyList());
                } else {
                    return IndexGraveyardTests.createRandom();
                }
            }

            @Override
            public MetaData.Custom randomChange(MetaData.Custom part) {
                return part;
            }
        });
    }

    /**
     * Randomly adds, deletes or updates in-progress snapshot and restore records in the cluster state
     */
    private ClusterState.Builder randomClusterStateCustoms(final ClusterState clusterState) {
        return ClusterState.builder(randomClusterStateParts(clusterState, "custom", new RandomClusterPart<ClusterState.Custom>() {

            @Override
            public ImmutableOpenMap<String, ClusterState.Custom> parts(ClusterState clusterState) {
                return clusterState.customs();
            }

            @Override
            public ClusterState.Builder put(ClusterState.Builder builder, ClusterState.Custom part) {
                return builder.putCustom(part.getWriteableName(), part);
            }

            @Override
            public ClusterState.Builder remove(ClusterState.Builder builder, String name) {
                return builder.removeCustom(name);
            }

            @Override
            public ClusterState.Custom randomCreate(String name) {
                switch (randomIntBetween(0, 1)) {
                    case 0:
                        return new SnapshotsInProgress(new SnapshotsInProgress.Entry(
                                new Snapshot(randomName("repo"), new SnapshotId(randomName("snap"), UUIDs.randomBase64UUID())),
                                randomBoolean(),
                                randomBoolean(),
                                SnapshotsInProgress.State.fromValue((byte) randomIntBetween(0, 6)),
                                Collections.emptyList(),
                                Math.abs(randomLong()),
                                (long) randomIntBetween(0, 1000),
                                ImmutableOpenMap.of(),
                                SnapshotInfoTests.randomUserMetadata(),
                                randomBoolean()));
                    case 1:
                        return new RestoreInProgress.Builder().add(
                            new RestoreInProgress.Entry(
                                UUIDs.randomBase64UUID(),
                                new Snapshot(randomName("repo"), new SnapshotId(randomName("snap"), UUIDs.randomBase64UUID())),
                                RestoreInProgress.State.fromValue((byte) randomIntBetween(0, 3)),
                                emptyList(),
                                ImmutableOpenMap.of())).build();
                    default:
                        throw new IllegalArgumentException("Shouldn't be here");
                }
            }

            @Override
            public ClusterState.Custom randomChange(ClusterState.Custom part)  {
                return part;
            }
        }));
    }

    /**
     * Generates a random name that starts with the given prefix
     */
    private String randomName(String prefix) {
        return prefix + UUIDs.randomBase64UUID(random());
    }
}
