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
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexGraveyardTests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.snapshots.SnapshotsInProgressSerializationTests;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.VersionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.metadata.AliasMetadata.newAliasMetadataBuilder;
import static org.elasticsearch.cluster.routing.RandomShardRoutingMutator.randomChange;
import static org.elasticsearch.cluster.routing.UnassignedInfoTests.randomUnassignedInfo;
import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.elasticsearch.test.XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder;
import static org.elasticsearch.test.index.IndexVersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0, numClientNodes = 0)
public class ClusterStateDiffIT extends ESIntegTestCase {
    public void testClusterStateDiffSerialization() throws Exception {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        DiscoveryNode masterNode = randomNode("master");
        DiscoveryNode otherNode = randomNode("other");
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(masterNode).add(otherNode).localNodeId(masterNode.getId()).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoveryNodes)
            .putTransportVersion("master", TransportVersionUtils.randomVersion(random()))
            .putTransportVersion("other", TransportVersionUtils.randomVersion(random()))
            .build();
        ClusterState clusterStateFromDiffs = ClusterState.Builder.fromBytes(
            ClusterState.Builder.toBytes(clusterState),
            otherNode,
            namedWriteableRegistry
        );

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
                builder = switch (randomInt(5)) {
                    case 0 -> randomNodes(clusterState);
                    case 1 -> randomRoutingTable(clusterState);
                    case 2 -> randomBlocks(clusterState);
                    case 3 -> randomClusterStateCustoms(clusterState);
                    case 4 -> randomMetadataChanges(clusterState);
                    case 5 -> randomCoordinationMetadata(clusterState);
                    default -> throw new IllegalArgumentException("Shouldn't be here");
                };
            }
            clusterState = builder.incrementVersion().build();

            if (randomIntBetween(0, 10) < 1) {
                // Update cluster state via full serialization from time to time
                clusterStateFromDiffs = ClusterState.Builder.fromBytes(
                    ClusterState.Builder.toBytes(clusterState),
                    previousClusterStateFromDiffs.nodes().getLocalNode(),
                    namedWriteableRegistry
                );
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
                assertThat(clusterStateFromDiffs.coordinationMetadata(), equalTo(clusterState.coordinationMetadata()));

                // Check nodes
                assertThat(clusterStateFromDiffs.nodes().getNodes(), equalTo(clusterState.nodes().getNodes()));
                assertThat(clusterStateFromDiffs.nodes().getLocalNodeId(), equalTo(previousClusterStateFromDiffs.nodes().getLocalNodeId()));
                assertThat(clusterStateFromDiffs.nodes().getNodes(), equalTo(clusterState.nodes().getNodes()));
                for (Map.Entry<String, DiscoveryNode> node : clusterStateFromDiffs.nodes().getNodes().entrySet()) {
                    DiscoveryNode node1 = clusterState.nodes().get(node.getKey());
                    DiscoveryNode node2 = clusterStateFromDiffs.nodes().get(node.getKey());
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
                assertThat(
                    clusterStateFromDiffs.blocks().disableStatePersistence(),
                    equalTo(clusterStateFromDiffs.blocks().disableStatePersistence())
                );

                // Check metadata
                assertThat(clusterStateFromDiffs.metadata().version(), equalTo(clusterState.metadata().version()));
                assertThat(clusterStateFromDiffs.metadata().clusterUUID(), equalTo(clusterState.metadata().clusterUUID()));
                assertThat(clusterStateFromDiffs.metadata().transientSettings(), equalTo(clusterState.metadata().transientSettings()));
                assertThat(clusterStateFromDiffs.metadata().persistentSettings(), equalTo(clusterState.metadata().persistentSettings()));
                assertThat(clusterStateFromDiffs.metadata().indices(), equalTo(clusterState.metadata().indices()));
                assertThat(clusterStateFromDiffs.metadata().templates(), equalTo(clusterState.metadata().templates()));
                assertThat(clusterStateFromDiffs.metadata().customs(), equalTo(clusterState.metadata().customs()));
                assertThat(clusterStateFromDiffs.metadata().equalsAliases(clusterState.metadata()), is(true));

                // JSON Serialization test - make sure that both states produce similar JSON
                assertNull(differenceBetweenMapsIgnoringArrayOrder(convertToMap(clusterStateFromDiffs), convertToMap(clusterState)));

                // Smoke test - we cannot compare bytes to bytes because some elements might get serialized in different order
                // however, serialized size should remain the same
                assertThat(
                    ClusterState.Builder.toBytes(clusterStateFromDiffs).length,
                    equalTo(ClusterState.Builder.toBytes(clusterState).length)
                );
            } catch (AssertionError error) {
                logger.error(
                    "Cluster state:\n{}\nCluster state from diffs:\n{}",
                    clusterState.toString(),
                    clusterStateFromDiffs.toString()
                );
                throw error;
            }
        }

        logger.info("Final cluster state:[{}]", clusterState.toString());

    }

    private ClusterState.Builder randomCoordinationMetadata(ClusterState clusterState) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        CoordinationMetadata.Builder metaBuilder = CoordinationMetadata.builder(clusterState.coordinationMetadata());
        metaBuilder.term(randomNonNegativeLong());
        if (randomBoolean()) {
            metaBuilder.lastCommittedConfiguration(
                new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false)))
            );
        }
        if (randomBoolean()) {
            metaBuilder.lastAcceptedConfiguration(
                new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false)))
            );
        }
        if (randomBoolean()) {
            metaBuilder.addVotingConfigExclusion(new VotingConfigExclusion(randomNode("node-" + randomAlphaOfLength(10))));
        }
        return builder;
    }

    private DiscoveryNode randomNode(String nodeId) {
        Version nodeVersion = VersionUtils.randomVersion(random());
        IndexVersion indexVersion = randomVersion(random());
        return DiscoveryNodeUtils.builder(nodeId)
            .roles(emptySet())
            .version(nodeVersion, IndexVersion.fromId(indexVersion.id() - 1_000_000), indexVersion)
            .build();
    }

    /**
     * Randomly updates nodes in the cluster state
     */
    private ClusterState.Builder randomNodes(ClusterState clusterState) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());
        Map<String, TransportVersion> transports = new HashMap<>(clusterState.transportVersions());
        List<String> nodeIds = randomSubsetOf(
            randomInt(clusterState.nodes().getNodes().size() - 1),
            clusterState.nodes().getNodes().keySet().toArray(new String[0])
        );
        for (String nodeId : nodeIds) {
            if (nodeId.startsWith("node-")) {
                nodes.remove(nodeId);
                transports.remove(nodeId);
                if (randomBoolean()) {
                    nodes.add(randomNode(nodeId));
                    transports.put(nodeId, TransportVersionUtils.randomVersion(random()));
                }
            }
        }
        int additionalNodeCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalNodeCount; i++) {
            String id = "node-" + randomAlphaOfLength(10);
            nodes.add(randomNode(id));
            transports.put(id, TransportVersionUtils.randomVersion(random()));
        }

        return ClusterState.builder(clusterState).nodes(nodes).transportVersions(transports);
    }

    /**
     * Randomly updates routing table in the cluster state
     */
    private ClusterState.Builder randomRoutingTable(ClusterState clusterState) {
        RoutingTable.Builder builder = RoutingTable.builder(clusterState.routingTable());
        int numberOfIndices = clusterState.routingTable().indicesRouting().size();
        if (numberOfIndices > 0) {
            List<String> randomIndices = randomSubsetOf(
                randomInt(numberOfIndices - 1),
                clusterState.routingTable().indicesRouting().keySet().toArray(new String[0])
            );
            for (String index : randomIndices) {
                if (randomBoolean()) {
                    builder.remove(index);
                } else {
                    builder.add(
                        randomChangeToIndexRoutingTable(
                            clusterState.routingTable().indicesRouting().get(index),
                            clusterState.nodes().getNodes().keySet().toArray(new String[0])
                        )
                    );
                }
            }
        }
        int additionalIndexCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalIndexCount; i++) {
            builder.add(randomIndexRoutingTable("index-" + randomInt(), clusterState.nodes().getNodes().keySet().toArray(new String[0])));
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
                var newState = j == 0 ? ShardRoutingState.STARTED : randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED);
                var unassignedInfo = randomInt(5) == 1 && newState == ShardRoutingState.INITIALIZING
                    ? randomUnassignedInfo(randomAlphaOfLength(10))
                    : null;
                if (availableNodeIds.isEmpty()) {
                    break;
                }
                String nodeId = randomFrom(availableNodeIds);
                availableNodeIds.remove(nodeId);
                indexShard.addShard(TestShardRouting.newShardRouting(index, i, nodeId, null, j == 0, newState, unassignedInfo));
            }
            builder.addIndexShard(indexShard);
        }
        return builder.build();
    }

    /**
     * Randomly updates index routing table in the cluster state
     */
    private IndexRoutingTable randomChangeToIndexRoutingTable(IndexRoutingTable original, String[] nodes) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(original.getIndex());
        for (int i = 0; i < original.size(); i++) {
            IndexShardRoutingTable indexShardRoutingTable = original.shard(i);
            Set<String> availableNodes = Sets.newHashSet(nodes);
            for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = indexShardRoutingTable.shard(copy);
                availableNodes.remove(shardRouting.currentNodeId());
                if (shardRouting.relocating()) {
                    availableNodes.remove(shardRouting.relocatingNodeId());
                }
            }

            for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                ShardRouting shardRouting = indexShardRoutingTable.shard(copy);
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
            List<ClusterBlock> blocks = randomSubsetOf(
                randomInt(globalBlocksCount - 1),
                clusterState.blocks().global().toArray(new ClusterBlock[globalBlocksCount])
            );
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
        return switch (randomInt(2)) {
            case 0 -> NoMasterBlockService.NO_MASTER_BLOCK_ALL;
            case 1 -> NoMasterBlockService.NO_MASTER_BLOCK_WRITES;
            default -> GatewayService.STATE_NOT_RECOVERED_BLOCK;
        };
    }

    /**
     * Random cluster state part generator interface. Used by {@link #randomClusterStateParts(ClusterState, String, RandomClusterPart)}
     * method to update cluster state with randomly generated parts
     */
    private interface RandomClusterPart<T> {
        /**
         * Returns list of parts from metadata
         */
        Map<String, T> parts(ClusterState clusterState);

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
        Map<String, T> parts = randomPart.parts(clusterState);
        int partCount = parts.size();
        if (partCount > 0) {
            List<String> randomParts = randomSubsetOf(
                randomInt(partCount - 1),
                randomPart.parts(clusterState).keySet().toArray(new String[0])
            );
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
    private ClusterState.Builder randomMetadataChanges(ClusterState clusterState) {
        Metadata metadata = clusterState.metadata();
        int changesCount = randomIntBetween(1, 10);
        for (int i = 0; i < changesCount; i++) {
            metadata = switch (randomInt(3)) {
                case 0 -> randomMetadataSettings(metadata);
                case 1 -> randomIndices(metadata);
                case 2 -> randomTemplates(metadata);
                case 3 -> randomMetadataCustoms(metadata);
                default -> throw new IllegalArgumentException("Shouldn't be here");
            };
        }
        return ClusterState.builder(clusterState).metadata(Metadata.builder(metadata).version(metadata.version() + 1).build());
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
     * Updates persistent cluster settings of the given metadata
     */
    private Metadata randomMetadataSettings(Metadata metadata) {
        return Metadata.builder(metadata).persistentSettings(randomSettings(metadata.persistentSettings())).build();
    }

    /**
     * Random metadata part generator
     */
    private interface RandomPart<T> {
        /**
         * Returns list of parts from metadata
         */
        Map<String, T> parts(Metadata metadata);

        /**
         * Puts the part back into metadata
         */
        Metadata.Builder put(Metadata.Builder builder, T part);

        /**
         * Remove the part from metadata
         */
        Metadata.Builder remove(Metadata.Builder builder, String name);

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
    private <T> Metadata randomParts(Metadata metadata, String prefix, RandomPart<T> randomPart) {
        Metadata.Builder builder = Metadata.builder(metadata);
        Map<String, T> parts = randomPart.parts(metadata);
        int partCount = parts.size();
        if (partCount > 0) {
            List<String> randomParts = randomSubsetOf(randomInt(partCount - 1), randomPart.parts(metadata).keySet().toArray(new String[0]));
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
    private Metadata randomIndices(Metadata metadata) {
        return randomParts(metadata, "index", new RandomPart<IndexMetadata>() {

            @Override
            public Map<String, IndexMetadata> parts(Metadata metadata) {
                return metadata.indices();
            }

            @Override
            public Metadata.Builder put(Metadata.Builder builder, IndexMetadata part) {
                return builder.put(part, true);
            }

            @Override
            public Metadata.Builder remove(Metadata.Builder builder, String name) {
                return builder.remove(name);
            }

            @Override
            public IndexMetadata randomCreate(String name) {
                IndexMetadata.Builder builder = IndexMetadata.builder(name);
                Settings.Builder settingsBuilder = Settings.builder();
                setRandomIndexSettings(random(), settingsBuilder);
                settingsBuilder.put(randomSettings(Settings.EMPTY)).put(IndexMetadata.SETTING_VERSION_CREATED, randomVersion(random()));
                builder.settings(settingsBuilder);
                builder.numberOfShards(randomIntBetween(1, 10)).numberOfReplicas(randomInt(10));
                int aliasCount = randomInt(10);
                for (int i = 0; i < aliasCount; i++) {
                    builder.putAlias(randomAlias());
                }
                return builder.build();
            }

            @Override
            public IndexMetadata randomChange(IndexMetadata part) {
                IndexMetadata.Builder builder = IndexMetadata.builder(part);
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        builder.settings(Settings.builder().put(part.getSettings()).put(randomSettings(Settings.EMPTY)));
                        break;
                    case 1:
                        if (randomBoolean() && part.getAliases().isEmpty() == false) {
                            builder.removeAlias(randomFrom(part.getAliases().keySet().toArray(new String[0])));
                        } else {
                            builder.putAlias(AliasMetadata.builder(randomAlphaOfLength(10)));
                        }
                        break;
                    case 2:
                        builder.settings(Settings.builder().put(part.getSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
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
    private Metadata randomTemplates(Metadata metadata) {
        return randomParts(metadata, "template", new RandomPart<IndexTemplateMetadata>() {
            @Override
            public Map<String, IndexTemplateMetadata> parts(Metadata metadata) {
                return metadata.templates();
            }

            @Override
            public Metadata.Builder put(Metadata.Builder builder, IndexTemplateMetadata part) {
                return builder.put(part);
            }

            @Override
            public Metadata.Builder remove(Metadata.Builder builder, String name) {
                return builder.removeTemplate(name);
            }

            @Override
            public IndexTemplateMetadata randomCreate(String name) {
                IndexTemplateMetadata.Builder builder = IndexTemplateMetadata.builder(name);
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
            public IndexTemplateMetadata randomChange(IndexTemplateMetadata part) {
                IndexTemplateMetadata.Builder builder = new IndexTemplateMetadata.Builder(part);
                builder.order(randomInt(1000));
                return builder.build();
            }
        });
    }

    /**
     * Generates random alias
     */
    private AliasMetadata randomAlias() {
        AliasMetadata.Builder builder = newAliasMetadataBuilder(randomName("alias"));
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
    private Metadata randomMetadataCustoms(final Metadata metadata) {
        return randomParts(metadata, "custom", new RandomPart<Metadata.Custom>() {

            @Override
            public Map<String, Metadata.Custom> parts(Metadata metadata) {
                return metadata.customs();
            }

            @Override
            public Metadata.Builder put(Metadata.Builder builder, Metadata.Custom part) {
                return builder.putCustom(part.getWriteableName(), part);
            }

            @Override
            public Metadata.Builder remove(Metadata.Builder builder, String name) {
                if (IndexGraveyard.TYPE.equals(name)) {
                    // there must always be at least an empty graveyard
                    return builder.indexGraveyard(IndexGraveyard.builder().build());
                } else {
                    return builder.removeCustom(name);
                }
            }

            @Override
            public Metadata.Custom randomCreate(String name) {
                if (randomBoolean()) {
                    return new RepositoriesMetadata(Collections.emptyList());
                } else {
                    return IndexGraveyardTests.createRandom();
                }
            }

            @Override
            public Metadata.Custom randomChange(Metadata.Custom part) {
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
            public Map<String, ClusterState.Custom> parts(ClusterState clusterState) {
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
                return switch (randomIntBetween(0, 1)) {
                    case 0 -> SnapshotsInProgress.EMPTY.withAddedEntry(
                        SnapshotsInProgress.Entry.snapshot(
                            new Snapshot(randomName("repo"), new SnapshotId(randomName("snap"), UUIDs.randomBase64UUID())),
                            randomBoolean(),
                            randomBoolean(),
                            SnapshotsInProgressSerializationTests.randomState(ImmutableOpenMap.of()),
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Math.abs(randomLong()),
                            randomIntBetween(0, 1000),
                            ImmutableOpenMap.of(),
                            null,
                            SnapshotInfoTestUtils.randomUserMetadata(),
                            randomVersion(random())
                        )
                    );
                    case 1 -> new RestoreInProgress.Builder().add(
                        new RestoreInProgress.Entry(
                            UUIDs.randomBase64UUID(),
                            new Snapshot(randomName("repo"), new SnapshotId(randomName("snap"), UUIDs.randomBase64UUID())),
                            RestoreInProgress.State.fromValue((byte) randomIntBetween(0, 3)),
                            randomBoolean(),
                            emptyList(),
                            ImmutableOpenMap.of()
                        )
                    ).build();
                    default -> throw new IllegalArgumentException("Shouldn't be here");
                };
            }

            @Override
            public ClusterState.Custom randomChange(ClusterState.Custom part) {
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
