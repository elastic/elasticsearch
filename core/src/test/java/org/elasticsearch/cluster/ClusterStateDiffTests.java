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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.List;

import static org.elasticsearch.cluster.metadata.AliasMetaData.newAliasMetaDataBuilder;
import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.elasticsearch.test.XContentTestUtils.mapsEqualIgnoringArrayOrder;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;


@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 0, numClientNodes = 0)
public class ClusterStateDiffTests extends ElasticsearchIntegrationTest {

    @Test
    public void testClusterStateDiffSerialization() throws Exception {
        DiscoveryNode masterNode = new DiscoveryNode("master", new LocalTransportAddress("master"), Version.CURRENT);
        DiscoveryNode otherNode = new DiscoveryNode("other", new LocalTransportAddress("other"), Version.CURRENT);
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().put(masterNode).put(otherNode).localNodeId(masterNode.id()).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        ClusterState clusterStateFromDiffs = ClusterState.Builder.fromBytes(ClusterState.Builder.toBytes(clusterState), otherNode);

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
                switch (randomInt(4)) {
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
                    case 4:
                        builder = randomMetaDataChanges(clusterState);
                        break;
                    default:
                        throw new IllegalArgumentException("Shouldn't be here");
                }
            }
            clusterState = builder.incrementVersion().build();

            if (randomIntBetween(0, 10) < 1) {
                // Update cluster state via full serialization from time to time
                clusterStateFromDiffs = ClusterState.Builder.fromBytes(ClusterState.Builder.toBytes(clusterState), previousClusterStateFromDiffs.nodes().localNode());
            } else {
                // Update cluster states using diffs
                Diff<ClusterState> diffBeforeSerialization = clusterState.diff(previousClusterState);
                BytesStreamOutput os = new BytesStreamOutput();
                diffBeforeSerialization.writeTo(os);
                byte[] diffBytes = os.bytes().toBytes();
                Diff<ClusterState> diff;
                try (StreamInput input = StreamInput.wrap(diffBytes)) {
                    diff = previousClusterStateFromDiffs.readDiffFrom(input);
                    clusterStateFromDiffs = diff.apply(previousClusterStateFromDiffs);
                }
            }


            try {
                // Check non-diffable elements
                assertThat(clusterStateFromDiffs.version(), equalTo(clusterState.version()));
                assertThat(clusterStateFromDiffs.uuid(), equalTo(clusterState.uuid()));

                // Check nodes
                assertThat(clusterStateFromDiffs.nodes().nodes(), equalTo(clusterState.nodes().nodes()));
                assertThat(clusterStateFromDiffs.nodes().localNodeId(), equalTo(previousClusterStateFromDiffs.nodes().localNodeId()));
                assertThat(clusterStateFromDiffs.nodes().nodes(), equalTo(clusterState.nodes().nodes()));
                for (ObjectCursor<String> node : clusterStateFromDiffs.nodes().nodes().keys()) {
                    DiscoveryNode node1 = clusterState.nodes().get(node.value);
                    DiscoveryNode node2 = clusterStateFromDiffs.nodes().get(node.value);
                    assertThat(node1.version(), equalTo(node2.version()));
                    assertThat(node1.address(), equalTo(node2.address()));
                    assertThat(node1.attributes(), equalTo(node2.attributes()));
                }

                // Check routing table
                assertThat(clusterStateFromDiffs.routingTable().version(), equalTo(clusterState.routingTable().version()));
                assertThat(clusterStateFromDiffs.routingTable().indicesRouting(), equalTo(clusterState.routingTable().indicesRouting()));

                // Check cluster blocks
                assertThat(clusterStateFromDiffs.blocks().global(), equalTo(clusterStateFromDiffs.blocks().global()));
                assertThat(clusterStateFromDiffs.blocks().indices(), equalTo(clusterStateFromDiffs.blocks().indices()));
                assertThat(clusterStateFromDiffs.blocks().disableStatePersistence(), equalTo(clusterStateFromDiffs.blocks().disableStatePersistence()));

                // Check metadata
                assertThat(clusterStateFromDiffs.metaData().version(), equalTo(clusterState.metaData().version()));
                assertThat(clusterStateFromDiffs.metaData().uuid(), equalTo(clusterState.metaData().uuid()));
                assertThat(clusterStateFromDiffs.metaData().transientSettings(), equalTo(clusterState.metaData().transientSettings()));
                assertThat(clusterStateFromDiffs.metaData().persistentSettings(), equalTo(clusterState.metaData().persistentSettings()));
                assertThat(clusterStateFromDiffs.metaData().indices(), equalTo(clusterState.metaData().indices()));
                assertThat(clusterStateFromDiffs.metaData().templates(), equalTo(clusterState.metaData().templates()));
                assertThat(clusterStateFromDiffs.metaData().customs(), equalTo(clusterState.metaData().customs()));
                assertThat(clusterStateFromDiffs.metaData().aliases(), equalTo(clusterState.metaData().aliases()));

                // JSON Serialization test - make sure that both states produce similar JSON
                assertThat(mapsEqualIgnoringArrayOrder(convertToMap(clusterStateFromDiffs), convertToMap(clusterState)), equalTo(true));

                // Smoke test - we cannot compare bytes to bytes because some elements might get serialized in different order
                // however, serialized size should remain the same
                assertThat(ClusterState.Builder.toBytes(clusterStateFromDiffs).length, equalTo(ClusterState.Builder.toBytes(clusterState).length));
            } catch (AssertionError error) {
                logger.error("Cluster state:\n{}\nCluster state from diffs:\n{}", clusterState.toString(), clusterStateFromDiffs.toString());
                throw error;
            }
        }

        logger.info("Final cluster state:[{}]", clusterState.toString());

    }

    private ClusterState.Builder randomNodes(ClusterState clusterState) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());
        List<String> nodeIds = randomSubsetOf(randomInt(clusterState.nodes().nodes().size() - 1), clusterState.nodes().nodes().keys().toArray(String.class));
        for (String nodeId : nodeIds) {
            if (nodeId.startsWith("node-")) {
                if (randomBoolean()) {
                    nodes.remove(nodeId);
                } else {
                    nodes.put(new DiscoveryNode(nodeId, new LocalTransportAddress(randomAsciiOfLength(10)), randomVersion(random())));
                }
            }
        }
        int additionalNodeCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalNodeCount; i++) {
            nodes.put(new DiscoveryNode("node-" + randomAsciiOfLength(10), new LocalTransportAddress(randomAsciiOfLength(10)), randomVersion(random())));
        }
        return ClusterState.builder(clusterState).nodes(nodes);
    }

    private ClusterState.Builder randomRoutingTable(ClusterState clusterState) {
        RoutingTable.Builder builder = RoutingTable.builder(clusterState.routingTable());
        int numberOfIndices = clusterState.routingTable().indicesRouting().size();
        if (numberOfIndices > 0) {
            List<String> randomIndices = randomSubsetOf(randomInt(numberOfIndices - 1), clusterState.routingTable().indicesRouting().keySet().toArray(new String[numberOfIndices]));
            for (String index : randomIndices) {
                if (randomBoolean()) {
                    builder.remove(index);
                } else {
                    builder.add(randomIndexRoutingTable(index, clusterState.nodes().nodes().keys().toArray(String.class)));
                }
            }
        }
        int additionalIndexCount = randomIntBetween(1, 20);
        for (int i = 0; i < additionalIndexCount; i++) {
            builder.add(randomIndexRoutingTable("index-" + randomInt(), clusterState.nodes().nodes().keys().toArray(String.class)));
        }
        return ClusterState.builder(clusterState).routingTable(builder.build());
    }

    private IndexRoutingTable randomIndexRoutingTable(String index, String[] nodeIds) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        int shardCount = randomInt(10);

        for (int i = 0; i < shardCount; i++) {
            IndexShardRoutingTable.Builder indexShard = new IndexShardRoutingTable.Builder(new ShardId(index, i), randomBoolean());
            int replicaCount = randomIntBetween(1, 10);
            for (int j = 0; j < replicaCount; j++) {
                indexShard.addShard(
                        new MutableShardRouting(index, i, randomFrom(nodeIds), j == 0, ShardRoutingState.fromValue((byte) randomIntBetween(1, 4)), 1));
            }
            builder.addIndexShard(indexShard.build());
        }
        return builder.build();
    }

    private ClusterState.Builder randomBlocks(ClusterState clusterState) {
        ClusterBlocks.Builder builder = ClusterBlocks.builder().blocks(clusterState.blocks());
        int globalBlocksCount = clusterState.blocks().global().size();
        if (globalBlocksCount > 0) {
            List<ClusterBlock> blocks = randomSubsetOf(randomInt(globalBlocksCount - 1), clusterState.blocks().global().toArray(new ClusterBlock[globalBlocksCount]));
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

    private ClusterBlock randomGlobalBlock() {
        switch (randomInt(2)) {
            case 0:
                return DiscoverySettings.NO_MASTER_BLOCK_ALL;
            case 1:
                return DiscoverySettings.NO_MASTER_BLOCK_WRITES;
            default:
                return GatewayService.STATE_NOT_RECOVERED_BLOCK;
        }
    }

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

    private Settings randomSettings(Settings settings) {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put(settings);
        }
        int settingsCount = randomInt(10);
        for (int i = 0; i < settingsCount; i++) {
            builder.put(randomAsciiOfLength(10), randomAsciiOfLength(10));
        }
        return builder.build();

    }

    private MetaData randomMetaDataSettings(MetaData metaData) {
        if (randomBoolean()) {
            return MetaData.builder(metaData).persistentSettings(randomSettings(metaData.persistentSettings())).build();
        } else {
            return MetaData.builder(metaData).transientSettings(randomSettings(metaData.transientSettings())).build();
        }
    }

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

    private <T> MetaData randomParts(MetaData metaData, String prefix, RandomPart<T> randomPart) {
        MetaData.Builder builder = MetaData.builder(metaData);
        ImmutableOpenMap<String, T> parts = randomPart.parts(metaData);
        int partCount = parts.size();
        if (partCount > 0) {
            List<String> randomParts = randomSubsetOf(randomInt(partCount - 1), randomPart.parts(metaData).keys().toArray(String.class));
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
                setRandomSettings(getRandom(), settingsBuilder);
                settingsBuilder.put(randomSettings(Settings.EMPTY)).put(IndexMetaData.SETTING_VERSION_CREATED, randomVersion(random()));
                builder.settings(settingsBuilder);
                builder.numberOfShards(randomIntBetween(1, 10)).numberOfReplicas(randomInt(10));
                int aliasCount = randomInt(10);
                if (randomBoolean()) {
                    builder.putCustom(IndexWarmersMetaData.TYPE, randomWarmers());
                }
                for (int i = 0; i < aliasCount; i++) {
                    builder.putAlias(randomAlias());
                }
                return builder.build();
            }

            @Override
            public IndexMetaData randomChange(IndexMetaData part) {
                IndexMetaData.Builder builder = IndexMetaData.builder(part);
                switch (randomIntBetween(0, 3)) {
                    case 0:
                        builder.settings(Settings.builder().put(part.settings()).put(randomSettings(Settings.EMPTY)));
                        break;
                    case 1:
                        if (randomBoolean() && part.aliases().isEmpty() == false) {
                            builder.removeAlias(randomFrom(part.aliases().keys().toArray(String.class)));
                        } else {
                            builder.putAlias(AliasMetaData.builder(randomAsciiOfLength(10)));
                        }
                        break;
                    case 2:
                        builder.settings(Settings.builder().put(part.settings()).put(IndexMetaData.SETTING_UUID, Strings.randomBase64UUID()));
                        break;
                    case 3:
                        builder.putCustom(IndexWarmersMetaData.TYPE, randomWarmers());
                        break;
                    default:
                        throw new IllegalArgumentException("Shouldn't be here");
                }
                return builder.build();
            }
        });
    }

    private IndexWarmersMetaData randomWarmers() {
        if (randomBoolean()) {
            return new IndexWarmersMetaData(
                    new IndexWarmersMetaData.Entry(
                            randomName("warm"),
                            new String[]{randomName("type")},
                            randomBoolean(),
                            new BytesArray(randomAsciiOfLength(1000)))
            );
        } else {
            return new IndexWarmersMetaData();
        }
    }

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
                        .template(randomName("temp"))
                        .settings(randomSettings(Settings.EMPTY));
                int aliasCount = randomIntBetween(0, 10);
                for (int i = 0; i < aliasCount; i++) {
                    builder.putAlias(randomAlias());
                }
                if (randomBoolean()) {
                    builder.putCustom(IndexWarmersMetaData.TYPE, randomWarmers());
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

    private AliasMetaData randomAlias() {
        AliasMetaData.Builder builder = newAliasMetaDataBuilder(randomName("alias"));
        if (randomBoolean()) {
            builder.filter(QueryBuilders.termQuery("test", randomRealisticUnicodeOfCodepointLength(10)).toString());
        }
        if (randomBoolean()) {
            builder.routing(randomAsciiOfLength(10));
        }
        return builder.build();
    }

    private MetaData randomMetaDataCustoms(final MetaData metaData) {
        return randomParts(metaData, "custom", new RandomPart<MetaData.Custom>() {

            @Override
            public ImmutableOpenMap<String, MetaData.Custom> parts(MetaData metaData) {
                return metaData.customs();
            }

            @Override
            public MetaData.Builder put(MetaData.Builder builder, MetaData.Custom part) {
                if (part instanceof SnapshotMetaData) {
                    return builder.putCustom(SnapshotMetaData.TYPE, part);
                } else if (part instanceof RepositoriesMetaData) {
                    return builder.putCustom(RepositoriesMetaData.TYPE, part);
                } else if (part instanceof RestoreMetaData) {
                    return builder.putCustom(RestoreMetaData.TYPE, part);
                }
                throw new IllegalArgumentException("Unknown custom part " + part);
            }

            @Override
            public MetaData.Builder remove(MetaData.Builder builder, String name) {
                return builder.removeCustom(name);
            }

            @Override
            public MetaData.Custom randomCreate(String name) {
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        return new SnapshotMetaData(new SnapshotMetaData.Entry(
                                new SnapshotId(randomName("repo"), randomName("snap")),
                                randomBoolean(),
                                SnapshotMetaData.State.fromValue((byte) randomIntBetween(0, 6)),
                                ImmutableList.<String>of(),
                                Math.abs(randomLong()),
                                ImmutableMap.<ShardId, SnapshotMetaData.ShardSnapshotStatus>of()));
                    case 1:
                        return new RepositoriesMetaData();
                    case 2:
                        return new RestoreMetaData(new RestoreMetaData.Entry(
                                new SnapshotId(randomName("repo"), randomName("snap")),
                                RestoreMetaData.State.fromValue((byte) randomIntBetween(0, 3)),
                                ImmutableList.<String>of(),
                                ImmutableMap.<ShardId, RestoreMetaData.ShardRestoreStatus>of()));
                    default:
                        throw new IllegalArgumentException("Shouldn't be here");
                }
            }

            @Override
            public MetaData.Custom randomChange(MetaData.Custom part) {
                return part;
            }
        });
    }

    private String randomName(String prefix) {
        return prefix + Strings.randomBase64UUID(getRandom());
    }
}