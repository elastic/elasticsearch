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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.min;
import static org.hamcrest.Matchers.endsWith;

public class MetaDataCreateIndexServiceTests extends ESTestCase {

    private ClusterState createClusterState(String name, int numShards, int numReplicas, Settings settings) {
        int numRoutingShards = settings.getAsInt(IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.getKey(), numShards);
        MetaData.Builder metaBuilder = MetaData.builder();
        IndexMetaData indexMetaData = IndexMetaData.builder(name).settings(settings(Version.CURRENT)
            .put(settings))
            .numberOfShards(numShards).numberOfReplicas(numReplicas)
            .setRoutingNumShards(numRoutingShards).build();
        metaBuilder.put(indexMetaData, false);
        MetaData metaData = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metaData.index(name));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY))
            .metaData(metaData).routingTable(routingTable).blocks(ClusterBlocks.builder().addBlocks(indexMetaData)).build();
        return clusterState;
    }

    public static boolean isShrinkable(int source, int target) {
        int x = source / target;
        assert source > target : source  + " <= " + target;
        return target * x == source;
    }

    public static boolean isSplitable(int source, int target) {
        int x = target / source;
        assert source < target : source  + " >= " + target;
        return source * x == target;
    }

    public void testValidateShrinkIndex() {
        int numShards = randomIntBetween(2, 42);
        ClusterState state = createClusterState("source", numShards, randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());

        assertEquals("index [source] already exists",
            expectThrows(ResourceAlreadyExistsException.class, () ->
                MetaDataCreateIndexService.validateShrinkIndex(state, "target", Collections.emptySet(), "source", Settings.EMPTY)
            ).getMessage());

        assertEquals("no such index",
            expectThrows(IndexNotFoundException.class, () ->
                MetaDataCreateIndexService.validateShrinkIndex(state, "no such index", Collections.emptySet(), "target", Settings.EMPTY)
            ).getMessage());

        Settings targetSettings = Settings.builder().put("index.number_of_shards", 1).build();
        assertEquals("can't shrink an index with only one shard",
            expectThrows(IllegalArgumentException.class, () -> MetaDataCreateIndexService.validateShrinkIndex(createClusterState("source",
                1, 0, Settings.builder().put("index.blocks.write", true).build()), "source",
                Collections.emptySet(), "target", targetSettings)).getMessage());

        assertEquals("the number of target shards [10] must be less that the number of source shards [5]",
            expectThrows(IllegalArgumentException.class, () -> MetaDataCreateIndexService.validateShrinkIndex(createClusterState("source",
                5, 0, Settings.builder().put("index.blocks.write", true).build()), "source",
                Collections.emptySet(), "target", Settings.builder().put("index.number_of_shards", 10).build())).getMessage());


        assertEquals("index source must be read-only to resize index. use \"index.blocks.write=true\"",
            expectThrows(IllegalStateException.class, () ->
                    MetaDataCreateIndexService.validateShrinkIndex(
                        createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY)
                        , "source", Collections.emptySet(), "target", targetSettings)
            ).getMessage());

        assertEquals("index source must have all shards allocated on the same node to shrink index",
            expectThrows(IllegalStateException.class, () ->
                MetaDataCreateIndexService.validateShrinkIndex(state, "source", Collections.emptySet(), "target", targetSettings)

            ).getMessage());
        assertEquals("the number of source shards [8] must be a must be a multiple of [3]",
            expectThrows(IllegalArgumentException.class, () ->
                    MetaDataCreateIndexService.validateShrinkIndex(createClusterState("source", 8, randomIntBetween(0, 10),
                        Settings.builder().put("index.blocks.write", true).build()), "source", Collections.emptySet(), "target",
                        Settings.builder().put("index.number_of_shards", 3).build())
            ).getMessage());

        assertEquals("mappings are not allowed when resizing indices, all mappings are copied from the source index",
            expectThrows(IllegalArgumentException.class, () -> {
                MetaDataCreateIndexService.validateShrinkIndex(state, "source", Collections.singleton("foo"),
                    "target", targetSettings);
                }
            ).getMessage());

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState("source", numShards, 0,
            Settings.builder().put("index.blocks.write", true).build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(Settings.builder().build(), new AllocationDeciders(Settings.EMPTY,
            Collections.singleton(new MaxRetryAllocationDecider(Settings.EMPTY))),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = service.applyStartedShards(clusterState,
            routingTable.index("source").shardsWithState(ShardRoutingState.INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        int targetShards;
        do {
            targetShards = randomIntBetween(1, numShards/2);
        } while (isShrinkable(numShards, targetShards) == false);
        MetaDataCreateIndexService.validateShrinkIndex(clusterState, "source", Collections.emptySet(), "target",
            Settings.builder().put("index.number_of_shards", targetShards).build());
    }

    public void testValidateSplitIndex() {
        int numShards = randomIntBetween(1, 42);
        Settings targetSettings = Settings.builder().put("index.number_of_shards", numShards * 2).build();
        ClusterState state = createClusterState("source", numShards, randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());

        assertEquals("index [source] already exists",
            expectThrows(ResourceAlreadyExistsException.class, () ->
                MetaDataCreateIndexService.validateSplitIndex(state, "target", Collections.emptySet(), "source", targetSettings)
            ).getMessage());

        assertEquals("no such index",
            expectThrows(IndexNotFoundException.class, () ->
                MetaDataCreateIndexService.validateSplitIndex(state, "no such index", Collections.emptySet(), "target", targetSettings)
            ).getMessage());

        assertEquals("the number of source shards [10] must be less that the number of target shards [5]",
            expectThrows(IllegalArgumentException.class, () -> MetaDataCreateIndexService.validateSplitIndex(createClusterState("source",
                10, 0, Settings.builder().put("index.blocks.write", true).build()), "source", Collections.emptySet(),
                "target", Settings.builder().put("index.number_of_shards", 5).build())
            ).getMessage());


        assertEquals("index source must be read-only to resize index. use \"index.blocks.write=true\"",
            expectThrows(IllegalStateException.class, () ->
                MetaDataCreateIndexService.validateSplitIndex(
                    createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY)
                    , "source", Collections.emptySet(), "target", targetSettings)
            ).getMessage());


        assertEquals("the number of source shards [3] must be a must be a factor of [4]",
            expectThrows(IllegalArgumentException.class, () ->
                MetaDataCreateIndexService.validateSplitIndex(createClusterState("source", 3, randomIntBetween(0, 10),
                    Settings.builder().put("index.blocks.write", true).build()), "source", Collections.emptySet(), "target",
                    Settings.builder().put("index.number_of_shards", 4).build())
            ).getMessage());

        assertEquals("mappings are not allowed when resizing indices, all mappings are copied from the source index",
            expectThrows(IllegalArgumentException.class, () -> {
                    MetaDataCreateIndexService.validateSplitIndex(state, "source", Collections.singleton("foo"),
                        "target", targetSettings);
                }
            ).getMessage());

        int targetShards;
        do {
            targetShards = randomIntBetween(numShards+1, 100);
        } while (isSplitable(numShards, targetShards) == false);
        ClusterState clusterState = ClusterState.builder(createClusterState("source", numShards, 0,
            Settings.builder().put("index.blocks.write", true).put("index.number_of_routing_shards", targetShards).build()))
            .nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        AllocationService service = new AllocationService(Settings.builder().build(), new AllocationDeciders(Settings.EMPTY,
            Collections.singleton(new MaxRetryAllocationDecider(Settings.EMPTY))),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = service.applyStartedShards(clusterState,
            routingTable.index("source").shardsWithState(ShardRoutingState.INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        MetaDataCreateIndexService.validateSplitIndex(clusterState, "source", Collections.emptySet(), "target",
            Settings.builder().put("index.number_of_shards", targetShards).build());
    }

    public void testResizeIndexSettings() {
        String indexName = randomAlphaOfLength(10);
        List<Version> versions = Arrays.asList(VersionUtils.randomVersion(random()), VersionUtils.randomVersion(random()),
            VersionUtils.randomVersion(random()));
        versions.sort(Comparator.comparingLong(l -> l.id));
        Version version = versions.get(0);
        Version minCompat = versions.get(1);
        Version upgraded = versions.get(2);
        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState(indexName, randomIntBetween(2, 10), 0,
            Settings.builder()
                .put("index.blocks.write", true)
                .put("index.similarity.default.type", "BM25")
                .put("index.version.created", version)
                .put("index.version.upgraded", upgraded)
                .put("index.version.minimum_compatible", minCompat.luceneVersion.toString())
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "keyword")
                .build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(Settings.builder().build(), new AllocationDeciders(Settings.EMPTY,
            Collections.singleton(new MaxRetryAllocationDecider(Settings.EMPTY))),
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = service.applyStartedShards(clusterState,
            routingTable.index(indexName).shardsWithState(ShardRoutingState.INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        Settings.Builder builder = Settings.builder();
        builder.put("index.number_of_shards", 1);
        MetaDataCreateIndexService.prepareResizeIndexSettings(clusterState, Collections.emptySet(), builder,
            clusterState.metaData().index(indexName).getIndex(), "target", ResizeType.SHRINK);
        assertEquals("similarity settings must be copied", "BM25", builder.build().get("index.similarity.default.type"));
        assertEquals("analysis settings must be copied",
            "keyword", builder.build().get("index.analysis.analyzer.my_analyzer.tokenizer"));
        assertEquals("node1", builder.build().get("index.routing.allocation.initial_recovery._id"));
        assertEquals("1", builder.build().get("index.allocation.max_retries"));
        assertEquals(version, builder.build().getAsVersion("index.version.created", null));
        assertEquals(upgraded, builder.build().getAsVersion("index.version.upgraded", null));
    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(),
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(DiscoveryNode.Role.MASTER, DiscoveryNode.Role.DATA))), Version.CURRENT);
    }

    public void testValidateIndexName() throws Exception {

        validateIndexName("index?name", "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);

        validateIndexName("index#name", "must not contain '#'");

        validateIndexName("_indexname", "must not start with '_', '-', or '+'");
        validateIndexName("-indexname", "must not start with '_', '-', or '+'");
        validateIndexName("+indexname", "must not start with '_', '-', or '+'");

        validateIndexName("INDEXNAME", "must be lowercase");

        validateIndexName("..", "must not be '.' or '..'");

        MetaDataCreateIndexService.validateIndexName("foo:bar", ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING
                        .getDefault(Settings.EMPTY)).build());
        assertWarnings("index or alias name [foo:bar] containing ':' is deprecated and will not be supported in Elasticsearch 7.0+");
    }

    private void validateIndexName(String indexName, String errorMessage) {
        InvalidIndexNameException e = expectThrows(InvalidIndexNameException.class,
            () -> MetaDataCreateIndexService.validateIndexName(indexName, ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING
                .getDefault(Settings.EMPTY)).build()));
        assertThat(e.getMessage(), endsWith(errorMessage));
    }
}
