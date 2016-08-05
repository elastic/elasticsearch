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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.NoopGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.endsWith;

public class MetaDataCreateIndexServiceTests extends ESTestCase {

    private ClusterState createClusterState(String name, int numShards, int numReplicas, Settings settings) {
        MetaData.Builder metaBuilder = MetaData.builder();
        IndexMetaData indexMetaData = IndexMetaData.builder(name).settings(settings(Version.CURRENT)
            .put(settings))
            .numberOfShards(numShards).numberOfReplicas(numReplicas).build();
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

    public void testValidateShrinkIndex() {
        int numShards = randomIntBetween(2, 42);
        ClusterState state = createClusterState("source", numShards, randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());

        assertEquals("index [source] already exists",
            expectThrows(IndexAlreadyExistsException.class, () ->
                MetaDataCreateIndexService.validateShrinkIndex(state, "target", Collections.emptySet(), "source", Settings.EMPTY)
            ).getMessage());

        assertEquals("no such index",
            expectThrows(IndexNotFoundException.class, () ->
                MetaDataCreateIndexService.validateShrinkIndex(state, "no such index", Collections.emptySet(), "target", Settings.EMPTY)
            ).getMessage());

        assertEquals("can't shrink an index with only one shard",
            expectThrows(IllegalArgumentException.class, () -> MetaDataCreateIndexService.validateShrinkIndex(createClusterState("source",
                1, 0, Settings.builder().put("index.blocks.write", true).build()), "source", Collections.emptySet(),
                        "target", Settings.EMPTY)
            ).getMessage());

        assertEquals("the number of target shards must be less that the number of source shards",
            expectThrows(IllegalArgumentException.class, () -> MetaDataCreateIndexService.validateShrinkIndex(createClusterState("source",
                5, 0, Settings.builder().put("index.blocks.write", true).build()), "source", Collections.emptySet(),
                "target", Settings.builder().put("index.number_of_shards", 10).build())
            ).getMessage());


        assertEquals("index source must be read-only to shrink index. use \"index.blocks.write=true\"",
            expectThrows(IllegalStateException.class, () ->
                    MetaDataCreateIndexService.validateShrinkIndex(
                        createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY)
                        , "source", Collections.emptySet(), "target", Settings.EMPTY)
            ).getMessage());

        assertEquals("index source must have all shards allocated on the same node to shrink index",
            expectThrows(IllegalStateException.class, () ->
                MetaDataCreateIndexService.validateShrinkIndex(state, "source", Collections.emptySet(), "target", Settings.EMPTY)

            ).getMessage());
        assertEquals("the number of source shards [8] must be a must be a multiple of [3]",
            expectThrows(IllegalArgumentException.class, () ->
                    MetaDataCreateIndexService.validateShrinkIndex(createClusterState("source", 8, randomIntBetween(0, 10),
                        Settings.builder().put("index.blocks.write", true).build()), "source", Collections.emptySet(), "target",
                        Settings.builder().put("index.number_of_shards", 3).build())
            ).getMessage());

        assertEquals("mappings are not allowed when shrinking indices, all mappings are copied from the source index",
            expectThrows(IllegalArgumentException.class, () -> {
                MetaDataCreateIndexService.validateShrinkIndex(state, "source", Collections.singleton("foo"),
                    "target", Settings.EMPTY);
                }
            ).getMessage());

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState("source", numShards, 0,
            Settings.builder().put("index.blocks.write", true).build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(Settings.builder().build(), new AllocationDeciders(Settings.EMPTY,
            Collections.singleton(new MaxRetryAllocationDecider(Settings.EMPTY))),
            NoopGatewayAllocator.INSTANCE, new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);

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

    public void testShrinkIndexSettings() {
        String indexName = randomAsciiOfLength(10);
        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState(indexName, randomIntBetween(2, 10), 0,
            Settings.builder()
                .put("index.blocks.write", true)
                .put("index.similarity.default.type", "BM25")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "keyword")
                .build())).nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();
        AllocationService service = new AllocationService(Settings.builder().build(), new AllocationDeciders(Settings.EMPTY,
            Collections.singleton(new MaxRetryAllocationDecider(Settings.EMPTY))),
            NoopGatewayAllocator.INSTANCE, new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);

        RoutingTable routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we start the shard
        routingTable = service.applyStartedShards(clusterState,
            routingTable.index(indexName).shardsWithState(ShardRoutingState.INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        Settings.Builder builder = Settings.builder();
        MetaDataCreateIndexService.prepareShrinkIndexSettings(
            clusterState, Collections.emptySet(), builder, clusterState.metaData().index(indexName).getIndex(), "target");
        assertEquals("similarity settings must be copied", "BM25", builder.build().get("index.similarity.default.type"));
        assertEquals("analysis settings must be copied",
            "keyword", builder.build().get("index.analysis.analyzer.my_analyzer.tokenizer"));
        assertEquals("node1", builder.build().get("index.routing.allocation.initial_recovery._id"));
        assertEquals("1", builder.build().get("index.allocation.max_retries"));
    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId, LocalTransportAddress.buildUnique(), emptyMap(),
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(DiscoveryNode.Role.MASTER, DiscoveryNode.Role.DATA))), Version.CURRENT);
    }

    public void testValidateIndexName() throws Exception {

        validateIndexName("index?name", "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);

        validateIndexName("index#name", "must not contain '#'");

        validateIndexName("_indexname", "must not start with '_'");

        validateIndexName("INDEXNAME", "must be lowercase");

        validateIndexName("..", "must not be '.' or '..'");

    }

    private void validateIndexName(String indexName, String errorMessage) {
        InvalidIndexNameException e = expectThrows(InvalidIndexNameException.class,
            () -> getCreateIndexService().validateIndexName(indexName, ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING
                .getDefault(Settings.EMPTY)).build()));
        assertThat(e.getMessage(), endsWith(errorMessage));
    }

    private MetaDataCreateIndexService getCreateIndexService() {
        return new MetaDataCreateIndexService(
            Settings.EMPTY,
            null,
            null,
            null,
            null,
            new HashSet<>(),
            null,
            null,
            null,
            null);
    }
}
