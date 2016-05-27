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

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.shrink.ShrinkRequest;
import org.elasticsearch.action.admin.indices.shrink.TransportShrinkAction;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.NoopGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static java.util.Collections.emptyMap;

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
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT)
            .metaData(metaData).routingTable(routingTable).blocks(ClusterBlocks.builder().addBlocks(indexMetaData)).build();
        return clusterState;
    }

    public void testValidateShrinkIndex() {
        ClusterState state = createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10),
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
            expectThrows(IllegalArgumentException.class, () ->
                    MetaDataCreateIndexService.validateShrinkIndex(createClusterState("source", 1, 0,
                        Settings.builder().put("index.blocks.write", true).build()), "source", Collections.emptySet(),
                        "target", Settings.EMPTY)
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

        assertEquals("can not shrink index into more than one shard",
            expectThrows(IllegalArgumentException.class, () ->
                MetaDataCreateIndexService.validateShrinkIndex(state, "source", Collections.emptySet(), "target",
                    Settings.builder().put("index.number_of_shards", 2).build())
            ).getMessage());

        assertEquals("mappings are not allowed when shrinking indices, all mappings are copied from the source index",
            expectThrows(IllegalArgumentException.class, () -> {
                MetaDataCreateIndexService.validateShrinkIndex(state, "source", Collections.singleton("foo"),
                    "target", Settings.EMPTY);
                }
            ).getMessage());

        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState("source", randomIntBetween(2, 10), 0,
            Settings.builder().put("index.blocks.write", true).build())).nodes(DiscoveryNodes.builder().put(newNode("node1")))
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

        MetaDataCreateIndexService.validateShrinkIndex(clusterState, "source", Collections.emptySet(), "target", Settings.EMPTY);
    }

    public void testShrinkIndexSettings() {
        String indexName = randomAsciiOfLength(10);
        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState(indexName, randomIntBetween(2, 10), 0,
            Settings.builder()
                .put("index.blocks.write", true)
                .put("index.similarity.default.type", "BM25")
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "keyword")
                .build())).nodes(DiscoveryNodes.builder().put(newNode("node1")))
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
        assertEquals("1", builder.build().get("index.number_of_shards"));
        assertEquals("similarity settings must be copied", "BM25", builder.build().get("index.similarity.default.type"));
        assertEquals("analysis settings must be copied",
            "keyword", builder.build().get("index.analysis.analyzer.my_analyzer.tokenizer"));
        assertEquals("node1", builder.build().get("index.routing.allocation.include._id"));
        assertEquals("1", builder.build().get("index.allocation.max_retries"));
    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId, DummyTransportAddress.INSTANCE, emptyMap(),
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(DiscoveryNode.Role.MASTER, DiscoveryNode.Role.DATA))), Version.CURRENT);
    }
}
