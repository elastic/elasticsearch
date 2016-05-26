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

package org.elasticsearch.action.admin.indices.shrink;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
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

public class TransportShrinkActionTests extends ESTestCase {

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

    public void testErrorCondition() {
        ClusterState state = createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());
        DocsStats stats = new DocsStats(randomIntBetween(0, IndexWriter.MAX_DOCS-1), randomIntBetween(1, 1000));

        assertEquals("index [source] already exists",
            expectThrows(IndexAlreadyExistsException.class, () ->
                TransportShrinkAction.prepareCreateIndexRequest(new ShrinkRequest("source", "source"), state, stats,
                    new IndexNameExpressionResolver(Settings.EMPTY))
            ).getMessage());

        assertEquals("no such index",
            expectThrows(IndexNotFoundException.class, () ->
                TransportShrinkAction.prepareCreateIndexRequest(new ShrinkRequest("target", "no such index"), state, stats,
                    new IndexNameExpressionResolver(Settings.EMPTY))
            ).getMessage());

        assertEquals("can't shrink an index with only one shard",
            expectThrows(IllegalArgumentException.class, () ->
                TransportShrinkAction.prepareCreateIndexRequest(new ShrinkRequest("target", "source"),
                    createClusterState("source", 1, 0, Settings.builder().put("index.blocks.write", true).build()),
                    stats, new IndexNameExpressionResolver(Settings.EMPTY))
            ).getMessage());

        assertEquals("Can't merge index with more than [2147483519] docs -  too many documents",
            expectThrows(IllegalStateException.class, () ->
            TransportShrinkAction.prepareCreateIndexRequest(new ShrinkRequest("target", "source"), state,
                new DocsStats(Integer.MAX_VALUE, randomIntBetween(1, 1000)), new IndexNameExpressionResolver(Settings.EMPTY))
        ).getMessage());

        assertEquals("index source must be read-only to shrink index. use \"index.blocks.write=true\"",
            expectThrows(IllegalStateException.class, () ->
                TransportShrinkAction.prepareCreateIndexRequest(new ShrinkRequest("target", "source"),
                    createClusterState("source", randomIntBetween(2, 100), randomIntBetween(0, 10), Settings.EMPTY),
                    stats, new IndexNameExpressionResolver(Settings.EMPTY))
            ).getMessage());

        assertEquals("index source must have all shards allocated on the same node to shrink index",
            expectThrows(IllegalStateException.class, () ->
                TransportShrinkAction.prepareCreateIndexRequest(new ShrinkRequest("target", "source"), state, stats,
                    new IndexNameExpressionResolver(Settings.EMPTY))
            ).getMessage());

        assertEquals("can not shrink index into more than one shard",
            expectThrows(IllegalArgumentException.class, () -> {
                ShrinkRequest shrinkRequest = new ShrinkRequest("target", "source");
                shrinkRequest.getShrinkIndexReqeust().settings(Settings.builder().put(randomBoolean()
                    ? "number_of_shards" : "index.number_of_shards", 2));
                TransportShrinkAction.prepareCreateIndexRequest(shrinkRequest, state, stats,
                    new IndexNameExpressionResolver(Settings.EMPTY));
                }
            ).getMessage());

        assertEquals("mappings are not allowed when shrinking indices, all mappings are copied from the source index",
            expectThrows(IllegalArgumentException.class, () -> {
                    ShrinkRequest shrinkRequest = new ShrinkRequest("target", "source");
                    shrinkRequest.getShrinkIndexReqeust().mapping("foo", "{}");
                    TransportShrinkAction.prepareCreateIndexRequest(shrinkRequest, state, stats,
                        new IndexNameExpressionResolver(Settings.EMPTY));
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

        TransportShrinkAction.prepareCreateIndexRequest(new ShrinkRequest("target", "source"), clusterState, stats,
            new IndexNameExpressionResolver(Settings.EMPTY));
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

        DocsStats stats = new DocsStats(randomIntBetween(0, IndexWriter.MAX_DOCS-1), randomIntBetween(1, 1000));
        ShrinkRequest target = new ShrinkRequest("target", indexName);
        CreateIndexClusterStateUpdateRequest request = TransportShrinkAction.prepareCreateIndexRequest(
            target, clusterState, stats, new IndexNameExpressionResolver(Settings.EMPTY));
        assertEquals(indexName, request.settings().get("index.shrink.source.name"));
        assertEquals("1", request.settings().get("index.number_of_shards"));
        assertEquals("0", request.settings().get("index.number_of_replicas"));
        assertEquals("similarity settings must be copied", "BM25", request.settings().get("index.similarity.default.type"));
        assertEquals("analysis settings must be copied",
            "keyword", request.settings().get("index.analysis.analyzer.my_analyzer.tokenizer"));
        assertEquals("node1", request.settings().get("index.routing.allocation.require._id"));
        assertEquals("shrink_index", request.cause());

        target.getShrinkIndexReqeust().settings(Settings.builder().put("number_of_replicas", 1));
        request = TransportShrinkAction.prepareCreateIndexRequest(
            target, clusterState, stats, new IndexNameExpressionResolver(Settings.EMPTY));
        assertEquals(indexName, request.settings().get("index.shrink.source.name"));
        assertEquals("1", request.settings().get("index.number_of_shards"));
        assertEquals("1", request.settings().get("index.number_of_replicas"));

        assertEquals("similarity settings must be copied", "BM25", request.settings().get("index.similarity.default.type"));
        assertEquals("analysis settings must be copied",
            "keyword", request.settings().get("index.analysis.analyzer.my_analyzer.tokenizer"));
        assertEquals("node1", request.settings().get("index.routing.allocation.require._id"));

    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId, DummyTransportAddress.INSTANCE, emptyMap(),
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(DiscoveryNode.Role.MASTER, DiscoveryNode.Role.DATA))), Version.CURRENT);
    }
}
