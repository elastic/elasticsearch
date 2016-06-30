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
import org.elasticsearch.cluster.ClusterName;
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
import org.elasticsearch.index.shard.DocsStats;
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
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metaData(metaData).routingTable(routingTable).blocks(ClusterBlocks.builder().addBlocks(indexMetaData)).build();
        return clusterState;
    }

    public void testErrorCondition() {
        ClusterState state = createClusterState("source", randomIntBetween(2, 42), randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());
        assertTrue(
            expectThrows(IllegalStateException.class, () ->
            TransportShrinkAction.prepareCreateIndexRequest(new ShrinkRequest("target", "source"), state,
                (i) -> new DocsStats(Integer.MAX_VALUE, randomIntBetween(1, 1000)), new IndexNameExpressionResolver(Settings.EMPTY))
        ).getMessage().startsWith("Can't merge index with more than [2147483519] docs - too many documents in shards "));


        assertTrue(
            expectThrows(IllegalStateException.class, () -> {
                ShrinkRequest req = new ShrinkRequest("target", "source");
                req.getShrinkIndexRequest().settings(Settings.builder().put("index.number_of_shards", 4));
                ClusterState clusterState = createClusterState("source", 8, 1,
                    Settings.builder().put("index.blocks.write", true).build());
                    TransportShrinkAction.prepareCreateIndexRequest(req, clusterState,
                        (i) -> i == 2 || i == 3 ? new DocsStats(Integer.MAX_VALUE/2, randomIntBetween(1, 1000)) : null,
                        new IndexNameExpressionResolver(Settings.EMPTY));
                }
            ).getMessage().startsWith("Can't merge index with more than [2147483519] docs - too many documents in shards "));


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

        TransportShrinkAction.prepareCreateIndexRequest(new ShrinkRequest("target", "source"), clusterState,
            (i) -> new DocsStats(randomIntBetween(1, 1000), randomIntBetween(1, 1000)), new IndexNameExpressionResolver(Settings.EMPTY));
    }

    public void testShrinkIndexSettings() {
        String indexName = randomAsciiOfLength(10);
        // create one that won't fail
        ClusterState clusterState = ClusterState.builder(createClusterState(indexName, randomIntBetween(2, 10), 0,
            Settings.builder()
                .put("index.blocks.write", true)
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
        int numSourceShards = clusterState.metaData().index(indexName).getNumberOfShards();
        DocsStats stats = new DocsStats(randomIntBetween(0, (IndexWriter.MAX_DOCS) / numSourceShards), randomIntBetween(1, 1000));
        ShrinkRequest target = new ShrinkRequest("target", indexName);
        CreateIndexClusterStateUpdateRequest request = TransportShrinkAction.prepareCreateIndexRequest(
            target, clusterState, (i) -> stats,
            new IndexNameExpressionResolver(Settings.EMPTY));
        assertNotNull(request.shrinkFrom());
        assertEquals(indexName, request.shrinkFrom().getName());
        assertEquals("1", request.settings().get("index.number_of_shards"));
        assertEquals("shrink_index", request.cause());
    }

    private DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId, DummyTransportAddress.INSTANCE, emptyMap(),
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(DiscoveryNode.Role.MASTER, DiscoveryNode.Role.DATA))), Version.CURRENT);
    }
}
