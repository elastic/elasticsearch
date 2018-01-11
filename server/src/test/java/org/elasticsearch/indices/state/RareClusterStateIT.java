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

package org.elasticsearch.indices.state;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0)
@TestLogging("_root:DEBUG")
public class RareClusterStateIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false).build();
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testUnassignedShardAndEmptyNodesInRoutingTable() throws Exception {
        internalCluster().startNode();
        createIndex("a");
        ensureSearchable("a");
        ClusterState current = clusterService().state();
        GatewayAllocator allocator = internalCluster().getInstance(GatewayAllocator.class);

        AllocationDeciders allocationDeciders = new AllocationDeciders(Settings.EMPTY, Collections.emptyList());
        RoutingNodes routingNodes = new RoutingNodes(
                ClusterState.builder(current)
                        .routingTable(RoutingTable.builder(current.routingTable()).remove("a").addAsRecovery(current.metaData().index("a")).build())
                        .nodes(DiscoveryNodes.EMPTY_NODES)
                        .build(), false
        );
        RoutingAllocation routingAllocation = new RoutingAllocation(allocationDeciders, routingNodes, current, ClusterInfo.EMPTY, System.nanoTime());
        allocator.allocateUnassigned(routingAllocation);
    }

    public void testAssignmentWithJustAddedNodes() throws Exception {
        internalCluster().startNode();
        final String index = "index";
        prepareCreate(index).setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).get();
        ensureGreen(index);

        // close to have some unassigned started shards shards..
        client().admin().indices().prepareClose(index).get();


        final String masterName = internalCluster().getMasterName();
        final ClusterService clusterService = internalCluster().clusterService(masterName);
        final AllocationService allocationService = internalCluster().getInstance(AllocationService.class, masterName);
        clusterService.submitStateUpdateTask("test-inject-node-and-reroute", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                // inject a node
                ClusterState.Builder builder = ClusterState.builder(currentState);
                builder.nodes(DiscoveryNodes.builder(currentState.nodes()).add(new DiscoveryNode("_non_existent",
                        buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT)));

                // open index
                final IndexMetaData indexMetaData = IndexMetaData.builder(currentState.metaData().index(index)).state(IndexMetaData.State.OPEN).build();

                builder.metaData(MetaData.builder(currentState.metaData()).put(indexMetaData, true));
                builder.blocks(ClusterBlocks.builder().blocks(currentState.blocks()).removeIndexBlocks(index));
                ClusterState updatedState = builder.build();

                RoutingTable.Builder routingTable = RoutingTable.builder(updatedState.routingTable());
                routingTable.addAsRecovery(updatedState.metaData().index(index));
                updatedState = ClusterState.builder(updatedState).routingTable(routingTable.build()).build();

                return allocationService.reroute(updatedState, "reroute");

            }

            @Override
            public void onFailure(String source, Exception e) {

            }
        });
        ensureGreen(index);
        // remove the extra node
        clusterService.submitStateUpdateTask("test-remove-injected-node", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ClusterState.Builder builder = ClusterState.builder(currentState);
                builder.nodes(DiscoveryNodes.builder(currentState.nodes()).remove("_non_existent"));

                currentState = builder.build();
                return allocationService.deassociateDeadNodes(currentState, true, "reroute");

            }

            @Override
            public void onFailure(String source, Exception e) {

            }
        });
    }

    public void testDeleteCreateInOneBulk() throws Exception {
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut());
        prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).addMapping("type").get();
        ensureGreen("test");

        // now that the cluster is stable, remove publishing timeout
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "0")
                .put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "30s")));

        // block none master node.
        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(dataNode, random());
        internalCluster().setDisruptionScheme(disruption);
        logger.info("--> indexing a doc");
        index("test", "type", "1");
        refresh();
        disruption.startDisrupting();
        logger.info("--> delete index and recreate it");
        assertFalse(client().admin().indices().prepareDelete("test").setTimeout("200ms").get().isAcknowledged());
        assertFalse(prepareCreate("test").setTimeout("200ms").setSettings(Settings.builder().put(IndexMetaData
                .SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "0")).get().isAcknowledged());
        logger.info("--> letting cluster proceed");
        disruption.stopDisrupting();
        ensureGreen(TimeValue.timeValueMinutes(30), "test");
        assertHitCount(client().prepareSearch("test").get(), 0);
    }

    public void testDelayedMappingPropagationOnPrimary() throws Exception {
        // Here we want to test that things go well if there is a first request
        // that adds mappings but before mappings are propagated to all nodes
        // another index request introduces the same mapping. The master node
        // will reply immediately since it did not change the cluster state
        // but the change might not be on the node that performed the indexing
        // operation yet

        Settings settings = Settings.builder()
            .put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "30s") // explicitly set so it won't default to publish timeout
            .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "0s") // don't wait post commit as we are blocking things by design
            .build();
        final List<String> nodeNames = internalCluster().startNodes(2, settings);
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut());

        final String master = internalCluster().getMasterName();
        assertThat(nodeNames, hasItem(master));
        String otherNode = null;
        for (String node : nodeNames) {
            if (node.equals(master) == false) {
                otherNode = node;
                break;
            }
        }
        assertNotNull(otherNode);

        // Don't allocate the shard on the master node
        assertAcked(prepareCreate("index").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.exclude._name", master)).get());
        ensureGreen();

        // Check routing tables
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertEquals(master, state.nodes().getMasterNode().getName());
        List<ShardRouting> shards = state.routingTable().allShards("index");
        assertThat(shards, hasSize(1));
        for (ShardRouting shard : shards) {
            if (shard.primary()) {
                // primary must not be on the master node
                assertFalse(state.nodes().getMasterNodeId().equals(shard.currentNodeId()));
            } else {
                fail(); // only primaries
            }
        }

        // Block cluster state processing where our shard is
        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(otherNode, random());
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        // Add a new mapping...
        final AtomicReference<Object> putMappingResponse = new AtomicReference<>();
        client().admin().indices().preparePutMapping("index").setType("type").setSource("field", "type=long").execute(new ActionListener<PutMappingResponse>() {
            @Override
            public void onResponse(PutMappingResponse response) {
                putMappingResponse.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                putMappingResponse.set(e);
            }
        });
        // ...and wait for mappings to be available on master
        assertBusy(() -> {
            ImmutableOpenMap<String, MappingMetaData> indexMappings = client().admin().indices().prepareGetMappings("index").get().getMappings().get("index");
            assertNotNull(indexMappings);
            MappingMetaData typeMappings = indexMappings.get("type");
            assertNotNull(typeMappings);
            Object properties;
            try {
                properties = typeMappings.getSourceAsMap().get("properties");
            } catch (ElasticsearchParseException e) {
                throw new AssertionError(e);
            }
            assertNotNull(properties);
            Object fieldMapping = ((Map<String, Object>) properties).get("field");
            assertNotNull(fieldMapping);
        });

        final AtomicReference<Object> docIndexResponse = new AtomicReference<>();
        client().prepareIndex("index", "type", "1").setSource("field", 42).execute(new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse response) {
                docIndexResponse.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                docIndexResponse.set(e);
            }
        });

        // Wait a bit to make sure that the reason why we did not get a response
        // is that cluster state processing is blocked and not just that it takes
        // time to process the indexing request
        Thread.sleep(100);
        assertThat(putMappingResponse.get(), equalTo(null));
        assertThat(docIndexResponse.get(), equalTo(null));

        // Now make sure the indexing request finishes successfully
        disruption.stopDisrupting();
        assertBusy(() -> {
            assertThat(putMappingResponse.get(), instanceOf(PutMappingResponse.class));
            PutMappingResponse resp = (PutMappingResponse) putMappingResponse.get();
            assertTrue(resp.isAcknowledged());
            assertThat(docIndexResponse.get(), instanceOf(IndexResponse.class));
            IndexResponse docResp = (IndexResponse) docIndexResponse.get();
            assertEquals(Arrays.toString(docResp.getShardInfo().getFailures()),
                    1, docResp.getShardInfo().getTotal());
        });
    }

    public void testDelayedMappingPropagationOnReplica() throws Exception {
        // This is essentially the same thing as testDelayedMappingPropagationOnPrimary
        // but for replicas
        // Here we want to test that everything goes well if the mappings that
        // are needed for a document are not available on the replica at the
        // time of indexing it
        final List<String> nodeNames = internalCluster().startNodes(2,
            Settings.builder()
                .put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "30s") // explicitly set so it won't default to publish timeout
                .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "0s") // don't wait post commit as we are blocking things by design
                .build());
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut());

        final String master = internalCluster().getMasterName();
        assertThat(nodeNames, hasItem(master));
        String otherNode = null;
        for (String node : nodeNames) {
            if (node.equals(master) == false) {
                otherNode = node;
                break;
            }
        }
        assertNotNull(otherNode);

        // Force allocation of the primary on the master node by first only allocating on the master
        // and then allowing all nodes so that the replica gets allocated on the other node
        assertAcked(prepareCreate("index").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.routing.allocation.include._name", master)).get());
        assertAcked(client().admin().indices().prepareUpdateSettings("index").setSettings(Settings.builder()
                .put("index.routing.allocation.include._name", "")).get());
        ensureGreen();

        // Check routing tables
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertEquals(master, state.nodes().getMasterNode().getName());
        List<ShardRouting> shards = state.routingTable().allShards("index");
        assertThat(shards, hasSize(2));
        for (ShardRouting shard : shards) {
            if (shard.primary()) {
                // primary must be on the master
                assertEquals(state.nodes().getMasterNodeId(), shard.currentNodeId());
            } else {
                assertTrue(shard.active());
            }
        }

        // Block cluster state processing on the replica
        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(otherNode, random());
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        final AtomicReference<Object> putMappingResponse = new AtomicReference<>();
        client().admin().indices().preparePutMapping("index").setType("type").setSource("field", "type=long").execute(new ActionListener<PutMappingResponse>() {
            @Override
            public void onResponse(PutMappingResponse response) {
                putMappingResponse.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                putMappingResponse.set(e);
            }
        });
        final Index index = resolveIndex("index");
        // Wait for mappings to be available on master
        assertBusy(() -> {
            final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, master);
            final IndexService indexService = indicesService.indexServiceSafe(index);
            assertNotNull(indexService);
            final MapperService mapperService = indexService.mapperService();
            DocumentMapper mapper = mapperService.documentMapper("type");
            assertNotNull(mapper);
            assertNotNull(mapper.mappers().getMapper("field"));
        });

        final AtomicReference<Object> docIndexResponse = new AtomicReference<>();
        client().prepareIndex("index", "type", "1").setSource("field", 42).execute(new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse response) {
                docIndexResponse.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                docIndexResponse.set(e);
            }
        });

        // Wait for document to be indexed on primary
        assertBusy(() -> assertTrue(client().prepareGet("index", "type", "1").setPreference("_primary").get().isExists()));

        // The mappings have not been propagated to the replica yet as a consequence the document count not be indexed
        // We wait on purpose to make sure that the document is not indexed because the shard operation is stalled
        // and not just because it takes time to replicate the indexing request to the replica
        Thread.sleep(100);
        assertThat(putMappingResponse.get(), equalTo(null));
        assertThat(docIndexResponse.get(), equalTo(null));

        // Now make sure the indexing request finishes successfully
        disruption.stopDisrupting();
        assertBusy(() -> {
            assertThat(putMappingResponse.get(), instanceOf(PutMappingResponse.class));
            PutMappingResponse resp = (PutMappingResponse) putMappingResponse.get();
            assertTrue(resp.isAcknowledged());
            assertThat(docIndexResponse.get(), instanceOf(IndexResponse.class));
            IndexResponse docResp = (IndexResponse) docIndexResponse.get();
            assertEquals(Arrays.toString(docResp.getShardInfo().getFailures()),
                    2, docResp.getShardInfo().getTotal()); // both shards should have succeeded
        });
    }

}
