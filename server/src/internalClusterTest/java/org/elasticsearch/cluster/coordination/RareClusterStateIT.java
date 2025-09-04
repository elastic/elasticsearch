/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportSettings;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;
import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class RareClusterStateIT extends ESIntegTestCase {

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testAssignmentWithJustAddedNodes() {
        internalCluster().startNode(Settings.builder().put(TransportSettings.CONNECT_TIMEOUT.getKey(), "1s"));
        final String index = "index";
        prepareCreate(index).setSettings(indexSettings(1, 0)).get();
        ensureGreen(index);

        // close to have some unassigned started shards shards..
        indicesAdmin().prepareClose(index).get();

        final String masterName = internalCluster().getMasterName();
        final ClusterService clusterService = internalCluster().clusterService(masterName);
        final AllocationService allocationService = internalCluster().getInstance(AllocationService.class, masterName);
        clusterService.submitUnbatchedStateUpdateTask("test-inject-node-and-reroute", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // inject a node
                ClusterState.Builder builder = ClusterState.builder(currentState);
                builder.nodes(
                    DiscoveryNodes.builder(currentState.nodes()).add(DiscoveryNodeUtils.builder("_non_existent").roles(emptySet()).build())
                );

                // open index
                final IndexMetadata indexMetadata = IndexMetadata.builder(currentState.metadata().getProject().index(index))
                    .state(IndexMetadata.State.OPEN)
                    .build();

                builder.metadata(Metadata.builder(currentState.metadata()).put(indexMetadata, true));
                builder.blocks(ClusterBlocks.builder().blocks(currentState.blocks()).removeIndexBlocks(index));
                ClusterState updatedState = builder.build();

                RoutingTable.Builder routingTable = RoutingTable.builder(
                    TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
                    updatedState.routingTable()
                );
                routingTable.addAsRecovery(updatedState.metadata().getProject().index(index));
                updatedState = ClusterState.builder(updatedState).routingTable(routingTable.build()).build();

                return allocationService.reroute(updatedState, "reroute", ActionListener.noop());
            }

            @Override
            public void onFailure(Exception e) {}
        });
        ensureGreen(index);
        // remove the extra node
        clusterService.submitUnbatchedStateUpdateTask("test-remove-injected-node", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ClusterState.Builder builder = ClusterState.builder(currentState);
                builder.nodes(DiscoveryNodes.builder(currentState.nodes()).remove("_non_existent"));

                currentState = builder.build();
                return allocationService.disassociateDeadNodes(currentState, true, "reroute");
            }

            @Override
            public void onFailure(Exception e) {}
        });
    }

    public void testDeleteCreateInOneBulk() throws Exception {
        final var master = internalCluster().startMasterOnlyNode();
        final var masterClusterService = internalCluster().clusterService(master);

        final var dataNode = internalCluster().startDataOnlyNode();
        final var dataNodeClusterService = internalCluster().clusterService(dataNode);

        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForNodes("2").get().isTimedOut());
        prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)).get();
        ensureGreen("test");

        final var originalIndexUuid = masterClusterService.state().metadata().getProject().index("test").getIndexUUID();
        final var uuidChangedListener = ClusterServiceUtils.addTemporaryStateListener(
            dataNodeClusterService,
            clusterState -> originalIndexUuid.equals(clusterState.metadata().getProject().index("test").getIndexUUID()) == false
                // NB throws a NPE which fails the test if the data node sees the intermediate state with the index deleted
                && clusterState.routingTable().index("test").allShardsActive()
        );

        logger.info("--> indexing a doc");
        indexDoc("test", "1");
        refresh();

        // block publications received by non-master node.
        final var dataNodeTransportService = MockTransportService.getInstance(dataNode);
        dataNodeTransportService.addRequestHandlingBehavior(
            PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
            (handler, request, channel, task) -> channel.sendResponse(new IllegalStateException("cluster state updates blocked"))
        );

        logger.info("--> delete index");
        assertFalse(indicesAdmin().prepareDelete("test").setTimeout(TimeValue.ZERO).get().isAcknowledged());
        logger.info("--> and recreate it");
        assertFalse(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "0")
            ).setTimeout(TimeValue.ZERO).get().isAcknowledged()
        );

        // unblock publications & do a trivial cluster state update to bring data node up to date
        logger.info("--> letting cluster proceed");
        dataNodeTransportService.clearAllRules();
        publishTrivialClusterStateUpdate();

        safeAwait(uuidChangedListener);
        ensureGreen("test");
        final var finalClusterStateVersion = masterClusterService.state().version();
        assertBusy(() -> assertThat(dataNodeClusterService.state().version(), greaterThanOrEqualTo(finalClusterStateVersion)));
        assertHitCount(prepareSearch("test"), 0);
    }

    private static void publishTrivialClusterStateUpdate() {
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask("trivial cluster state update", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).build();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });
    }

    public void testDelayedMappingPropagationOnPrimary() throws Exception {
        // Here we want to test that things go well if there is a first request
        // that adds mappings but before mappings are propagated to all nodes
        // another index request introduces the same mapping. The master node
        // will reply immediately since it did not change the cluster state
        // but the change might not be on the node that performed the indexing
        // operation yet

        final var master = internalCluster().startMasterOnlyNode();
        final var primaryNode = internalCluster().startDataOnlyNode();

        assertAcked(prepareCreate("index").setSettings(indexSettings(1, 0)).get());
        ensureGreen();

        // Block cluster state processing where our shard is
        final var primaryNodeTransportService = MockTransportService.getInstance(primaryNode);
        primaryNodeTransportService.addRequestHandlingBehavior(
            PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
            (handler, request, channel, task) -> channel.sendResponse(new IllegalStateException("cluster state updates blocked"))
        );

        final ActionFuture<DocWriteResponse> docIndexResponseFuture;
        try {
            // Add a new mapping...
            assertFalse(indicesAdmin().preparePutMapping("index").setSource("field", "type=long").get().isAcknowledged());

            // ...and check mappings are available on master
            {
                MappingMetadata typeMappings = internalCluster().clusterService(master)
                    .state()
                    .metadata()
                    .getProject()
                    .index("index")
                    .mapping();
                assertNotNull(typeMappings);
                Object properties;
                try {
                    properties = typeMappings.getSourceAsMap().get("properties");
                } catch (ElasticsearchParseException e) {
                    throw new AssertionError(e);
                }
                assertNotNull(properties);
                @SuppressWarnings("unchecked")
                Object fieldMapping = ((Map<String, Object>) properties).get("field");
                assertNotNull(fieldMapping);
            }

            // this request does not change the cluster state, because the mapping is already created
            docIndexResponseFuture = prepareIndex("index").setId("1").setSource("field", 42).execute();

            // Wait a bit to make sure that the reason why we did not get a response
            // is that cluster state processing is blocked and not just that it takes
            // time to process the indexing request
            Thread.sleep(100);
            assertFalse(docIndexResponseFuture.isDone());

            // Now make sure the indexing request finishes successfully
        } finally {
            primaryNodeTransportService.clearAllRules();
            publishTrivialClusterStateUpdate();
        }
        assertEquals(1, asInstanceOf(IndexResponse.class, docIndexResponseFuture.get(10, TimeUnit.SECONDS)).getShardInfo().getTotal());
    }

    public void testDelayedMappingPropagationOnReplica() throws Exception {
        // This is essentially the same thing as testDelayedMappingPropagationOnPrimary but for replicas
        // Here we want to test that everything goes well if the mappings that are needed for a document are not available on the replica
        // at the time of indexing it

        final var master = internalCluster().startMasterOnlyNode();
        final var masterClusterService = internalCluster().clusterService(master);

        final var primaryNode = internalCluster().startDataOnlyNode();
        assertAcked(prepareCreate("index").setSettings(indexSettings(1, 0)));
        ensureGreen();

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        final var replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen();

        // Check routing table to make sure the shard copies are where we need them to be
        final var state = masterClusterService.state();
        assertEquals(master, state.nodes().getMasterNode().getName());
        List<ShardRouting> shards = state.routingTable().allShards("index");
        assertThat(shards, hasSize(2));
        for (ShardRouting shard : shards) {
            assertTrue(shard.active());
            assertEquals(shard.primary() ? primaryNode : replicaNode, state.nodes().get(shard.currentNodeId()).getName());
        }

        final var primaryIndexService = internalCluster().getInstance(IndicesService.class, primaryNode)
            .indexServiceSafe(state.metadata().getProject().index("index").getIndex());

        // Block cluster state processing on the replica
        final var replicaNodeTransportService = MockTransportService.getInstance(replicaNode);
        replicaNodeTransportService.addRequestHandlingBehavior(
            PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
            (handler, request, channel, task) -> channel.sendResponse(new IllegalStateException("cluster state updates blocked"))
        );

        final ActionFuture<DocWriteResponse> docIndexResponseFuture, dynamicMappingsFuture;
        try {
            // Add a new mapping...
            assertFalse(indicesAdmin().preparePutMapping("index").setSource("field", "type=long").get().isAcknowledged());

            // ...and check mappings are available on the primary
            {
                DocumentMapper mapper = primaryIndexService.mapperService().documentMapper();
                assertNotNull(mapper);
                assertNotNull(mapper.mappers().getMapper("field"));
            }

            // this request does not change the cluster state, because the mapping is already created
            docIndexResponseFuture = prepareIndex("index").setId("1").setSource("field", 42).execute();

            // wait for it to be indexed on the primary (it won't be on the replica yet because of the blocked mapping update)
            assertBusy(() -> assertTrue(client().prepareGet("index", "1").get().isExists()));

            // index another document, this time using dynamic mappings.
            // The ack timeout of 0 on dynamic mapping updates makes it possible for the document to be indexed on the primary, even
            // if the dynamic mapping update is not applied on the replica yet.
            dynamicMappingsFuture = prepareIndex("index").setId("2").setSource("field2", 42).execute();

            // ...and wait for second mapping to be available on the primary
            assertBusy(() -> {
                DocumentMapper mapper = primaryIndexService.mapperService().documentMapper();
                assertNotNull(mapper);
                assertNotNull(mapper.mappers().getMapper("field2"));
            });

            assertBusy(() -> assertTrue(client().prepareGet("index", "2").get().isExists()));

            // The mappings have not been propagated to the replica yet so the document shouldn't be indexed there.
            // We wait on purpose to make sure that the document is not indexed because the shard operation is stalled
            // and not just because it takes time to replicate the indexing request to the replica
            Thread.sleep(100);
            assertFalse(docIndexResponseFuture.isDone());
            assertFalse(dynamicMappingsFuture.isDone());
        } finally {
            // Now make sure the indexing request finishes successfully
            replicaNodeTransportService.clearAllRules();
            publishTrivialClusterStateUpdate();
        }

        // both shards should have succeeded
        assertEquals(2, asInstanceOf(IndexResponse.class, docIndexResponseFuture.get(10, TimeUnit.SECONDS)).getShardInfo().getTotal());
        assertThat(dynamicMappingsFuture.get(30, TimeUnit.SECONDS).getResult(), equalTo(CREATED));
    }
}
