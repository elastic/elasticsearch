/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TransportPrepareReindexTargetActionTests extends ESTestCase {

    private ClusterState createClusterWithIndexAndSettings(String index, int numShards,
                                                           int numReplicas, Settings settings) {
        return createClusterWithIndexAndSettings(index, numShards, numReplicas, numShards, settings);
    }
    private ClusterState createClusterWithIndexAndSettings(String index, int numShards,
                                                           int numReplicas, int numRoutingShards, Settings settings) {
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder(index).settings(settings(Version.CURRENT)
            .put(settings))
            .numberOfShards(numShards).numberOfReplicas(numReplicas).setRoutingNumShards(numRoutingShards).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metadata.index(index));

        RoutingTable routingTable = routingTableBuilder.build();
        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata).routingTable(routingTable)
            .blocks(ClusterBlocks.builder().addBlocks(indexMetadata))
            .build();
    }

    private TransportPrepareReindexTargetAction getMockInstance() {
        final TransportService mockTransportService = mock(TransportService.class);
        final ClusterService mockClusterService = mock(ClusterService.class);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        final MetadataCreateIndexService mockCreateIndexService = mock(MetadataCreateIndexService.class);
        final IndexNameExpressionResolver mockIndexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        final ActionFilters mockActionFilters = mock(ActionFilters.class);

        return new TransportPrepareReindexTargetAction(PrepareReindexTargetAction.NAME, mockTransportService,
            mockClusterService, mockThreadPool, mockCreateIndexService, mockActionFilters,
            mockIndexNameExpressionResolver);
    }

    public void testErrorConditions() throws Exception {
        ClusterState state = createClusterWithIndexAndSettings("source",
            randomIntBetween(2, 42), randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());
        expectThrows(IndexNotFoundException.class, () ->
            TransportPrepareReindexTargetAction.prepareReindexRequest(
                new PrepareReindexRequest("target", "not_source"), state));
        assertThat(
            expectThrows(ResourceAlreadyExistsException.class, () ->
                TransportPrepareReindexTargetAction.prepareReindexRequest(
                    new PrepareReindexRequest("source", "source"), state))
                .getMessage(), equalTo("index already present"));
    }

    public void testFailToCloneIndex() throws Exception {
        ClusterState state = createClusterWithIndexAndSettings("source", randomIntBetween(2, 42),
            randomIntBetween(0, 10), Settings.builder().put("index.blocks.write", true).build());
        PrepareReindexRequest prepareAndCloneIndexRequest = new PrepareReindexRequest("target", "source");
        TransportPrepareReindexTargetAction mockInstance =  getMockInstance();
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            CreateIndexClusterStateUpdateRequest request = (CreateIndexClusterStateUpdateRequest) args[0];
            ActionListener<CreateIndexResponse> listener =
                (ActionListener<CreateIndexResponse>) args[1];
            listener.onResponse(new CreateIndexResponse(false, false, request.index()));
            return null;
        }).when(mockInstance.getCreateIndexService()).createIndex(any(), any());

        PlainActionFuture<CreateIndexResponse> future = new PlainActionFuture<>();
        mockInstance.masterOperation(mock(Task.class), prepareAndCloneIndexRequest, state, future);
        CreateIndexResponse response = future.actionGet();
        assertFalse(response.isAcknowledged());
        assertFalse(response.isShardsAcknowledged());
    }

    public void testIndexClonedAndShardsNotAcknowledged() throws Exception {
        ClusterState state =
            createClusterWithIndexAndSettings("source", randomIntBetween(2, 42), randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());
        PrepareReindexRequest prepareAndCloneIndexRequest = new PrepareReindexRequest("target", "source");
        TransportPrepareReindexTargetAction mockInstance =  getMockInstance();
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            CreateIndexClusterStateUpdateRequest request = (CreateIndexClusterStateUpdateRequest) args[0];
            ActionListener<CreateIndexResponse> listener =
                (ActionListener<CreateIndexResponse>) args[1];
            listener.onResponse(new CreateIndexResponse(true, false, request.index()));
            return null;
        }).when(mockInstance.getCreateIndexService()).createIndex(any(), any());

        PlainActionFuture<CreateIndexResponse> future = new PlainActionFuture<>();
        mockInstance.masterOperation(mock(Task.class), prepareAndCloneIndexRequest, state, future);
        CreateIndexResponse response = future.actionGet();
        assertTrue(response.isAcknowledged());
        assertFalse(response.isShardsAcknowledged());
    }

    public void testPrepareAndCloneIndexResponse() throws Exception {
        ClusterState state =
            createClusterWithIndexAndSettings("source", randomIntBetween(2, 42), randomIntBetween(0, 10),
            Settings.builder().put("index.blocks.write", true).build());
        PrepareReindexRequest prepareAndCloneIndexRequest = new PrepareReindexRequest("target", "source");
        TransportPrepareReindexTargetAction mockInstance =  getMockInstance();
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            CreateIndexClusterStateUpdateRequest request = (CreateIndexClusterStateUpdateRequest) args[0];
            ActionListener<CreateIndexResponse> listener =
                (ActionListener<CreateIndexResponse>) args[1];
            listener.onResponse(
                new CreateIndexResponse(true, true, request.index()));
            return null;
        }).when(mockInstance.getCreateIndexService()).createIndex(any(), any());

        PlainActionFuture<CreateIndexResponse> future = new PlainActionFuture<>();
        mockInstance.masterOperation(mock(Task.class), prepareAndCloneIndexRequest, state, future);
        CreateIndexResponse response = future.actionGet();
        assertTrue(response.isAcknowledged());
        assertTrue(response.isShardsAcknowledged());
    }

}
