/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.inference.InferenceAction;
import org.elasticsearch.action.inference.results.SparseEmbeddingResults;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportBulkActionInferenceTests  extends ESTestCase {

    public static final String INDEX_NAME = "index";
    public static final String INFERENCE_FIELD = "inference_field";
    public static final String MODEL_ID = "model_id";
    private TransportService transportService;
    private CapturingTransport capturingTransport;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private NodeClient nodeClient;
    private TransportBulkAction transportBulkAction;


    @Before
    public void setup() {
        threadPool = new TestThreadPool(getClass().getName());

        nodeClient = mock(NodeClient.class);

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        DiscoveryNode remoteNode = mock(DiscoveryNode.class);
        Map<String, DiscoveryNode> ingestNodes = Map.of("node", remoteNode);
        when(nodes.getIngestNodes()).thenReturn(ingestNodes);
        Metadata metadata = Metadata.builder()
            .indices(
                Map.of(
                    INDEX_NAME,
                    IndexMetadata.builder(INDEX_NAME)
                        .settings(settings(IndexVersion.current()))
                        .fieldsForModels(Map.of(MODEL_ID, Set.of(INFERENCE_FIELD)))
                        .numberOfShards(1)
                        .numberOfReplicas(1)
                        .build()
            ))
            .build();

        DiscoveryNode masterNode = DiscoveryNodeUtils.create(UUIDs.base64UUID());
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).masterNodeId(masterNode.getId()))
            .build();

        clusterService = ClusterServiceUtils.createClusterService(state, threadPool);

        capturingTransport = new CapturingTransport();
        transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        IngestService ingestService = mock(IngestService.class);
        transportBulkAction = new TransportBulkAction(
            threadPool,
            transportService,
            clusterService,
            ingestService,
            nodeClient,
            new ActionFilters(Collections.emptySet()),
            TestIndexNameExpressionResolver.newInstance(),
            new IndexingPressure(Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), true).build()),
            EmptySystemIndices.INSTANCE
        );
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        super.tearDown();
    }


    public void testBulkRequestWithInference() {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("id");
        indexRequest.source(INFERENCE_FIELD, "some text");
        bulkRequest.add(indexRequest);

        doAnswer(invocation -> {

            InferenceAction.Request request = (InferenceAction.Request) invocation.getArguments()[1];
            assertThat(request.getModelId(), equalTo(MODEL_ID));
            assertThat(request.getInput(), equalTo(List.of("some text")));

            @SuppressWarnings("unchecked")
            var listener = (ActionListener<InferenceAction.Response>) invocation.getArguments()[2];
            listener.onResponse(
            new InferenceAction.Response(
                new SparseEmbeddingResults(
                    List.of(
                        new SparseEmbeddingResults.Embedding(
                            List.of(
                                new SparseEmbeddingResults.WeightedToken("some", 0.5f),
                                new SparseEmbeddingResults.WeightedToken("text", 0.5f)
                            ), false)
                    )
                )
            ));
            return Void.TYPE;
        }
        ).when(nodeClient).execute(eq(InferenceAction.INSTANCE), any(InferenceAction.Request.class), any());

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<BulkShardResponse>) invocation.getArguments()[2];
            ShardId shardId = new ShardId(INDEX_NAME, "UUID", 0);
            BulkItemResponse successResponse = BulkItemResponse.success(
                0,
                DocWriteRequest.OpType.INDEX,
                new IndexResponse(shardId, "id", 0, 0, 0, true)
            );
            listener.onResponse(new BulkShardResponse(shardId, new BulkItemResponse[] { successResponse }));
            return null;
        }).when(nodeClient).executeLocally(eq(TransportShardBulkAction.TYPE), any(BulkShardRequest.class), any());

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(transportBulkAction, null, bulkRequest, future);
        BulkResponse response = future.actionGet();


        assertEquals(1, response.getItems().length);
        verify(nodeClient).execute(eq(InferenceAction.INSTANCE), any(InferenceAction.Request.class), any());

    }
}
