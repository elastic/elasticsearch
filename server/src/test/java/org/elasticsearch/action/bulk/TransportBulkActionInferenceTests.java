/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
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
import org.mockito.verification.VerificationMode;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportBulkActionInferenceTests extends ESTestCase {

    public static final String INDEX_NAME = "index";
    public static final String INFERENCE_FIELD_1_MODEL_A = "inference_field_1_model_a";
    public static final String MODEL_A_ID = "model_a_id";
    private static final String INFERENCE_FIELD_2_MODEL_A = "inference_field_2_model_a";
    public static final String MODEL_B_ID = "model_b_id";
    private static final String INFERENCE_FIELD_MODEL_B = "inference_field_model_b";
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
                        .fieldsForModels(
                            Map.of(
                                MODEL_A_ID,
                                Set.of(INFERENCE_FIELD_1_MODEL_A, INFERENCE_FIELD_2_MODEL_A),
                                MODEL_B_ID,
                                Set.of(INFERENCE_FIELD_MODEL_B)
                            )
                        )
                        .numberOfShards(1)
                        .numberOfReplicas(1)
                        .build()
                )
            )
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

    public void testBulkRequestWithoutInference() {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("id");
        indexRequest.source("non_inference_field", "text", "another_non_inference_field", "other text");
        bulkRequest.add(indexRequest);

        expectTransportShardBulkActionRequest();

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(transportBulkAction, null, bulkRequest, future);
        BulkResponse response = future.actionGet();

        assertEquals(1, response.getItems().length);
        verifyInferenceExecuted(never());
    }

    public void testBulkRequestWithInference() {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("id");
        String inferenceFieldText = "some text";
        indexRequest.source(INFERENCE_FIELD_1_MODEL_A, inferenceFieldText, "non_inference_field", "other text");
        bulkRequest.add(indexRequest);

        expectInferenceRequest(Map.of(MODEL_A_ID, Set.of(inferenceFieldText)));

        expectTransportShardBulkActionRequest();

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(transportBulkAction, null, bulkRequest, future);
        BulkResponse response = future.actionGet();

        assertEquals(1, response.getItems().length);
        verifyInferenceExecuted(times(1));
    }

    public void testBulkRequestWithMultipleFieldsInference() {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("id");
        String inferenceField1Text = "some text";
        String inferenceField2Text = "some other text";
        String inferenceField3Text = "more inference text";
        indexRequest.source(
            INFERENCE_FIELD_1_MODEL_A,
            inferenceField1Text,
            INFERENCE_FIELD_2_MODEL_A,
            inferenceField2Text,
            INFERENCE_FIELD_MODEL_B,
            inferenceField3Text,
            "non_inference_field",
            "other text"
        );
        bulkRequest.add(indexRequest);

        expectInferenceRequest(
            Map.of(MODEL_A_ID, Set.of(inferenceField1Text, inferenceField2Text), MODEL_B_ID, Set.of(inferenceField3Text))
        );

        expectTransportShardBulkActionRequest();

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(transportBulkAction, null, bulkRequest, future);
        BulkResponse response = future.actionGet();

        assertEquals(1, response.getItems().length);
        verifyInferenceExecuted(times(2));
    }

    private void verifyInferenceExecuted(VerificationMode times) {
        verify(nodeClient, times).execute(eq(InferenceAction.INSTANCE), any(InferenceAction.Request.class), any());
    }

    private void expectTransportShardBulkActionRequest() {
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
    }

    private void expectInferenceRequest(Map<String, Set<String>> modelsAndInferenceTextMap) {
        doAnswer(invocation -> {
            InferenceAction.Request request = (InferenceAction.Request) invocation.getArguments()[1];
            Set<String> textsForModel = modelsAndInferenceTextMap.get(request.getModelId());
            assertThat("model is not expected", textsForModel, notNullValue());
            assertThat("unexpected inference field values", request.getInput(), containsInAnyOrder(textsForModel.toArray()));

            @SuppressWarnings("unchecked")
            var listener = (ActionListener<InferenceAction.Response>) invocation.getArguments()[2];
            listener.onResponse(
                new InferenceAction.Response(
                    new SparseEmbeddingResults(
                        request.getInput().stream()
                            .map(
                                text -> new SparseEmbeddingResults.Embedding(
                                    List.of(new SparseEmbeddingResults.WeightedToken(text.toString(), 1.0f)),
                                    false
                                )
                            )
                            .toList()
                    )
                )
            );
            return Void.TYPE;
        }).when(nodeClient).execute(eq(InferenceAction.INSTANCE), argThat(r -> inferenceRequestMatches(r, modelsAndInferenceTextMap.keySet())), any());
    }

    private boolean inferenceRequestMatches(ActionRequest request, Set<String> models) {
        return request instanceof InferenceAction.Request && models.contains(((InferenceAction.Request) request).getModelId());
    }
}
