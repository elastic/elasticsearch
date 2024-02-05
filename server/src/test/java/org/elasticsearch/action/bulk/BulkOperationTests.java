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
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BulkOperationTests extends ESTestCase {

    public static final String INFERENCE_SERVICE_ID = "inferenece_service_id";
    public static final String INDEX_NAME = "test-index";
    public static final String INFERENCE_FIELD = "inference_field";
    public static final String SERVICE_ID = "elser_v2";
    private static TestThreadPool threadPool;

    @SuppressWarnings("unchecked")
    public void testInference() {

        Map<String, Set<String>> fieldsForModels = Map.of(INFERENCE_SERVICE_ID, Set.of(INFERENCE_FIELD));

        ModelRegistry modelRegistry = createModelRegistry();

        Model model = mock(Model.class);
        InferenceService inferenceService = createInferenceService(model);
        InferenceServiceRegistry inferenceServiceRegistry = createInferenceServiceRegistry(inferenceService);

        String inferenceText = "test";
        Map<String, String> originalSource = Map.of(INFERENCE_FIELD, inferenceText);

        BulkShardRequest bulkShardRequest = runBulkOperation(originalSource, fieldsForModels , modelRegistry, inferenceServiceRegistry);

        verifyInferenceDone(modelRegistry, inferenceService, model, inferenceText);

        BulkItemRequest[] items = bulkShardRequest.items();
        assertThat(items.length, equalTo(1));
        Map<String, Object> docSource = ((IndexRequest) items[0].request()).sourceAsMap();
        Map<String, Object> inferenceRootResultField = (Map<String, Object>) docSource.get(BulkShardRequestInferenceProvider.ROOT_INFERENCE_FIELD);
        List<Map<String, Object>> inferenceFieldResults = (List<Map<String, Object>>) inferenceRootResultField.get(INFERENCE_FIELD);
        assertNotNull(inferenceFieldResults);
        assertThat(inferenceFieldResults.size(), equalTo(1));
        Map<String, Object> inferenceResultElement = inferenceFieldResults.get(0);
        assertNotNull(inferenceResultElement.get(BulkShardRequestInferenceProvider.SPARSE_VECTOR_SUBFIELD_NAME));
        assertThat(inferenceResultElement.get(BulkShardRequestInferenceProvider.TEXT_SUBFIELD_NAME), equalTo(inferenceText));
    }

    private static void verifyInferenceDone(ModelRegistry modelRegistry, InferenceService inferenceService, Model model, String inferenceText) {
        verify(modelRegistry).getModel(eq(INFERENCE_SERVICE_ID), any());
        verify(inferenceService).parsePersistedConfig(eq(INFERENCE_SERVICE_ID), eq(TaskType.SPARSE_EMBEDDING), anyMap());
        verify(inferenceService).infer(eq(model), eq(List.of(inferenceText)), anyMap(), eq(InputType.INGEST), any());
    }

    private static BulkShardRequest runBulkOperation(Map<String, String> docSource, Map<String, Set<String>> fieldsForModels, ModelRegistry modelRegistry, InferenceServiceRegistry inferenceServiceRegistry) {


        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX_NAME)
            .fieldsForModels(fieldsForModels)
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterService clusterService = createClusterService(indexMetadata);

        IndexNameExpressionResolver indexResolver = mock(IndexNameExpressionResolver.class);
        when(indexResolver.resolveWriteIndexAbstraction(any(), any())).thenReturn(new IndexAbstraction.ConcreteIndex(indexMetadata));

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(INDEX_NAME).source(docSource));

        NodeClient client = mock(NodeClient.class);

        ArgumentCaptor<BulkShardRequest> bulkShardRequestCaptor = ArgumentCaptor.forClass(BulkShardRequest.class);
        doAnswer(invocation -> {
            BulkShardRequest request = invocation.getArgument(1);
            ActionListener<BulkShardResponse> bulkShardResponseListener = invocation.getArgument(2);

            BulkShardResponse bulkShardResponse = new BulkShardResponse(
                request.shardId(),
                Arrays.stream(request.items())
                    .map(
                        item -> BulkItemResponse.success(
                            item.id(),
                            DocWriteRequest.OpType.INDEX,
                            new IndexResponse(
                                request.shardId(),
                                randomIdentifier(),
                                randomLong(),
                                randomLong(),
                                randomLong(),
                                randomBoolean()
                            )
                        )
                    )
                    .toArray(BulkItemResponse[]::new)
            );
            bulkShardResponseListener.onResponse(bulkShardResponse);
            return null;
            }
        ).when(client).executeLocally(eq(TransportShardBulkAction.TYPE), bulkShardRequestCaptor.capture(), any());

        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> bulkOperationListener = mock(ActionListener.class);
        Task task = new Task(randomLong(), "transport", "action", "", null, emptyMap());
        BulkOperation bulkOperation = new BulkOperation(
            task,
            threadPool,
            ThreadPool.Names.WRITE,
            clusterService,
            bulkRequest,
            client,
            new AtomicArray<>(bulkRequest.requests.size()),
            new HashMap<>(),
            indexResolver,
            () -> System.nanoTime(),
            System.nanoTime(),
            modelRegistry,
            inferenceServiceRegistry,
            bulkOperationListener
        );

        bulkOperation.doRun();
        verify(bulkOperationListener).onResponse(any());
        verify(client).executeLocally(eq(TransportShardBulkAction.TYPE), any(), any());

        return bulkShardRequestCaptor.getValue();
    }

    private static InferenceService createInferenceService(Model model) {
        InferenceService inferenceService = mock(InferenceService.class);
        when(inferenceService.parsePersistedConfig(eq(INFERENCE_SERVICE_ID), eq(TaskType.SPARSE_EMBEDDING), anyMap())).thenReturn(model);
        InferenceServiceResults inferenceServiceResults = mock(InferenceServiceResults.class);
        InferenceResults inferenceResults = mock(InferenceResults.class);
        when(inferenceResults.asMap(any())).then(
            invocation -> Map.of(
                (String) invocation.getArguments()[0],
                Map.of("sparse_embedding", randomMap(1, 10, () -> new Tuple<>(randomAlphaOfLength(10), randomFloat())))
            )
        );
        doReturn(List.of(inferenceResults)).when(inferenceServiceResults).transformToLegacyFormat();
        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(4);
            listener.onResponse(inferenceServiceResults);
            return null;
        }).when(inferenceService).infer(eq(model), anyList(), anyMap(), eq(InputType.INGEST), any());
        return inferenceService;
    }

    private static InferenceServiceRegistry createInferenceServiceRegistry(InferenceService inferenceService) {
        InferenceServiceRegistry inferenceServiceRegistry = mock(InferenceServiceRegistry.class);
        when(inferenceServiceRegistry.getService(SERVICE_ID)).thenReturn(Optional.of(inferenceService));
        return inferenceServiceRegistry;
    }

    private static ModelRegistry createModelRegistry() {
        ModelRegistry modelRegistry = mock(ModelRegistry.class);
        ModelRegistry.UnparsedModel unparsedModel = new ModelRegistry.UnparsedModel(
            INFERENCE_SERVICE_ID,
            TaskType.SPARSE_EMBEDDING,
            SERVICE_ID,
            emptyMap(),
            emptyMap()
        );
        doAnswer(invocation -> {
            ActionListener<ModelRegistry.UnparsedModel> listener = invocation.getArgument(1);
            listener.onResponse(unparsedModel);
            return null;
        }).when(modelRegistry).getModel(eq(INFERENCE_SERVICE_ID), any());
        return modelRegistry;
    }

    private static ClusterService createClusterService(IndexMetadata indexMetadata) {
        Metadata metadata = Metadata.builder().indices(Map.of(INDEX_NAME, indexMetadata)).build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(DiscoveryNodeUtils.create(randomIdentifier()));

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).version(randomNonNegativeLong()).build();
        when(clusterService.state()).thenReturn(clusterState);

        ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.state()).thenReturn(clusterState);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        return clusterService;
    }


    @BeforeClass
    public static void createThreadPool() {
        threadPool = new TestThreadPool(getTestClass().getName());
    }

    @AfterClass
    public static void stopThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

}
