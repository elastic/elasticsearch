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
import org.mockito.ArgumentMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class BulkOperationTests extends ESTestCase {

    public static final String INDEX_NAME = "test-index";
    public static final String INFERENCE_SERVICE_1_ID = "inference_service_1_id";
    public static final String INFERENCE_SERVICE_2_ID = "inference_service_2_id";
    public static final String FIRST_INFERENCE_FIELD_SERVICE_1 = "first_inference_field_service_1";
    public static final String SECOND_INFERENCE_FIELD_SERVICE_1 = "second_inference_field_service_1";
    public static final String INFERENCE_FIELD_SERVICE_2 = "inference_field_service_2";
    public static final String SERVICE_1_ID = "elser_v2";
    public static final String SERVICE_2_ID = "e5";
    private static TestThreadPool threadPool;

    @SuppressWarnings("unchecked")
    public void testInference() {

        Map<String, Set<String>> fieldsForModels = Map.of(
            INFERENCE_SERVICE_1_ID,
            Set.of(FIRST_INFERENCE_FIELD_SERVICE_1, SECOND_INFERENCE_FIELD_SERVICE_1),
            INFERENCE_SERVICE_2_ID,
            Set.of(INFERENCE_FIELD_SERVICE_2)
        );

        ModelRegistry modelRegistry = createModelRegistry(
            Map.of(INFERENCE_SERVICE_1_ID, SERVICE_1_ID, INFERENCE_SERVICE_2_ID, SERVICE_2_ID)
        );

        Model model1 = mock(Model.class);
        InferenceService inferenceService1 = createInferenceService(model1, INFERENCE_SERVICE_1_ID, 2);
        Model model2 = mock(Model.class);
        InferenceService inferenceService2 = createInferenceService(model2, INFERENCE_SERVICE_2_ID, 1);
        InferenceServiceRegistry inferenceServiceRegistry = createInferenceServiceRegistry(
            Map.of(SERVICE_1_ID, inferenceService1, SERVICE_2_ID, inferenceService2)
        );

        String firstInferenceTextService1 = "firstInferenceTextService1";
        String secondInferenceTextService1 = "secondInferenceTextService1";
        String inferenceTextService2 = "inferenceTextService2";
        Map<String, String> originalSource = Map.of(
            FIRST_INFERENCE_FIELD_SERVICE_1,
            firstInferenceTextService1,
            SECOND_INFERENCE_FIELD_SERVICE_1,
            secondInferenceTextService1,
            INFERENCE_FIELD_SERVICE_2,
            inferenceTextService2,
            "other_field",
            "other_value",
            "yet_another_field",
            "yet_another_value"
        );

        ActionListener<BulkResponse> bulkOperationListener = mock(ActionListener.class);
        BulkShardRequest bulkShardRequest = runBulkOperation(
            originalSource,
            fieldsForModels,
            modelRegistry,
            inferenceServiceRegistry,
            bulkOperationListener
        );
        verify(bulkOperationListener).onResponse(any());

        BulkItemRequest[] items = bulkShardRequest.items();
        assertThat(items.length, equalTo(1));

        Map<String, Object> writtenDocSource = ((IndexRequest) items[0].request()).sourceAsMap();
        // Original doc source is preserved
        assertTrue(writtenDocSource.keySet().containsAll(originalSource.keySet()));
        assertTrue(writtenDocSource.values().containsAll(originalSource.values()));

        // Check inference results
        verifyInferenceServiceInvoked(
            modelRegistry,
            INFERENCE_SERVICE_1_ID,
            inferenceService1,
            model1,
            List.of(firstInferenceTextService1, secondInferenceTextService1)
        );
        verifyInferenceServiceInvoked(modelRegistry, INFERENCE_SERVICE_2_ID, inferenceService2, model2, List.of(inferenceTextService2));
        Map<String, Object> inferenceRootResultField = (Map<String, Object>) writtenDocSource.get(
            BulkShardRequestInferenceProvider.ROOT_INFERENCE_FIELD
        );

        checkInferenceResult(inferenceRootResultField, FIRST_INFERENCE_FIELD_SERVICE_1, firstInferenceTextService1);
        checkInferenceResult(inferenceRootResultField, SECOND_INFERENCE_FIELD_SERVICE_1, secondInferenceTextService1);
        checkInferenceResult(inferenceRootResultField, INFERENCE_FIELD_SERVICE_2, inferenceTextService2);
    }

    public void testFailedInference() {

        Map<String, Set<String>> fieldsForModels = Map.of(INFERENCE_SERVICE_1_ID, Set.of(FIRST_INFERENCE_FIELD_SERVICE_1));

        ModelRegistry modelRegistry = createModelRegistry(Map.of(INFERENCE_SERVICE_1_ID, SERVICE_1_ID));

        Model model = mock(Model.class);
        InferenceService inferenceService = createInferenceServiceThatFails(model, INFERENCE_SERVICE_1_ID);
        InferenceServiceRegistry inferenceServiceRegistry = createInferenceServiceRegistry(Map.of(SERVICE_1_ID, inferenceService));

        String firstInferenceTextService1 = "firstInferenceTextService1";
        Map<String, String> originalSource = Map.of(
            FIRST_INFERENCE_FIELD_SERVICE_1,
            firstInferenceTextService1,
            "other_field",
            "other_value"
        );

        ArgumentCaptor<BulkResponse> bulkResponseCaptor = ArgumentCaptor.forClass(BulkResponse.class);
        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> bulkOperationListener = mock(ActionListener.class);
        BulkShardRequest bulkShardRequest = runBulkOperation(
            originalSource,
            fieldsForModels,
            modelRegistry,
            inferenceServiceRegistry,
            bulkOperationListener
        );
        BulkItemRequest[] items = bulkShardRequest.items();
        assertThat(items.length, equalTo(1));
        assertNull(items[0]);
        verify(bulkOperationListener).onResponse(bulkResponseCaptor.capture());
        BulkResponse bulkResponse = bulkResponseCaptor.getValue();
        assertTrue(bulkResponse.hasFailures());

        verifyInferenceServiceInvoked(modelRegistry, INFERENCE_SERVICE_1_ID, inferenceService, model, List.of(firstInferenceTextService1));

    }

    private static void checkInferenceResult(Map<String, Object> inferenceRootResultField, String fieldName, String expectedText) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> inferenceService1FieldResults = (List<Map<String, Object>>) inferenceRootResultField.get(fieldName);
        assertNotNull(inferenceService1FieldResults);
        assertThat(inferenceService1FieldResults.size(), equalTo(1));
        Map<String, Object> inferenceResultElement = inferenceService1FieldResults.get(0);
        assertNotNull(inferenceResultElement.get(BulkShardRequestInferenceProvider.SPARSE_VECTOR_SUBFIELD_NAME));
        assertThat(inferenceResultElement.get(BulkShardRequestInferenceProvider.TEXT_SUBFIELD_NAME), equalTo(expectedText));
    }

    private static void verifyInferenceServiceInvoked(
        ModelRegistry modelRegistry,
        String inferenceService1Id,
        InferenceService inferenceService,
        Model model,
        List<String> inferenceTexts
    ) {
        verify(modelRegistry).getModel(eq(inferenceService1Id), any());
        verify(inferenceService).parsePersistedConfig(eq(inferenceService1Id), eq(TaskType.SPARSE_EMBEDDING), anyMap());
        verify(inferenceService).infer(eq(model), argThat(containsAll(inferenceTexts)), anyMap(), eq(InputType.INGEST), any());
        verifyNoMoreInteractions(inferenceService);
    }

    private static ArgumentMatcher<List<String>> containsAll(List<String> expected) {
        return new ArgumentMatcher<>() {
            @Override
            public boolean matches(List<String> argument) {
                return argument.containsAll(expected) && argument.size() == expected.size();
            }

            @Override
            public String toString() {
                return "containsAll(" + expected.stream().collect(Collectors.joining(", ")) + ")";
            }
        };
    }

    private static BulkShardRequest runBulkOperation(
        Map<String, String> docSource,
        Map<String, Set<String>> fieldsForModels,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry inferenceServiceRegistry,
        ActionListener<BulkResponse> bulkOperationListener
    ) {
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
                    .filter(Objects::nonNull)
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
        }).when(client).executeLocally(eq(TransportShardBulkAction.TYPE), bulkShardRequestCaptor.capture(), any());

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
        verify(client).executeLocally(eq(TransportShardBulkAction.TYPE), any(), any());

        return bulkShardRequestCaptor.getValue();
    }

    private static InferenceService createInferenceService(Model model, String inferenceServiceId, int numResults) {
        InferenceService inferenceService = mock(InferenceService.class);
        when(inferenceService.parsePersistedConfig(eq(inferenceServiceId), eq(TaskType.SPARSE_EMBEDDING), anyMap())).thenReturn(model);
        InferenceServiceResults inferenceServiceResults = mock(InferenceServiceResults.class);
        List<InferenceResults> inferenceResults = new ArrayList<>();
        for (int i = 0; i < numResults; i++) {
            inferenceResults.add(createInferenceResults());
        }
        doReturn(inferenceResults).when(inferenceServiceResults).transformToLegacyFormat();
        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(4);
            listener.onResponse(inferenceServiceResults);
            return null;
        }).when(inferenceService).infer(eq(model), anyList(), anyMap(), eq(InputType.INGEST), any());
        return inferenceService;
    }

    private static InferenceService createInferenceServiceThatFails(Model model, String inferenceServiceId) {
        InferenceService inferenceService = mock(InferenceService.class);
        when(inferenceService.parsePersistedConfig(eq(inferenceServiceId), eq(TaskType.SPARSE_EMBEDDING), anyMap())).thenReturn(model);
        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(4);
            listener.onFailure(new IllegalArgumentException("Inference failed"));
            return null;
        }).when(inferenceService).infer(eq(model), anyList(), anyMap(), eq(InputType.INGEST), any());
        return inferenceService;
    }

    private static InferenceResults createInferenceResults() {
        InferenceResults inferenceResults = mock(InferenceResults.class);
        when(inferenceResults.asMap(any())).then(
            invocation -> Map.of(
                (String) invocation.getArguments()[0],
                Map.of("sparse_embedding", randomMap(1, 10, () -> new Tuple<>(randomAlphaOfLength(10), randomFloat())))
            )
        );
        return inferenceResults;
    }

    private static InferenceServiceRegistry createInferenceServiceRegistry(Map<String, InferenceService> inferenceServices) {
        InferenceServiceRegistry inferenceServiceRegistry = mock(InferenceServiceRegistry.class);
        inferenceServices.forEach((id, service) -> when(inferenceServiceRegistry.getService(id)).thenReturn(Optional.of(service)));
        return inferenceServiceRegistry;
    }

    private static ModelRegistry createModelRegistry(Map<String, String> inferenceIdsToServiceIds) {
        ModelRegistry modelRegistry = mock(ModelRegistry.class);
        inferenceIdsToServiceIds.forEach((inferenceId, serviceId) -> {
            ModelRegistry.UnparsedModel unparsedModel = new ModelRegistry.UnparsedModel(
                inferenceId,
                TaskType.SPARSE_EMBEDDING,
                serviceId,
                emptyMap(),
                emptyMap()
            );
            doAnswer(invocation -> {
                ActionListener<ModelRegistry.UnparsedModel> listener = invocation.getArgument(1);
                listener.onResponse(unparsedModel);
                return null;
            }).when(modelRegistry).getModel(eq(inferenceId), any());
        });

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
