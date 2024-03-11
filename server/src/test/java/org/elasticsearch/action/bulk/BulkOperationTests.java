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
import org.elasticsearch.inference.SemanticTextModelSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider.INFERENCE_CHUNKS_RESULTS;
import static org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider.INFERENCE_CHUNKS_TEXT;
import static org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider.INFERENCE_RESULTS;
import static org.elasticsearch.action.bulk.BulkShardRequestInferenceProvider.ROOT_INFERENCE_FIELD;
import static org.hamcrest.CoreMatchers.containsString;
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

    private static final String INDEX_NAME = "test-index";
    private static final String INFERENCE_SERVICE_1_ID = "inference_service_1_id";
    private static final String INFERENCE_SERVICE_2_ID = "inference_service_2_id";
    private static final String FIRST_INFERENCE_FIELD_SERVICE_1 = "first_inference_field_service_1";
    private static final String SECOND_INFERENCE_FIELD_SERVICE_1 = "second_inference_field_service_1";
    private static final String INFERENCE_FIELD_SERVICE_2 = "inference_field_service_2";
    private static final String SERVICE_1_ID = "elser_v2";
    private static final String SERVICE_2_ID = "e5";
    private static final String INFERENCE_FAILED_MSG = "Inference failed";
    private static TestThreadPool threadPool;

    public void testNoInference() {

        Map<String, Set<String>> fieldsForModels = Map.of();
        ModelRegistry modelRegistry = createModelRegistry(
            Map.of(INFERENCE_SERVICE_1_ID, SERVICE_1_ID, INFERENCE_SERVICE_2_ID, SERVICE_2_ID)
        );

        Model model1 = mockModel(INFERENCE_SERVICE_1_ID);
        InferenceService inferenceService1 = createInferenceService(model1);
        Model model2 = mockModel(INFERENCE_SERVICE_2_ID);
        InferenceService inferenceService2 = createInferenceService(model2);
        InferenceServiceRegistry inferenceServiceRegistry = createInferenceServiceRegistry(
            Map.of(SERVICE_1_ID, inferenceService1, SERVICE_2_ID, inferenceService2)
        );

        Map<String, Object> originalSource = Map.of(
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 100),
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 100)
        );

        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> bulkOperationListener = mock(ActionListener.class);
        BulkShardRequest bulkShardRequest = runBulkOperation(
            originalSource,
            fieldsForModels,
            modelRegistry,
            inferenceServiceRegistry,
            true,
            bulkOperationListener
        );
        verify(bulkOperationListener).onResponse(any());

        BulkItemRequest[] items = bulkShardRequest.items();
        assertThat(items.length, equalTo(1));

        Map<String, Object> writtenDocSource = ((IndexRequest) items[0].request()).sourceAsMap();
        // Original doc source is preserved
        originalSource.forEach((key, value) -> assertThat(writtenDocSource.get(key), equalTo(value)));

        // Check inference not invoked
        verifyNoMoreInteractions(modelRegistry);
        verifyNoMoreInteractions(inferenceServiceRegistry);
    }

    private static Model mockModel(String inferenceServiceId) {
        Model model = mock(Model.class);

        when(model.getInferenceEntityId()).thenReturn(inferenceServiceId);
        TaskType taskType = randomBoolean() ? TaskType.SPARSE_EMBEDDING : TaskType.TEXT_EMBEDDING;
        when(model.getTaskType()).thenReturn(taskType);

        ServiceSettings serviceSettings = mock(ServiceSettings.class);
        when(model.getServiceSettings()).thenReturn(serviceSettings);
        SimilarityMeasure similarity = switch (randomInt(2)) {
            case 0 -> SimilarityMeasure.COSINE;
            case 1 -> SimilarityMeasure.DOT_PRODUCT;
            default -> null;
        };
        when(serviceSettings.similarity()).thenReturn(similarity);
        when(serviceSettings.dimensions()).thenReturn(randomBoolean() ? null : randomIntBetween(1, 1000));

        return model;
    }

    public void testFailedBulkShardRequest() {

        Map<String, Set<String>> fieldsForModels = Map.of();
        ModelRegistry modelRegistry = createModelRegistry(Map.of());
        InferenceServiceRegistry inferenceServiceRegistry = createInferenceServiceRegistry(Map.of());

        Map<String, Object> originalSource = Map.of(
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 100),
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 100)
        );

        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> bulkOperationListener = mock(ActionListener.class);
        ArgumentCaptor<BulkResponse> bulkResponseCaptor = ArgumentCaptor.forClass(BulkResponse.class);
        doAnswer(invocation -> null).when(bulkOperationListener).onResponse(bulkResponseCaptor.capture());

        runBulkOperation(
            originalSource,
            fieldsForModels,
            modelRegistry,
            inferenceServiceRegistry,
            bulkOperationListener,
            true,
            request -> new BulkShardResponse(
                request.shardId(),
                new BulkItemResponse[] {
                    BulkItemResponse.failure(
                        0,
                        DocWriteRequest.OpType.INDEX,
                        new BulkItemResponse.Failure(
                            INDEX_NAME,
                            randomIdentifier(),
                            new IllegalArgumentException("Error on bulk shard request")
                        )
                    ) }
            )
        );
        verify(bulkOperationListener).onResponse(any());

        BulkResponse bulkResponse = bulkResponseCaptor.getValue();
        assertTrue(bulkResponse.hasFailures());
        BulkItemResponse[] items = bulkResponse.getItems();
        assertTrue(items[0].isFailed());
    }

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

        Model model1 = mockModel(INFERENCE_SERVICE_1_ID);
        InferenceService inferenceService1 = createInferenceService(model1);
        Model model2 = mockModel(INFERENCE_SERVICE_2_ID);
        InferenceService inferenceService2 = createInferenceService(model2);
        InferenceServiceRegistry inferenceServiceRegistry = createInferenceServiceRegistry(
            Map.of(SERVICE_1_ID, inferenceService1, SERVICE_2_ID, inferenceService2)
        );

        String firstInferenceTextService1 = randomAlphaOfLengthBetween(1, 100);
        String secondInferenceTextService1 = randomAlphaOfLengthBetween(1, 100);
        String inferenceTextService2 = randomAlphaOfLengthBetween(1, 100);
        Map<String, Object> originalSource = Map.of(
            FIRST_INFERENCE_FIELD_SERVICE_1,
            firstInferenceTextService1,
            SECOND_INFERENCE_FIELD_SERVICE_1,
            secondInferenceTextService1,
            INFERENCE_FIELD_SERVICE_2,
            inferenceTextService2,
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 100),
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 100)
        );

        ActionListener<BulkResponse> bulkOperationListener = mock(ActionListener.class);
        BulkShardRequest bulkShardRequest = runBulkOperation(
            originalSource,
            fieldsForModels,
            modelRegistry,
            inferenceServiceRegistry,
            true,
            bulkOperationListener
        );
        verify(bulkOperationListener).onResponse(any());

        BulkItemRequest[] items = bulkShardRequest.items();
        assertThat(items.length, equalTo(1));

        Map<String, Object> writtenDocSource = ((IndexRequest) items[0].request()).sourceAsMap();
        // Original doc source is preserved
        originalSource.forEach((key, value) -> assertThat(writtenDocSource.get(key), equalTo(value)));

        // Check inference results
        verifyInferenceServiceInvoked(
            modelRegistry,
            INFERENCE_SERVICE_1_ID,
            inferenceService1,
            model1,
            List.of(firstInferenceTextService1, secondInferenceTextService1)
        );
        verifyInferenceServiceInvoked(modelRegistry, INFERENCE_SERVICE_2_ID, inferenceService2, model2, List.of(inferenceTextService2));
        checkInferenceResults(
            originalSource,
            writtenDocSource,
            FIRST_INFERENCE_FIELD_SERVICE_1,
            SECOND_INFERENCE_FIELD_SERVICE_1,
            INFERENCE_FIELD_SERVICE_2
        );
    }

    public void testFailedInference() {

        Map<String, Set<String>> fieldsForModels = Map.of(INFERENCE_SERVICE_1_ID, Set.of(FIRST_INFERENCE_FIELD_SERVICE_1));

        ModelRegistry modelRegistry = createModelRegistry(Map.of(INFERENCE_SERVICE_1_ID, SERVICE_1_ID));

        Model model = mockModel(INFERENCE_SERVICE_1_ID);
        InferenceService inferenceService = createInferenceServiceThatFails(model);
        InferenceServiceRegistry inferenceServiceRegistry = createInferenceServiceRegistry(Map.of(SERVICE_1_ID, inferenceService));

        String firstInferenceTextService1 = randomAlphaOfLengthBetween(1, 100);
        Map<String, Object> originalSource = Map.of(
            FIRST_INFERENCE_FIELD_SERVICE_1,
            firstInferenceTextService1,
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 100)
        );

        ArgumentCaptor<BulkResponse> bulkResponseCaptor = ArgumentCaptor.forClass(BulkResponse.class);
        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> bulkOperationListener = mock(ActionListener.class);
        runBulkOperation(originalSource, fieldsForModels, modelRegistry, inferenceServiceRegistry, false, bulkOperationListener);

        verify(bulkOperationListener).onResponse(bulkResponseCaptor.capture());
        BulkResponse bulkResponse = bulkResponseCaptor.getValue();
        assertTrue(bulkResponse.hasFailures());
        BulkItemResponse item = bulkResponse.getItems()[0];
        assertTrue(item.isFailed());
        assertThat(item.getFailure().getCause().getMessage(), equalTo(INFERENCE_FAILED_MSG));

        verifyInferenceServiceInvoked(modelRegistry, INFERENCE_SERVICE_1_ID, inferenceService, model, List.of(firstInferenceTextService1));

    }

    public void testInferenceFailsForIncorrectRootObject() {

        Map<String, Set<String>> fieldsForModels = Map.of(INFERENCE_SERVICE_1_ID, Set.of(FIRST_INFERENCE_FIELD_SERVICE_1));

        ModelRegistry modelRegistry = createModelRegistry(Map.of(INFERENCE_SERVICE_1_ID, SERVICE_1_ID));

        Model model = mockModel(INFERENCE_SERVICE_1_ID);
        InferenceService inferenceService = createInferenceServiceThatFails(model);
        InferenceServiceRegistry inferenceServiceRegistry = createInferenceServiceRegistry(Map.of(SERVICE_1_ID, inferenceService));

        Map<String, Object> originalSource = Map.of(
            FIRST_INFERENCE_FIELD_SERVICE_1,
            randomAlphaOfLengthBetween(1, 100),
            ROOT_INFERENCE_FIELD,
            "incorrect_root_object"
        );

        ArgumentCaptor<BulkResponse> bulkResponseCaptor = ArgumentCaptor.forClass(BulkResponse.class);
        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> bulkOperationListener = mock(ActionListener.class);
        runBulkOperation(originalSource, fieldsForModels, modelRegistry, inferenceServiceRegistry, false, bulkOperationListener);

        verify(bulkOperationListener).onResponse(bulkResponseCaptor.capture());
        BulkResponse bulkResponse = bulkResponseCaptor.getValue();
        assertTrue(bulkResponse.hasFailures());
        BulkItemResponse item = bulkResponse.getItems()[0];
        assertTrue(item.isFailed());
        assertThat(item.getFailure().getCause().getMessage(), containsString("[_semantic_text_inference] is not an object"));
    }

    public void testInferenceIdNotFound() {

        Map<String, Set<String>> fieldsForModels = Map.of(
            INFERENCE_SERVICE_1_ID,
            Set.of(FIRST_INFERENCE_FIELD_SERVICE_1, SECOND_INFERENCE_FIELD_SERVICE_1),
            INFERENCE_SERVICE_2_ID,
            Set.of(INFERENCE_FIELD_SERVICE_2)
        );

        ModelRegistry modelRegistry = createModelRegistry(Map.of(INFERENCE_SERVICE_1_ID, SERVICE_1_ID));

        Model model = mockModel(INFERENCE_SERVICE_1_ID);
        InferenceService inferenceService = createInferenceService(model);
        InferenceServiceRegistry inferenceServiceRegistry = createInferenceServiceRegistry(Map.of(SERVICE_1_ID, inferenceService));

        Map<String, Object> originalSource = Map.of(
            INFERENCE_FIELD_SERVICE_2,
            randomAlphaOfLengthBetween(1, 100),
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 100)
        );

        ArgumentCaptor<BulkResponse> bulkResponseCaptor = ArgumentCaptor.forClass(BulkResponse.class);
        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> bulkOperationListener = mock(ActionListener.class);
        doAnswer(invocation -> null).when(bulkOperationListener).onResponse(bulkResponseCaptor.capture());

        runBulkOperation(originalSource, fieldsForModels, modelRegistry, inferenceServiceRegistry, false, bulkOperationListener);

        verify(bulkOperationListener).onResponse(bulkResponseCaptor.capture());
        BulkResponse bulkResponse = bulkResponseCaptor.getValue();
        assertTrue(bulkResponse.hasFailures());
        BulkItemResponse item = bulkResponse.getItems()[0];
        assertTrue(item.isFailed());
        assertThat(
            item.getFailure().getCause().getMessage(),
            equalTo("No inference provider found for model ID " + INFERENCE_SERVICE_2_ID)
        );
    }

    @SuppressWarnings("unchecked")
    private static void checkInferenceResults(
        Map<String, Object> docSource,
        Map<String, Object> writtenDocSource,
        String... inferenceFieldNames
    ) {

        Map<String, Object> inferenceRootResultField = (Map<String, Object>) writtenDocSource.get(
            BulkShardRequestInferenceProvider.ROOT_INFERENCE_FIELD
        );

        for (String inferenceFieldName : inferenceFieldNames) {
            Map<String, Object> inferenceService1FieldResults = (Map<String, Object>) inferenceRootResultField.get(inferenceFieldName);
            assertNotNull(inferenceService1FieldResults);
            assertThat(inferenceService1FieldResults.size(), equalTo(2));
            Map<String, Object> modelSettings = (Map<String, Object>) inferenceService1FieldResults.get(SemanticTextModelSettings.NAME);
            assertNotNull(modelSettings);
            assertNotNull(modelSettings.get(SemanticTextModelSettings.TASK_TYPE_FIELD.getPreferredName()));
            assertNotNull(modelSettings.get(SemanticTextModelSettings.INFERENCE_ID_FIELD.getPreferredName()));

            List<Map<String, Object>> inferenceResultElement = (List<Map<String, Object>>) inferenceService1FieldResults.get(
                INFERENCE_RESULTS
            );
            assertFalse(inferenceResultElement.isEmpty());
            assertNotNull(inferenceResultElement.get(0).get(INFERENCE_CHUNKS_RESULTS));
            assertThat(inferenceResultElement.get(0).get(INFERENCE_CHUNKS_TEXT), equalTo(docSource.get(inferenceFieldName)));
        }
    }

    private static void verifyInferenceServiceInvoked(
        ModelRegistry modelRegistry,
        String inferenceService1Id,
        InferenceService inferenceService,
        Model model,
        Collection<String> inferenceTexts
    ) {
        verify(modelRegistry).getModelWithSecrets(eq(inferenceService1Id), any());
        verify(inferenceService).parsePersistedConfigWithSecrets(
            eq(inferenceService1Id),
            eq(TaskType.SPARSE_EMBEDDING),
            anyMap(),
            anyMap()
        );
        verify(inferenceService).infer(eq(model), argThat(containsInAnyOrder(inferenceTexts)), anyMap(), eq(InputType.INGEST), any());
        verifyNoMoreInteractions(inferenceService);
    }

    private static ArgumentMatcher<List<String>> containsInAnyOrder(Collection<String> expected) {
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
        Map<String, Object> docSource,
        Map<String, Set<String>> fieldsForModels,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry inferenceServiceRegistry,
        boolean expectTransportShardBulkActionToExecute,
        ActionListener<BulkResponse> bulkOperationListener
    ) {
        return runBulkOperation(
            docSource,
            fieldsForModels,
            modelRegistry,
            inferenceServiceRegistry,
            bulkOperationListener,
            expectTransportShardBulkActionToExecute,
            successfulBulkShardResponse
        );
    }

    private static BulkShardRequest runBulkOperation(
        Map<String, Object> docSource,
        Map<String, Set<String>> fieldsForModels,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry inferenceServiceRegistry,
        ActionListener<BulkResponse> bulkOperationListener,
        boolean expectTransportShardBulkActionToExecute,
        Function<BulkShardRequest, BulkShardResponse> bulkShardResponseSupplier
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
            bulkShardResponseListener.onResponse(bulkShardResponseSupplier.apply(request));
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
        if (expectTransportShardBulkActionToExecute) {
            verify(client).executeLocally(eq(TransportShardBulkAction.TYPE), any(), any());
            return bulkShardRequestCaptor.getValue();
        }

        return null;
    }

    private static final Function<BulkShardRequest, BulkShardResponse> successfulBulkShardResponse = (request) -> {
        return new BulkShardResponse(
            request.shardId(),
            Arrays.stream(request.items())
                .filter(Objects::nonNull)
                .map(
                    item -> BulkItemResponse.success(
                        item.id(),
                        DocWriteRequest.OpType.INDEX,
                        new IndexResponse(request.shardId(), randomIdentifier(), randomLong(), randomLong(), randomLong(), randomBoolean())
                    )
                )
                .toArray(BulkItemResponse[]::new)
        );
    };

    private static InferenceService createInferenceService(Model model) {
        InferenceService inferenceService = mock(InferenceService.class);
        when(
            inferenceService.parsePersistedConfigWithSecrets(
                eq(model.getInferenceEntityId()),
                eq(TaskType.SPARSE_EMBEDDING),
                anyMap(),
                anyMap()
            )
        ).thenReturn(model);
        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(4);
            InferenceServiceResults inferenceServiceResults = mock(InferenceServiceResults.class);
            List<String> texts = invocation.getArgument(1);
            List<InferenceResults> inferenceResults = new ArrayList<>();
            for (int i = 0; i < texts.size(); i++) {
                inferenceResults.add(createInferenceResults());
            }
            doReturn(inferenceResults).when(inferenceServiceResults).transformToCoordinationFormat();

            listener.onResponse(inferenceServiceResults);
            return null;
        }).when(inferenceService).infer(eq(model), anyList(), anyMap(), eq(InputType.INGEST), any());
        return inferenceService;
    }

    private static InferenceService createInferenceServiceThatFails(Model model) {
        InferenceService inferenceService = mock(InferenceService.class);
        when(
            inferenceService.parsePersistedConfigWithSecrets(
                eq(model.getInferenceEntityId()),
                eq(TaskType.SPARSE_EMBEDDING),
                anyMap(),
                anyMap()
            )
        ).thenReturn(model);
        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(4);
            listener.onFailure(new IllegalArgumentException(INFERENCE_FAILED_MSG));
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
        // Fails for unknown inference ids
        doAnswer(invocation -> {
            ActionListener<ModelRegistry.UnparsedModel> listener = invocation.getArgument(1);
            listener.onFailure(new IllegalArgumentException("Model not found"));
            return null;
        }).when(modelRegistry).getModelWithSecrets(any(), any());
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
            }).when(modelRegistry).getModelWithSecrets(eq(inferenceId), any());
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
