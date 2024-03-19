/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.results.ChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.inference.mapper.InferenceMetadataFieldMapper;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.elasticsearch.xpack.inference.mapper.InferenceMetadataFieldMapperTests.randomSparseEmbeddings;
import static org.elasticsearch.xpack.inference.mapper.InferenceMetadataFieldMapperTests.randomTextEmbeddings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardBulkInferenceActionFilterTests extends ESTestCase {
    private ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() throws Exception {
        terminate(threadPool);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testFilterNoop() throws Exception {
        ShardBulkInferenceActionFilter filter = createFilter(threadPool, Map.of());
        CountDownLatch chainExecuted = new CountDownLatch(1);
        ActionFilterChain actionFilterChain = (task, action, request, listener) -> {
            try {
                assertNull(((BulkShardRequest) request).getFieldsInferenceMetadata());
            } finally {
                chainExecuted.countDown();
            }
        };
        ActionListener actionListener = mock(ActionListener.class);
        Task task = mock(Task.class);
        BulkShardRequest request = new BulkShardRequest(
            new ShardId("test", "test", 0),
            WriteRequest.RefreshPolicy.NONE,
            new BulkItemRequest[0]
        );
        request.setFieldInferenceMetadata(Map.of("foo", Set.of("bar")));
        filter.apply(task, TransportShardBulkAction.ACTION_NAME, request, actionListener, actionFilterChain);
        awaitLatch(chainExecuted, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testInferenceNotFound() throws Exception {
        StaticModel model = randomStaticModel();
        ShardBulkInferenceActionFilter filter = createFilter(threadPool, Map.of(model.getInferenceEntityId(), model));
        CountDownLatch chainExecuted = new CountDownLatch(1);
        ActionFilterChain actionFilterChain = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkShardRequest = (BulkShardRequest) request;
                assertNull(bulkShardRequest.getFieldsInferenceMetadata());
                for (BulkItemRequest item : bulkShardRequest.items()) {
                    assertNotNull(item.getPrimaryResponse());
                    assertTrue(item.getPrimaryResponse().isFailed());
                    BulkItemResponse.Failure failure = item.getPrimaryResponse().getFailure();
                    assertThat(failure.getStatus(), equalTo(RestStatus.NOT_FOUND));
                }
            } finally {
                chainExecuted.countDown();
            }
        };
        ActionListener actionListener = mock(ActionListener.class);
        Task task = mock(Task.class);

        Map<String, Set<String>> inferenceFields = Map.of(
            model.getInferenceEntityId(),
            Set.of("field1"),
            "inference_0",
            Set.of("field2", "field3")
        );
        BulkItemRequest[] items = new BulkItemRequest[10];
        for (int i = 0; i < items.length; i++) {
            items[i] = randomBulkItemRequest(i, Map.of(), inferenceFields)[0];
        }
        BulkShardRequest request = new BulkShardRequest(new ShardId("test", "test", 0), WriteRequest.RefreshPolicy.NONE, items);
        request.setFieldInferenceMetadata(inferenceFields);
        filter.apply(task, TransportShardBulkAction.ACTION_NAME, request, actionListener, actionFilterChain);
        awaitLatch(chainExecuted, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testManyRandomDocs() throws Exception {
        Map<String, StaticModel> inferenceModelMap = new HashMap<>();
        int numModels = randomIntBetween(1, 5);
        for (int i = 0; i < numModels; i++) {
            StaticModel model = randomStaticModel();
            inferenceModelMap.put(model.getInferenceEntityId(), model);
        }

        int numInferenceFields = randomIntBetween(1, 5);
        Map<String, Set<String>> inferenceFields = new HashMap<>();
        for (int i = 0; i < numInferenceFields; i++) {
            String inferenceId = randomFrom(inferenceModelMap.keySet());
            String field = randomAlphaOfLengthBetween(5, 10);
            var res = inferenceFields.computeIfAbsent(inferenceId, k -> new HashSet<>());
            res.add(field);
        }

        int numRequests = randomIntBetween(100, 1000);
        BulkItemRequest[] originalRequests = new BulkItemRequest[numRequests];
        BulkItemRequest[] modifiedRequests = new BulkItemRequest[numRequests];
        for (int id = 0; id < numRequests; id++) {
            BulkItemRequest[] res = randomBulkItemRequest(id, inferenceModelMap, inferenceFields);
            originalRequests[id] = res[0];
            modifiedRequests[id] = res[1];
        }

        ShardBulkInferenceActionFilter filter = createFilter(threadPool, inferenceModelMap);
        CountDownLatch chainExecuted = new CountDownLatch(1);
        ActionFilterChain actionFilterChain = (task, action, request, listener) -> {
            try {
                assertThat(request, instanceOf(BulkShardRequest.class));
                BulkShardRequest bulkShardRequest = (BulkShardRequest) request;
                assertNull(bulkShardRequest.getFieldsInferenceMetadata());
                BulkItemRequest[] items = bulkShardRequest.items();
                assertThat(items.length, equalTo(originalRequests.length));
                for (int id = 0; id < items.length; id++) {
                    IndexRequest actualRequest = ShardBulkInferenceActionFilter.getIndexRequestOrNull(items[id].request());
                    IndexRequest expectedRequest = ShardBulkInferenceActionFilter.getIndexRequestOrNull(modifiedRequests[id].request());
                    try {
                        assertToXContentEquivalent(expectedRequest.source(), actualRequest.source(), actualRequest.getContentType());
                    } catch (Exception exc) {
                        throw new IllegalStateException(exc);
                    }
                }
            } finally {
                chainExecuted.countDown();
            }
        };
        ActionListener actionListener = mock(ActionListener.class);
        Task task = mock(Task.class);
        BulkShardRequest original = new BulkShardRequest(new ShardId("test", "test", 0), WriteRequest.RefreshPolicy.NONE, originalRequests);
        original.setFieldInferenceMetadata(inferenceFields);
        filter.apply(task, TransportShardBulkAction.ACTION_NAME, original, actionListener, actionFilterChain);
        awaitLatch(chainExecuted, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private static ShardBulkInferenceActionFilter createFilter(ThreadPool threadPool, Map<String, StaticModel> modelMap) {
        ModelRegistry modelRegistry = mock(ModelRegistry.class);
        Answer<?> unparsedModelAnswer = invocationOnMock -> {
            String id = (String) invocationOnMock.getArguments()[0];
            ActionListener<ModelRegistry.UnparsedModel> listener = (ActionListener<ModelRegistry.UnparsedModel>) invocationOnMock
                .getArguments()[1];
            var model = modelMap.get(id);
            if (model != null) {
                listener.onResponse(
                    new ModelRegistry.UnparsedModel(
                        model.getInferenceEntityId(),
                        model.getTaskType(),
                        model.getServiceSettings().model(),
                        XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(model.getTaskSettings()), false),
                        XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(model.getSecretSettings()), false)
                    )
                );
            } else {
                listener.onFailure(new ResourceNotFoundException("model id [{}] not found", id));
            }
            return null;
        };
        doAnswer(unparsedModelAnswer).when(modelRegistry).getModelWithSecrets(any(), any());

        InferenceService inferenceService = mock(InferenceService.class);
        Answer<?> chunkedInferAnswer = invocationOnMock -> {
            StaticModel model = (StaticModel) invocationOnMock.getArguments()[0];
            List<String> inputs = (List<String>) invocationOnMock.getArguments()[1];
            ActionListener<List<ChunkedInferenceServiceResults>> listener = (ActionListener<
                List<ChunkedInferenceServiceResults>>) invocationOnMock.getArguments()[5];
            Runnable runnable = () -> {
                List<ChunkedInferenceServiceResults> results = new ArrayList<>();
                for (String input : inputs) {
                    results.add(model.getResults(input));
                }
                listener.onResponse(results);
            };
            if (randomBoolean()) {
                try {
                    threadPool.generic().execute(runnable);
                } catch (Exception exc) {
                    listener.onFailure(exc);
                }
            } else {
                runnable.run();
            }
            return null;
        };
        doAnswer(chunkedInferAnswer).when(inferenceService).chunkedInfer(any(), any(), any(), any(), any(), any());

        Answer<Model> modelAnswer = invocationOnMock -> {
            String inferenceId = (String) invocationOnMock.getArguments()[0];
            return modelMap.get(inferenceId);
        };
        doAnswer(modelAnswer).when(inferenceService).parsePersistedConfigWithSecrets(any(), any(), any(), any());

        InferenceServiceRegistry inferenceServiceRegistry = mock(InferenceServiceRegistry.class);
        when(inferenceServiceRegistry.getService(any())).thenReturn(Optional.of(inferenceService));
        ShardBulkInferenceActionFilter filter = new ShardBulkInferenceActionFilter(inferenceServiceRegistry, modelRegistry);
        return filter;
    }

    private static BulkItemRequest[] randomBulkItemRequest(
        int id,
        Map<String, StaticModel> modelMap,
        Map<String, Set<String>> inferenceFieldMap
    ) {
        Map<String, Object> docMap = new LinkedHashMap<>();
        Map<String, Object> inferenceResultsMap = new LinkedHashMap<>();
        for (var entry : inferenceFieldMap.entrySet()) {
            String inferenceId = entry.getKey();
            var model = modelMap.get(inferenceId);
            for (var field : entry.getValue()) {
                String text = randomAlphaOfLengthBetween(10, 100);
                docMap.put(field, text);
                if (model == null) {
                    // ignore results, the doc should fail with a resource not found exception
                    continue;
                }
                int numChunks = randomIntBetween(1, 5);
                List<String> chunks = new ArrayList<>();
                for (int i = 0; i < numChunks; i++) {
                    chunks.add(randomAlphaOfLengthBetween(5, 10));
                }
                TaskType taskType = model.getTaskType();
                final ChunkedInferenceServiceResults results;
                switch (taskType) {
                    case TEXT_EMBEDDING:
                        results = randomTextEmbeddings(chunks);
                        break;

                    case SPARSE_EMBEDDING:
                        results = randomSparseEmbeddings(chunks);
                        break;

                    default:
                        throw new AssertionError("Unknown task type " + taskType.name());
                }
                model.putResult(text, results);
                InferenceMetadataFieldMapper.applyFieldInference(inferenceResultsMap, field, model, results);
            }
        }
        Map<String, Object> expectedDocMap = new LinkedHashMap<>(docMap);
        expectedDocMap.put(InferenceMetadataFieldMapper.NAME, inferenceResultsMap);
        return new BulkItemRequest[] {
            new BulkItemRequest(id, new IndexRequest("index").source(docMap)),
            new BulkItemRequest(id, new IndexRequest("index").source(expectedDocMap)) };
    }

    private static StaticModel randomStaticModel() {
        String serviceName = randomAlphaOfLengthBetween(5, 10);
        String inferenceId = randomAlphaOfLengthBetween(5, 10);
        return new StaticModel(
            inferenceId,
            randomBoolean() ? TaskType.TEXT_EMBEDDING : TaskType.SPARSE_EMBEDDING,
            serviceName,
            new TestModel.TestServiceSettings("my-model"),
            new TestModel.TestTaskSettings(randomIntBetween(1, 100)),
            new TestModel.TestSecretSettings(randomAlphaOfLength(10))
        );
    }

    private static class StaticModel extends TestModel {
        private final Map<String, ChunkedInferenceServiceResults> resultMap;

        StaticModel(
            String inferenceEntityId,
            TaskType taskType,
            String service,
            TestServiceSettings serviceSettings,
            TestTaskSettings taskSettings,
            TestSecretSettings secretSettings
        ) {
            super(inferenceEntityId, taskType, service, serviceSettings, taskSettings, secretSettings);
            this.resultMap = new HashMap<>();
        }

        ChunkedInferenceServiceResults getResults(String text) {
            return resultMap.getOrDefault(text, new ChunkedSparseEmbeddingResults(List.of()));
        }

        void putResult(String text, ChunkedInferenceServiceResults results) {
            resultMap.put(text, results);
        }
    }
}
