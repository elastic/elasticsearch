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
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.results.ErrorChunkedInferenceResults;
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.DEFAULT_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.getIndexRequestOrNull;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticText;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticTextInput;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSparseEmbeddings;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.semanticTextFieldFromChunkedInferenceResults;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.toChunkedResult;
import static org.hamcrest.Matchers.containsString;
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
        ShardBulkInferenceActionFilter filter = createFilter(threadPool, Map.of(), DEFAULT_BATCH_SIZE);
        CountDownLatch chainExecuted = new CountDownLatch(1);
        ActionFilterChain actionFilterChain = (task, action, request, listener) -> {
            try {
                assertNull(((BulkShardRequest) request).getInferenceFieldMap());
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
        request.setInferenceFieldMap(
            Map.of("foo", new InferenceFieldMetadata("foo", "bar", generateRandomStringArray(5, 10, false, false)))
        );
        filter.apply(task, TransportShardBulkAction.ACTION_NAME, request, actionListener, actionFilterChain);
        awaitLatch(chainExecuted, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testInferenceNotFound() throws Exception {
        StaticModel model = StaticModel.createRandomInstance();
        ShardBulkInferenceActionFilter filter = createFilter(
            threadPool,
            Map.of(model.getInferenceEntityId(), model),
            randomIntBetween(1, 10)
        );
        CountDownLatch chainExecuted = new CountDownLatch(1);
        ActionFilterChain actionFilterChain = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkShardRequest = (BulkShardRequest) request;
                assertNull(bulkShardRequest.getInferenceFieldMap());
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

        Map<String, InferenceFieldMetadata> inferenceFieldMap = Map.of(
            "field1",
            new InferenceFieldMetadata("field1", model.getInferenceEntityId(), new String[] { "field1" }),
            "field2",
            new InferenceFieldMetadata("field2", "inference_0", new String[] { "field2" }),
            "field3",
            new InferenceFieldMetadata("field3", "inference_0", new String[] { "field3" })
        );
        BulkItemRequest[] items = new BulkItemRequest[10];
        for (int i = 0; i < items.length; i++) {
            items[i] = randomBulkItemRequest(Map.of(), inferenceFieldMap)[0];
        }
        BulkShardRequest request = new BulkShardRequest(new ShardId("test", "test", 0), WriteRequest.RefreshPolicy.NONE, items);
        request.setInferenceFieldMap(inferenceFieldMap);
        filter.apply(task, TransportShardBulkAction.ACTION_NAME, request, actionListener, actionFilterChain);
        awaitLatch(chainExecuted, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testItemFailures() throws Exception {
        StaticModel model = StaticModel.createRandomInstance();
        ShardBulkInferenceActionFilter filter = createFilter(
            threadPool,
            Map.of(model.getInferenceEntityId(), model),
            randomIntBetween(1, 10)
        );
        model.putResult("I am a failure", new ErrorChunkedInferenceResults(new IllegalArgumentException("boom")));
        model.putResult("I am a success", randomSparseEmbeddings(List.of("I am a success")));
        CountDownLatch chainExecuted = new CountDownLatch(1);
        ActionFilterChain actionFilterChain = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkShardRequest = (BulkShardRequest) request;
                assertNull(bulkShardRequest.getInferenceFieldMap());
                assertThat(bulkShardRequest.items().length, equalTo(3));

                // item 0 is a failure
                assertNotNull(bulkShardRequest.items()[0].getPrimaryResponse());
                assertTrue(bulkShardRequest.items()[0].getPrimaryResponse().isFailed());
                BulkItemResponse.Failure failure = bulkShardRequest.items()[0].getPrimaryResponse().getFailure();
                assertThat(failure.getCause().getCause().getMessage(), containsString("boom"));

                // item 1 is a success
                assertNull(bulkShardRequest.items()[1].getPrimaryResponse());
                IndexRequest actualRequest = getIndexRequestOrNull(bulkShardRequest.items()[1].request());
                assertThat(XContentMapValues.extractValue("field1.text", actualRequest.sourceAsMap()), equalTo("I am a success"));

                // item 2 is a failure
                assertNotNull(bulkShardRequest.items()[2].getPrimaryResponse());
                assertTrue(bulkShardRequest.items()[2].getPrimaryResponse().isFailed());
                failure = bulkShardRequest.items()[2].getPrimaryResponse().getFailure();
                assertThat(failure.getCause().getCause().getMessage(), containsString("boom"));
            } finally {
                chainExecuted.countDown();
            }
        };
        ActionListener actionListener = mock(ActionListener.class);
        Task task = mock(Task.class);

        Map<String, InferenceFieldMetadata> inferenceFieldMap = Map.of(
            "field1",
            new InferenceFieldMetadata("field1", model.getInferenceEntityId(), new String[] { "field1" })
        );
        BulkItemRequest[] items = new BulkItemRequest[3];
        items[0] = new BulkItemRequest(0, new IndexRequest("index").source("field1", "I am a failure"));
        items[1] = new BulkItemRequest(1, new IndexRequest("index").source("field1", "I am a success"));
        items[2] = new BulkItemRequest(2, new IndexRequest("index").source("field1", "I am a failure"));
        BulkShardRequest request = new BulkShardRequest(new ShardId("test", "test", 0), WriteRequest.RefreshPolicy.NONE, items);
        request.setInferenceFieldMap(inferenceFieldMap);
        filter.apply(task, TransportShardBulkAction.ACTION_NAME, request, actionListener, actionFilterChain);
        awaitLatch(chainExecuted, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testManyRandomDocs() throws Exception {
        Map<String, StaticModel> inferenceModelMap = new HashMap<>();
        int numModels = randomIntBetween(1, 3);
        for (int i = 0; i < numModels; i++) {
            StaticModel model = StaticModel.createRandomInstance();
            inferenceModelMap.put(model.getInferenceEntityId(), model);
        }

        int numInferenceFields = randomIntBetween(1, 3);
        Map<String, InferenceFieldMetadata> inferenceFieldMap = new HashMap<>();
        for (int i = 0; i < numInferenceFields; i++) {
            String field = randomAlphaOfLengthBetween(5, 10);
            String inferenceId = randomFrom(inferenceModelMap.keySet());
            inferenceFieldMap.put(field, new InferenceFieldMetadata(field, inferenceId, new String[] { field }));
        }

        int numRequests = atLeast(100);
        BulkItemRequest[] originalRequests = new BulkItemRequest[numRequests];
        BulkItemRequest[] modifiedRequests = new BulkItemRequest[numRequests];
        for (int id = 0; id < numRequests; id++) {
            BulkItemRequest[] res = randomBulkItemRequest(inferenceModelMap, inferenceFieldMap);
            originalRequests[id] = res[0];
            modifiedRequests[id] = res[1];
        }

        ShardBulkInferenceActionFilter filter = createFilter(threadPool, inferenceModelMap, randomIntBetween(10, 30));
        CountDownLatch chainExecuted = new CountDownLatch(1);
        ActionFilterChain actionFilterChain = (task, action, request, listener) -> {
            try {
                assertThat(request, instanceOf(BulkShardRequest.class));
                BulkShardRequest bulkShardRequest = (BulkShardRequest) request;
                assertNull(bulkShardRequest.getInferenceFieldMap());
                BulkItemRequest[] items = bulkShardRequest.items();
                assertThat(items.length, equalTo(originalRequests.length));
                for (int id = 0; id < items.length; id++) {
                    IndexRequest actualRequest = getIndexRequestOrNull(items[id].request());
                    IndexRequest expectedRequest = getIndexRequestOrNull(modifiedRequests[id].request());
                    try {
                        assertToXContentEquivalent(expectedRequest.source(), actualRequest.source(), expectedRequest.getContentType());
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
        original.setInferenceFieldMap(inferenceFieldMap);
        filter.apply(task, TransportShardBulkAction.ACTION_NAME, original, actionListener, actionFilterChain);
        awaitLatch(chainExecuted, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private static ShardBulkInferenceActionFilter createFilter(ThreadPool threadPool, Map<String, StaticModel> modelMap, int batchSize) {
        ModelRegistry modelRegistry = mock(ModelRegistry.class);
        Answer<?> unparsedModelAnswer = invocationOnMock -> {
            String id = (String) invocationOnMock.getArguments()[0];
            ActionListener<UnparsedModel> listener = (ActionListener<UnparsedModel>) invocationOnMock.getArguments()[1];
            var model = modelMap.get(id);
            if (model != null) {
                listener.onResponse(
                    new UnparsedModel(
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
            List<String> inputs = (List<String>) invocationOnMock.getArguments()[2];
            ActionListener<List<ChunkedInferenceServiceResults>> listener = (ActionListener<
                List<ChunkedInferenceServiceResults>>) invocationOnMock.getArguments()[7];
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
        doAnswer(chunkedInferAnswer).when(inferenceService).chunkedInfer(any(), any(), any(), any(), any(), any(), any(), any());

        Answer<Model> modelAnswer = invocationOnMock -> {
            String inferenceId = (String) invocationOnMock.getArguments()[0];
            return modelMap.get(inferenceId);
        };
        doAnswer(modelAnswer).when(inferenceService).parsePersistedConfigWithSecrets(any(), any(), any(), any());

        InferenceServiceRegistry inferenceServiceRegistry = mock(InferenceServiceRegistry.class);
        when(inferenceServiceRegistry.getService(any())).thenReturn(Optional.of(inferenceService));
        ShardBulkInferenceActionFilter filter = new ShardBulkInferenceActionFilter(inferenceServiceRegistry, modelRegistry, batchSize);
        return filter;
    }

    private static BulkItemRequest[] randomBulkItemRequest(
        Map<String, StaticModel> modelMap,
        Map<String, InferenceFieldMetadata> fieldInferenceMap
    ) throws IOException {
        Map<String, Object> docMap = new LinkedHashMap<>();
        Map<String, Object> expectedDocMap = new LinkedHashMap<>();
        XContentType requestContentType = randomFrom(XContentType.values());
        for (var entry : fieldInferenceMap.values()) {
            String field = entry.getName();
            var model = modelMap.get(entry.getInferenceId());
            Object inputObject = randomSemanticTextInput();
            String inputText = inputObject.toString();
            docMap.put(field, inputObject);
            expectedDocMap.put(field, inputText);
            if (model == null) {
                // ignore results, the doc should fail with a resource not found exception
                continue;
            }

            SemanticTextField semanticTextField;
            // The model is not field aware and that is why we are skipping the embedding generation process for existing values.
            // This prevents a situation where embeddings in the expected docMap do not match those in the model, which could happen if
            // embeddings were overwritten.
            if (model.hasResult(inputText)) {
                ChunkedInferenceServiceResults results = model.getResults(inputText);
                semanticTextField = semanticTextFieldFromChunkedInferenceResults(
                    field,
                    model,
                    List.of(inputText),
                    results,
                    requestContentType
                );
            } else {
                semanticTextField = randomSemanticText(field, model, List.of(inputText), requestContentType);
                model.putResult(inputText, toChunkedResult(semanticTextField));
            }

            expectedDocMap.put(field, semanticTextField);
        }

        int requestId = randomIntBetween(0, Integer.MAX_VALUE);
        return new BulkItemRequest[] {
            new BulkItemRequest(requestId, new IndexRequest("index").source(docMap, requestContentType)),
            new BulkItemRequest(requestId, new IndexRequest("index").source(expectedDocMap, requestContentType)) };
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

        public static StaticModel createRandomInstance() {
            TestModel testModel = TestModel.createRandomInstance();
            return new StaticModel(
                testModel.getInferenceEntityId(),
                testModel.getTaskType(),
                randomAlphaOfLength(10),
                testModel.getServiceSettings(),
                testModel.getTaskSettings(),
                testModel.getSecretSettings()
            );
        }

        ChunkedInferenceServiceResults getResults(String text) {
            return resultMap.getOrDefault(text, new InferenceChunkedSparseEmbeddingResults(List.of()));
        }

        void putResult(String text, ChunkedInferenceServiceResults result) {
            resultMap.put(text, result);
        }

        boolean hasResult(String text) {
            return resultMap.containsKey(text);
        }
    }
}
