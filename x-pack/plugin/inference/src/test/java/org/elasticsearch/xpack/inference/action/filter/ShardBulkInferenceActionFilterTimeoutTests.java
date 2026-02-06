/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.inference.telemetry.InferenceStatsTests;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.INDICES_INFERENCE_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.INDICES_INFERENCE_BULK_TIMEOUT;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomChunkedInferenceEmbedding;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for timeout behavior in {@link ShardBulkInferenceActionFilter}.
 *
 * <p>Timeout scenarios tested:</p>
 * <ul>
 *   <li><b>Batch-level timeout</b>: When earlier batches consume time, later batches fail at the
 *       batch-level check in {@code addFieldInferenceRequests()} before inference starts.</li>
 *   <li><b>Inference service timeout</b>: The timeout is passed to {@code chunkedInfer()} and the
 *       inference service enforces it. Different inference IDs are processed independently.</li>
 *   <li><b>Pre-inference timeout after model loading</b>: If model loading takes long enough to
 *       exceed timeout, inference is skipped and items fail at the pre-inference check.</li>
 *   <li><b>Items without inference fields</b>: These pass through successfully even after timeout,
 *       since they don't require inference processing.</li>
 *   <li><b>Blank values</b>: Empty/whitespace values skip inference and don't trigger timeout failures.</li>
 *   <li><b>Multiple inference fields per document</b>: If any field times out, the entire document fails.</li>
 * </ul>
 *
 * <p>All tests randomly use {@link org.elasticsearch.action.index.IndexRequest} or
 * {@link org.elasticsearch.action.update.UpdateRequest} to ensure both are covered.</p>
 */
public class ShardBulkInferenceActionFilterTimeoutTests extends AbstractShardBulkInferenceActionFilterTestCase {

    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueMillis(50);
    private static final long SLOW_DELAY_MS = 100;  // Must exceed DEFAULT_TIMEOUT

    public ShardBulkInferenceActionFilterTimeoutTests(boolean useLegacyFormat) {
        super(useLegacyFormat);
    }

    // ========== Test Cases ==========

    /**
     * Tests batch-level timeout: when an earlier batch takes too long, subsequent batches
     * fail at the batch-level check before inference even starts.
     */
    public void testBatchLevelTimeout() throws Exception {
        StaticModel model = StaticModel.createRandomInstance();
        int totalBatches = randomIntBetween(2, 5);
        int itemsPerBatch = 2;  // 20-byte batch size, 10-byte texts
        int totalItems = totalBatches * itemsPerBatch;
        int slowBatch = randomIntBetween(1, totalBatches - 1);

        String[] texts = createTextsAndRegisterResults(model, totalItems);
        AtomicInteger batchCount = new AtomicInteger(0);

        ShardBulkInferenceActionFilter filter = createFilter(model, (inputs, listener) -> {
            int batch = batchCount.incrementAndGet();
            return batch == slowBatch ? DelayResult.delayThenComplete(SLOW_DELAY_MS) : DelayResult.immediate();
        });

        int expectedSuccessItems = slowBatch * itemsPerBatch;
        runFilterAndVerify(filter, model, "field1", texts, results -> {
            for (int i = 0; i < totalItems; i++) {
                BulkItemResponse response = results[i].getPrimaryResponse();
                if (i < expectedSuccessItems) {
                    assertNull("Item " + i + " should succeed", response);
                } else {
                    assertTimeout(response, "Item " + i);
                }
            }
        });

        assertThat(batchCount.get(), equalTo(slowBatch));
    }

    /**
     * Tests that timeout is passed to inference service and failures propagate correctly.
     * Items using different inference IDs are processed independently.
     */
    public void testInferenceServiceTimeoutPropagation() throws Exception {
        StaticModel fastModel = StaticModel.createRandomInstance();
        StaticModel slowModel = StaticModel.createRandomInstance();

        String fastText = "fast_text";
        String[] slowTexts = { "slow_text_1", "slow_text_2", "slow_text_3" };

        fastModel.putResult(fastText, randomChunkedInferenceEmbedding(fastModel, List.of(fastText)));
        for (String text : slowTexts) {
            slowModel.putResult(text, randomChunkedInferenceEmbedding(slowModel, List.of(text)));
        }

        AtomicInteger inferenceCallCount = new AtomicInteger(0);

        ShardBulkInferenceActionFilter filter = createFilter(
            Map.of(fastModel.getInferenceEntityId(), fastModel, slowModel.getInferenceEntityId(), slowModel),
            (inputs, listener) -> {
                inferenceCallCount.incrementAndGet();
                return inputs.get(0).inputText().startsWith("fast") ? DelayResult.immediate() : DelayResult.timeout();
            }
        );

        Map<String, InferenceFieldMetadata> fieldMap = Map.of(
            "fast_field",
            inferenceFieldMetadata("fast_field", fastModel),
            "slow_field",
            inferenceFieldMetadata("slow_field", slowModel)
        );

        BulkItemRequest[] items = {
            bulkItemRequest(0, "fast_field", fastText),
            bulkItemRequest(1, "slow_field", slowTexts[0]),
            bulkItemRequest(2, "slow_field", slowTexts[1]),
            bulkItemRequest(3, "slow_field", slowTexts[2]) };

        runFilterAndVerify(filter, fieldMap, items, results -> {
            assertNull("Fast model item should succeed", results[0].getPrimaryResponse());
            for (int i = 1; i <= 3; i++) {
                assertTimeout(results[i].getPrimaryResponse(), "Slow model item " + i);
            }
        });

        assertThat(inferenceCallCount.get(), equalTo(2));
    }

    /**
     * Tests that items without inference fields pass through successfully even after timeout.
     */
    public void testTimeoutDoesNotAffectItemsWithoutInferenceFields() throws Exception {
        StaticModel model = StaticModel.createRandomInstance();
        String[] texts = createTextsAndRegisterResults(model, 4);
        AtomicInteger batchCount = new AtomicInteger(0);

        ShardBulkInferenceActionFilter filter = createFilter(model, (inputs, listener) -> {
            int batch = batchCount.incrementAndGet();
            return batch == 1 ? DelayResult.delayThenComplete(SLOW_DELAY_MS) : DelayResult.immediate();
        });

        Map<String, InferenceFieldMetadata> fieldMap = Map.of("semantic_field", inferenceFieldMetadata("semantic_field", model));

        // Items 0,1 in batch 1 (succeed), item 2 has inference (timeout), item 3 has no inference (passes)
        BulkItemRequest[] items = {
            bulkItemRequest(0, "semantic_field", texts[0]),
            bulkItemRequest(1, "semantic_field", texts[1]),
            bulkItemRequest(2, "semantic_field", texts[2]),
            bulkItemRequest(3, "other_field", texts[3])  // No inference field
        };

        runFilterAndVerify(filter, fieldMap, items, results -> {
            assertNull("Item 0 should succeed", results[0].getPrimaryResponse());
            assertNull("Item 1 should succeed", results[1].getPrimaryResponse());
            assertTimeout(results[2].getPrimaryResponse(), "Item 2 (inference field after timeout)");
            assertNull("Item 3 (no inference field) should pass through", results[3].getPrimaryResponse());
        });

        assertThat(batchCount.get(), equalTo(1));
    }

    /**
     * Tests timeout check after model loading but before chunkedInfer is called.
     */
    public void testPreInferenceTimeoutAfterModelLoading() throws Exception {
        StaticModel model = StaticModel.createRandomInstance();
        String text = "test_input";
        model.putResult(text, randomChunkedInferenceEmbedding(model, List.of(text)));

        AtomicBoolean chunkedInferCalled = new AtomicBoolean(false);

        ShardBulkInferenceActionFilter filter = createFilterWithModelLoadingDelay(model, (inputs, listener) -> {
            chunkedInferCalled.set(true);
            return DelayResult.immediate();
        },
            SLOW_DELAY_MS  // Model loading exceeds timeout
        );

        runFilterAndVerify(filter, model, "field1", new String[] { text }, results -> {
            assertTimeout(results[0].getPrimaryResponse(), "Item should timeout after model loading");
        });

        assertFalse("chunkedInfer should not have been called", chunkedInferCalled.get());
    }

    /**
     * Tests that blank values skip inference and don't trigger timeout failures.
     */
    public void testBlankValuesDoNotTriggerTimeoutFailures() throws Exception {
        StaticModel model = StaticModel.createRandomInstance();
        String realText = "0000000000";
        model.putResult(realText, randomChunkedInferenceEmbedding(model, List.of(realText)));

        AtomicInteger batchCount = new AtomicInteger(0);

        ShardBulkInferenceActionFilter filter = createFilter(model, (inputs, listener) -> {
            int batch = batchCount.incrementAndGet();
            return batch == 1 ? DelayResult.delayThenComplete(SLOW_DELAY_MS) : DelayResult.immediate();
        });

        Map<String, InferenceFieldMetadata> fieldMap = Map.of("semantic_field", inferenceFieldMetadata("semantic_field", model));

        // Real text in batch 1, blank values in batch 2 (after timeout)
        BulkItemRequest[] items = {
            bulkItemRequest(0, "semantic_field", realText),
            bulkItemRequest(1, "semantic_field", ""),
            bulkItemRequest(2, "semantic_field", "   "),
            bulkItemRequest(3, "semantic_field", "\t\n") };

        runFilterAndVerify(filter, fieldMap, items, results -> {
            assertNull("Real text should succeed", results[0].getPrimaryResponse());
            // Blank values should pass through despite timeout
            for (int i = 1; i <= 3; i++) {
                assertNull("Blank value " + i + " should succeed", results[i].getPrimaryResponse());
            }
        });

        assertThat(batchCount.get(), equalTo(1));
    }

    /**
     * Tests that when a document has multiple inference fields and one times out, the entire document fails.
     */
    public void testMultipleInferenceFieldsPartialTimeout() throws Exception {
        StaticModel fastModel = StaticModel.createRandomInstance();
        StaticModel slowModel = StaticModel.createRandomInstance();

        String fastText = "fast_value";
        String slowText = "slow_value";

        fastModel.putResult(fastText, randomChunkedInferenceEmbedding(fastModel, List.of(fastText)));
        slowModel.putResult(slowText, randomChunkedInferenceEmbedding(slowModel, List.of(slowText)));

        ShardBulkInferenceActionFilter filter = createFilter(
            Map.of(fastModel.getInferenceEntityId(), fastModel, slowModel.getInferenceEntityId(), slowModel),
            (inputs, listener) -> inputs.get(0).inputText().startsWith("fast") ? DelayResult.immediate() : DelayResult.timeout()
        );

        Map<String, InferenceFieldMetadata> fieldMap = Map.of(
            "fast_field",
            inferenceFieldMetadata("fast_field", fastModel),
            "slow_field",
            inferenceFieldMetadata("slow_field", slowModel)
        );

        // Single document with both fields
        BulkItemRequest[] items = { new BulkItemRequest(0, randomDocWriteRequest(Map.of("fast_field", fastText, "slow_field", slowText))) };

        runFilterAndVerify(
            filter,
            fieldMap,
            items,
            results -> { assertTimeout(results[0].getPrimaryResponse(), "Document with partial timeout"); }
        );
    }

    /**
     * Tests that when timeout is set to zero, all inference items fail immediately
     * and items without inference fields still pass through.
     */
    public void testZeroTimeoutFailsAllInferenceItems() throws Exception {
        StaticModel model = StaticModel.createRandomInstance();
        String text = "some_text";
        model.putResult(text, randomChunkedInferenceEmbedding(model, List.of(text)));

        AtomicBoolean chunkedInferCalled = new AtomicBoolean(false);

        ShardBulkInferenceActionFilter filter = createFilterInternal(
            Map.of(model.getInferenceEntityId(), model),
            (inputs, listener) -> {
                chunkedInferCalled.set(true);
                return DelayResult.immediate();
            },
            TimeValue.ZERO,
            () -> 0
        );

        Map<String, InferenceFieldMetadata> fieldMap = Map.of(
            "semantic_field", inferenceFieldMetadata("semantic_field", model)
        );

        BulkItemRequest[] items = {
            bulkItemRequest(0, "semantic_field", text),
            bulkItemRequest(1, "semantic_field", text),
            bulkItemRequest(2, "other_field", "no inference needed")
        };

        runFilterAndVerify(filter, fieldMap, items, results -> {
            assertTimeout(results[0].getPrimaryResponse(), "Item 0 (inference field)");
            assertTimeout(results[1].getPrimaryResponse(), "Item 1 (inference field)");
            assertNull("Item 2 (no inference field) should pass through", results[2].getPrimaryResponse());
        });

        assertFalse("chunkedInfer should not have been called", chunkedInferCalled.get());
    }

    // ========== Helper Methods ==========

    private String[] createTextsAndRegisterResults(StaticModel model, int count) {
        String[] texts = new String[count];
        for (int i = 0; i < count; i++) {
            texts[i] = String.format(Locale.ROOT, "%010d", i);
            model.putResult(texts[i], randomChunkedInferenceEmbedding(model, List.of(texts[i])));
        }
        return texts;
    }

    private void assertTimeout(BulkItemResponse response, String itemDescription) {
        assertNotNull(itemDescription + " should have a response", response);
        assertTrue(itemDescription + " should be failed", response.isFailed());
        assertThat(itemDescription + " should have timeout status", response.getFailure().getStatus(), equalTo(RestStatus.REQUEST_TIMEOUT));
    }

    private void runFilterAndVerify(
        ShardBulkInferenceActionFilter filter,
        StaticModel model,
        String field,
        String[] texts,
        java.util.function.Consumer<BulkItemRequest[]> verifier
    ) throws Exception {
        Map<String, InferenceFieldMetadata> fieldMap = Map.of(field, inferenceFieldMetadata(field, model));
        BulkItemRequest[] items = new BulkItemRequest[texts.length];
        for (int i = 0; i < texts.length; i++) {
            items[i] = bulkItemRequest(i, field, texts[i]);
        }
        runFilterAndVerify(filter, fieldMap, items, verifier);
    }

    // ========== Filter Creation ==========

    private ShardBulkInferenceActionFilter createFilter(StaticModel model, InferenceDelayController delayController) {
        return createFilter(Map.of(model.getInferenceEntityId(), model), delayController);
    }

    private ShardBulkInferenceActionFilter createFilter(Map<String, StaticModel> modelMap, InferenceDelayController delayController) {
        return createFilterInternal(modelMap, delayController, DEFAULT_TIMEOUT, () -> 0);
    }

    private ShardBulkInferenceActionFilter createFilterWithModelLoadingDelay(
        StaticModel model,
        InferenceDelayController delayController,
        long modelLoadingDelayMs
    ) {
        return createFilterInternal(
            Map.of(model.getInferenceEntityId(), model),
            delayController,
            DEFAULT_TIMEOUT,
            () -> modelLoadingDelayMs
        );
    }

    @SuppressWarnings("unchecked")
    private ShardBulkInferenceActionFilter createFilterInternal(
        Map<String, StaticModel> modelMap,
        InferenceDelayController delayController,
        TimeValue timeout,
        ModelLoadingDelayController modelLoadingDelayController
    ) {
        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(InferencePlugin.INFERENCE_API_FEATURE)).thenReturn(true);

        ModelRegistry modelRegistry = mock(ModelRegistry.class);
        doAnswer(invocation -> {
            String id = (String) invocation.getArguments()[0];
            ActionListener<UnparsedModel> listener = (ActionListener<UnparsedModel>) invocation.getArguments()[1];
            var model = modelMap.get(id);

            Runnable response = () -> {
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
            };

            long delay = modelLoadingDelayController.getModelLoadingDelayMs();
            if (delay > 0) {
                threadPool.schedule(response, TimeValue.timeValueMillis(delay), threadPool.generic());
            } else {
                response.run();
            }
            return null;
        }).when(modelRegistry).getModelWithSecrets(any(), any());

        doAnswer(invocation -> {
            String inferenceId = (String) invocation.getArguments()[0];
            var model = modelMap.get(inferenceId);
            if (model == null) throw new ResourceNotFoundException("model id [{}] not found", inferenceId);
            return new MinimalServiceSettings(model);
        }).when(modelRegistry).getMinimalServiceSettings(any());

        InferenceService inferenceService = mock(InferenceService.class);
        doAnswer(invocation -> {
            StaticModel model = (StaticModel) invocation.getArguments()[0];
            List<ChunkInferenceInput> inputs = (List<ChunkInferenceInput>) invocation.getArguments()[2];
            TimeValue inferenceTimeout = (TimeValue) invocation.getArguments()[5];
            ActionListener<List<ChunkedInference>> listener = (ActionListener<List<ChunkedInference>>) invocation.getArguments()[6];

            DelayResult delayResult = delayController.getDelay(inputs, listener);

            if (delayResult.shouldTimeout()) {
                threadPool.schedule(
                    () -> listener.onFailure(new ElasticsearchStatusException("Request timed out", RestStatus.REQUEST_TIMEOUT)),
                    inferenceTimeout,
                    threadPool.generic()
                );
            } else if (delayResult.delayBeforeCompletionMs() > 0) {
                threadPool.schedule(() -> {
                    List<ChunkedInference> results = new ArrayList<>();
                    for (ChunkInferenceInput input : inputs) {
                        results.add(model.getResults(input.inputText()));
                    }
                    listener.onResponse(results);
                }, TimeValue.timeValueMillis(delayResult.delayBeforeCompletionMs()), threadPool.generic());
            } else {
                List<ChunkedInference> results = new ArrayList<>();
                for (ChunkInferenceInput input : inputs) {
                    results.add(model.getResults(input.inputText()));
                }
                listener.onResponse(results);
            }
            return null;
        }).when(inferenceService).chunkedInfer(any(), any(), any(), any(), any(), any(), any());

        doAnswer(invocation -> modelMap.get((String) invocation.getArguments()[0])).when(inferenceService)
            .parsePersistedConfigWithSecrets(any(), any(), any(), any());

        InferenceServiceRegistry registry = mock(InferenceServiceRegistry.class);
        when(registry.getService(any())).thenReturn(Optional.of(inferenceService));

        return new ShardBulkInferenceActionFilter(
            createClusterService(timeout),
            registry,
            modelRegistry,
            licenseState,
            NOOP_INDEXING_PRESSURE,
            InferenceStatsTests.mockInferenceStats()
        );
    }

    private ClusterService createClusterService(TimeValue timeout) {
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        var indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat)
            .build();
        when(indexMetadata.getSettings()).thenReturn(indexSettings);

        ProjectMetadata project = spy(ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID).build());
        when(project.index(anyString())).thenReturn(indexMetadata);

        Metadata metadata = mock(Metadata.class);
        when(metadata.getProject()).thenReturn(project);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        Settings settings = Settings.builder()
            .put(INDICES_INFERENCE_BATCH_SIZE.getKey(), ByteSizeValue.ofBytes(20))
            .put(INDICES_INFERENCE_BULK_TIMEOUT.getKey(), timeout)
            .build();
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.of(INDICES_INFERENCE_BATCH_SIZE, INDICES_INFERENCE_BULK_TIMEOUT))
        );
        return clusterService;
    }

<<<<<<< HEAD
    // ========== Test Execution ==========

    private void runFilterAndVerify(
        ShardBulkInferenceActionFilter filter,
        StaticModel model,
        String field,
        String[] texts,
        Consumer<BulkItemRequest[]> verifier
    ) throws Exception {
        Map<String, InferenceFieldMetadata> fieldMap = Map.of(field, inferenceFieldMetadata(field, model));
        BulkItemRequest[] items = new BulkItemRequest[texts.length];
        for (int i = 0; i < texts.length; i++) {
            items[i] = bulkItemRequest(i, field, texts[i]);
        }
        runFilterAndVerify(filter, fieldMap, items, verifier);
    }

    // ========== Supporting Types ==========

    @FunctionalInterface
    private interface InferenceDelayController {
        DelayResult getDelay(List<ChunkInferenceInput> inputs, ActionListener<List<ChunkedInference>> listener);
    }

    @FunctionalInterface
    private interface ModelLoadingDelayController {
        long getModelLoadingDelayMs();
    }

    private record DelayResult(boolean shouldTimeout, long delayBeforeCompletionMs) {
        static DelayResult immediate() {
            return new DelayResult(false, 0);
        }

        static DelayResult timeout() {
            return new DelayResult(true, 0);
        }

        static DelayResult delayThenComplete(long delayMs) {
            return new DelayResult(false, delayMs);
        }
    }
}
