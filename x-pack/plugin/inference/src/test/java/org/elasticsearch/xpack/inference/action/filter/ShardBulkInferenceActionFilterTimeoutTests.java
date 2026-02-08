/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.inference.telemetry.InferenceStatsTests;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.InferencePlugin.INDICES_INFERENCE_BULK_TIMEOUT;
import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.INDICES_INFERENCE_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomChunkedInferenceEmbedding;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

/**
 * Tests for timeout behavior in {@link ShardBulkInferenceActionFilter}.
 *
 * <p>Timeout scenarios tested:</p>
 * <ul>
 *   <li><b>Batch-level timeout</b>: When earlier batches consume time, later batches fail
 *       before inference starts.</li>
 *   <li><b>Pre-inference timeout after model loading</b>: If model loading takes long enough to
 *       exceed timeout, inference is skipped and items fail at the pre-inference check.</li>
 *   <li><b>Inference service timeout</b>: The timeout is passed to {@code chunkedInfer()} and the
 *       inference service enforces it. Different inference IDs are processed independently.</li>
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
     * Tests that timeout causes remaining inference items to fail while blank/null items pass through.
     * Randomly injects the timeout via one of two mechanisms:
     * <ul>
     *   <li><b>Inference delay</b>: a slow batch causes later batches to fail at the batch-level check.</li>
     *   <li><b>Model loading delay</b>: model loading exceeds the timeout, so all inference items
     *       fail at the pre-inference check and chunkedInfer is never called.</li>
     * </ul>
     */
    public void testTimeoutBasic() throws Exception {
        StaticModel model = StaticModel.createRandomInstance();
        int totalBatches = randomIntBetween(2, 5);
        int itemsPerBatch = 2;  // 20-byte batch size, 10-byte texts
        int totalItems = totalBatches * itemsPerBatch;
        int slowBatch = randomIntBetween(1, totalBatches - 1);

        String[] texts = createTextsAndRegisterResults(model, totalItems);
        AtomicInteger batchCount = new AtomicInteger(0);
        boolean useModelLoadingDelay = randomBoolean();

        ShardBulkInferenceActionFilter filter;
        if (useModelLoadingDelay) {
            filter = createFilterInternal(Map.of(model.getInferenceEntityId(), model), (inputs, listener) -> {
                batchCount.incrementAndGet();
                return DelayResult.immediate();
            }, DEFAULT_TIMEOUT, () -> SLOW_DELAY_MS);
        } else {
            filter = createFilter(model, (inputs, listener) -> {
                int batch = batchCount.incrementAndGet();
                return batch == slowBatch ? DelayResult.delayThenComplete(SLOW_DELAY_MS) : DelayResult.immediate();
            });
        }

        // Model loading delay: all inference items fail (timeout before any inference)
        // Inference delay: items before the slow batch succeed, rest fail
        int expectedSuccessItems = useModelLoadingDelay ? 0 : slowBatch * itemsPerBatch;

        Map<String, InferenceFieldMetadata> fieldMap = Map.of("field1", inferenceFieldMetadata("field1", model));

        BulkItemRequest[] items = new BulkItemRequest[totalItems + 4];
        for (int i = 0; i < totalItems; i++) {
            items[i] = bulkItemRequest(i, "field1", texts[i]);
        }
        items[totalItems] = bulkItemRequest(totalItems, "other_field", "no inference needed");
        items[totalItems + 1] = bulkItemRequest(totalItems + 1, "field1", "");
        items[totalItems + 2] = bulkItemRequest(totalItems + 2, "field1", "   ");
        items[totalItems + 3] = bulkItemRequest(totalItems + 3, "field1", null);

        runFilterAndVerify(filter, fieldMap, items, results -> {
            for (int i = 0; i < totalItems; i++) {
                BulkItemResponse response = results[i].getPrimaryResponse();
                if (i < expectedSuccessItems) {
                    assertNull("Item " + i + " should succeed", response);
                } else {
                    assertTimeoutWithMessage(response, "Item " + i, DEFAULT_TIMEOUT);
                }
            }
            assertNull("Non-inference field should pass through", results[totalItems].getPrimaryResponse());
            assertNull("Empty string should pass through", results[totalItems + 1].getPrimaryResponse());
            assertNull("Whitespace should pass through", results[totalItems + 2].getPrimaryResponse());
            assertNull("Null value should pass through", results[totalItems + 3].getPrimaryResponse());
        });

        if (useModelLoadingDelay) {
            assertThat("chunkedInfer should not have been called", batchCount.get(), equalTo(0));
        } else {
            assertThat(batchCount.get(), equalTo(slowBatch));
        }
    }

    /**
     * Tests that the timeout parameter is correctly passed to the inference service
     * and that the service enforces the timeout.
     * Items using different inference IDs are processed independently.
     */
    public void testInferenceServiceTimeout() throws Exception {
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
            bulkItemRequest(3, "slow_field", slowTexts[2]),
            bulkItemRequest(4, "slow_field", ""),
            bulkItemRequest(5, "slow_field", null) };

        runFilterAndVerify(filter, fieldMap, items, results -> {
            assertNull("Fast model item should succeed", results[0].getPrimaryResponse());
            for (int i = 1; i <= 3; i++) {
                assertTimeout(results[i].getPrimaryResponse(), "Slow model item " + i);
            }
            assertNull("Item 4 (empty slow_field) should pass through", results[4].getPrimaryResponse());
            assertNull("Item 5 (null slow_field) should pass through", results[5].getPrimaryResponse());
        });

        assertThat(inferenceCallCount.get(), equalTo(2));
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
     * Tests that when timeout is set to the minimum (1ms), inference items fail via timeout
     * and items without inference fields still pass through. With a 1ms timeout and model
     * loading delay that exceeds it, the pre-inference timeout check triggers.
     */
    public void testMinimalTimeoutFailsAllInferenceItems() throws Exception {
        StaticModel model = StaticModel.createRandomInstance();
        String text = "some_text";
        model.putResult(text, randomChunkedInferenceEmbedding(model, List.of(text)));

        AtomicBoolean chunkedInferCalled = new AtomicBoolean(false);

        // Use 1ms timeout with a model loading delay that guarantees the timeout expires
        // before inference starts (pre-inference timeout check triggers after model loading)
        ShardBulkInferenceActionFilter filter = createFilterInternal(Map.of(model.getInferenceEntityId(), model), (inputs, listener) -> {
            chunkedInferCalled.set(true);
            return DelayResult.immediate();
        }, TimeValue.timeValueMillis(1), () -> SLOW_DELAY_MS);

        Map<String, InferenceFieldMetadata> fieldMap = Map.of("semantic_field", inferenceFieldMetadata("semantic_field", model));

        BulkItemRequest[] items = {
            bulkItemRequest(0, "semantic_field", text),
            bulkItemRequest(1, "semantic_field", text),
            bulkItemRequest(2, "other_field", "no inference needed"),
            bulkItemRequest(3, "semantic_field", ""),
            bulkItemRequest(4, "semantic_field", "   "),
            bulkItemRequest(5, "semantic_field", null) };

        runFilterAndVerify(filter, fieldMap, items, results -> {
            assertTimeoutWithMessage(results[0].getPrimaryResponse(), "Item 0 (inference field)", TimeValue.timeValueMillis(1));
            assertTimeoutWithMessage(results[1].getPrimaryResponse(), "Item 1 (inference field)", TimeValue.timeValueMillis(1));
            assertNull("Item 2 (no inference field) should pass through", results[2].getPrimaryResponse());
            assertNull("Item 3 (empty string) should pass through", results[3].getPrimaryResponse());
            assertNull("Item 4 (whitespace) should pass through", results[4].getPrimaryResponse());
            assertNull("Item 5 (null value) should pass through", results[5].getPrimaryResponse());
        });

        assertFalse("chunkedInfer should not have been called", chunkedInferCalled.get());
    }

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

    /**
     * Asserts that the response is a timeout failure with the expected error message format
     * from the pre-inference timeout check: "Bulk inference timed out after [elapsed] (configured timeout for bulk request: [timeout])".
     * The {@link ElasticsearchStatusException} is wrapped in an {@code InferenceException}, so we check the cause chain.
     */
    private void assertTimeoutWithMessage(BulkItemResponse response, String itemDescription, TimeValue expectedConfiguredTimeout) {
        assertTimeout(response, itemDescription);
        Throwable cause = response.getFailure().getCause().getCause();
        assertNotNull(itemDescription + " should have a root cause", cause);
        String message = cause.getMessage();
        assertThat(itemDescription + " should mention timed out", message, containsString("Bulk inference timed out after"));
        assertThat(itemDescription + " should mention configured timeout", message, containsString("configured timeout for bulk request:"));
        assertThat(
            itemDescription + " should include configured timeout value",
            message,
            containsString(expectedConfiguredTimeout.toString())
        );
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

    private ShardBulkInferenceActionFilter createFilter(StaticModel model, InferenceDelayController delayController) {
        return createFilter(Map.of(model.getInferenceEntityId(), model), delayController);
    }

    private ShardBulkInferenceActionFilter createFilter(Map<String, StaticModel> modelMap, InferenceDelayController delayController) {
        return createFilterInternal(modelMap, delayController, DEFAULT_TIMEOUT, () -> 0);
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

        Answer<?> defaultModelAnswer = defaultGetModelWithSecretsAnswer(modelMap);
        Answer<?> modelLoadingAnswer = invocation -> {
            long delay = modelLoadingDelayController.getModelLoadingDelayMs();
            if (delay > 0) {
                ActionListener<UnparsedModel> listener = (ActionListener<UnparsedModel>) invocation.getArguments()[1];
                threadPool.schedule(() -> {
                    try {
                        defaultModelAnswer.answer(invocation);
                    } catch (Throwable e) {
                        listener.onFailure(e instanceof Exception ? (Exception) e : new RuntimeException(e));
                    }
                }, TimeValue.timeValueMillis(delay), threadPool.generic());
                return null;
            }
            return defaultModelAnswer.answer(invocation);
        };

        Answer<?> chunkedInferAnswer = invocation -> {
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
        };

        return super.createFilter(
            modelMap,
            createClusterService(timeout),
            licenseState,
            NOOP_INDEXING_PRESSURE,
            InferenceStatsTests.mockInferenceStats(),
            modelLoadingAnswer,
            chunkedInferAnswer
        );
    }

    private ClusterService createClusterService(TimeValue timeout) {
        Settings settings = Settings.builder()
            .put(INDICES_INFERENCE_BATCH_SIZE.getKey(), ByteSizeValue.ofBytes(20))
            .put(INDICES_INFERENCE_BULK_TIMEOUT.getKey(), timeout)
            .build();
        return createClusterService(useLegacyFormat, settings);
    }

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
