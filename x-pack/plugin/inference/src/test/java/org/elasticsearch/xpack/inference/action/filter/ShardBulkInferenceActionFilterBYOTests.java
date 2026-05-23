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
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.WriteRequest;
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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.inference.telemetry.InferenceStatsTests;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapperTests;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.INDICES_INFERENCE_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.getIndexRequestOrNull;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for BYO (bring-your-own) semantic_text handling in {@link ShardBulkInferenceActionFilter}.
 * Covers single-shot BYO, multi-part staging lifecycle, error conditions, and backward compatibility.
 */
public class ShardBulkInferenceActionFilterBYOTests extends ESTestCase {

    private static final IndexingPressure NOOP_INDEXING_PRESSURE = new NoopIndexingPressure();
    private ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() throws Exception {
        terminate(threadPool);
    }

    // ---- Single-shot tests ----

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testSingleShotBYOValidDocument() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        int dims = 3;
        StaticModel model = createModel(inferenceId, dims);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        Map<String, Object> byoValue = new LinkedHashMap<>();
        byoValue.put("text", "hello world");
        byoValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3)), chunkMap(5, 11, List.of(0.4, 0.5, 0.6))));

        CountDownLatch latch = new CountDownLatch(1);
        ActionFilterChain chain = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkReq = (BulkShardRequest) request;
                BulkItemRequest item = bulkReq.items()[0];
                // BYO should not set a failure
                assertNull(item.getPrimaryResponse());
                IndexRequest indexReq = getIndexRequestOrNull(item.request());
                Map<String, Object> source = indexReq.sourceAsMap();

                // The source field value should have been removed
                assertNull(source.get(fieldName));

                // Inference metadata should be present
                Map<String, Object> inferenceMetadata = (Map<String, Object>) source.get(InferenceMetadataFieldsMapper.NAME);
                assertThat(inferenceMetadata, notNullValue());
                Map<String, Object> fieldMeta = (Map<String, Object>) inferenceMetadata.get(fieldName);
                assertThat(fieldMeta, notNullValue());
                // Should have inference field with chunks
                Map<String, Object> inference = (Map<String, Object>) fieldMeta.get("inference");
                assertThat(inference, notNullValue());
                assertThat(inference.get("inference_id"), equalTo(inferenceId));
            } finally {
                latch.countDown();
            }
        };

        applyFilter(filter, fieldName, inferenceId, byoValue, chain);
        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testSingleShotBYOMissingText() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // Missing 'text' key entirely, but has chunks — this is not detected as BYO by isBYOValue
        // because it requires both 'text' and 'chunks' for single-shot.
        // So let's provide text as empty string.
        Map<String, Object> byoValue = new LinkedHashMap<>();
        byoValue.put("text", "");
        byoValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3))));

        CountDownLatch latch = new CountDownLatch(1);
        ActionFilterChain chain = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkReq = (BulkShardRequest) request;
                BulkItemRequest item = bulkReq.items()[0];
                assertThat(item.getPrimaryResponse(), notNullValue());
                assertTrue(item.getPrimaryResponse().isFailed());
                assertThat(item.getPrimaryResponse().getFailure().getMessage(), containsString("non-empty 'text'"));
            } finally {
                latch.countDown();
            }
        };

        applyFilter(filter, fieldName, inferenceId, byoValue, chain);
        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testSingleShotBYOGapInChunks() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // Gap: [0,3) and [5,10) — missing [3,5)
        Map<String, Object> byoValue = new LinkedHashMap<>();
        byoValue.put("text", "0123456789");
        byoValue.put("chunks", List.of(chunkMap(0, 3, List.of(0.1, 0.2, 0.3)), chunkMap(5, 10, List.of(0.4, 0.5, 0.6))));

        CountDownLatch latch = new CountDownLatch(1);
        ActionFilterChain chain = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkReq = (BulkShardRequest) request;
                BulkItemRequest item = bulkReq.items()[0];
                assertThat(item.getPrimaryResponse(), notNullValue());
                assertTrue(item.getPrimaryResponse().isFailed());
                assertThat(item.getPrimaryResponse().getFailure().getMessage(), containsString("Gap"));
            } finally {
                latch.countDown();
            }
        };

        applyFilter(filter, fieldName, inferenceId, byoValue, chain);
        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testSingleShotBYOOverlappingChunks() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // Overlap: [0,5) and [3,10)
        Map<String, Object> byoValue = new LinkedHashMap<>();
        byoValue.put("text", "0123456789");
        byoValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3)), chunkMap(3, 10, List.of(0.4, 0.5, 0.6))));

        CountDownLatch latch = new CountDownLatch(1);
        ActionFilterChain chain = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkReq = (BulkShardRequest) request;
                BulkItemRequest item = bulkReq.items()[0];
                assertThat(item.getPrimaryResponse(), notNullValue());
                assertTrue(item.getPrimaryResponse().isFailed());
                assertThat(item.getPrimaryResponse().getFailure().getMessage(), containsString("overlaps"));
            } finally {
                latch.countDown();
            }
        };

        applyFilter(filter, fieldName, inferenceId, byoValue, chain);
        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testSingleShotBYOEndOffsetExceedsTextLength() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // Text is 5 chars, but chunk claims [0,10)
        Map<String, Object> byoValue = new LinkedHashMap<>();
        byoValue.put("text", "hello");
        byoValue.put("chunks", List.of(chunkMap(0, 10, List.of(0.1, 0.2, 0.3))));

        CountDownLatch latch = new CountDownLatch(1);
        ActionFilterChain chain = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkReq = (BulkShardRequest) request;
                BulkItemRequest item = bulkReq.items()[0];
                assertThat(item.getPrimaryResponse(), notNullValue());
                assertTrue(item.getPrimaryResponse().isFailed());
                assertThat(item.getPrimaryResponse().getFailure().getMessage(), containsString("exceeds text_length"));
            } finally {
                latch.countDown();
            }
        };

        applyFilter(filter, fieldName, inferenceId, byoValue, chain);
        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    // ---- Staging lifecycle tests ----

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testStageInitCreatesStaged() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        Map<String, Object> stageInitValue = new LinkedHashMap<>();
        stageInitValue.put("_action", "stage_init");
        stageInitValue.put("text", "hello world");

        CountDownLatch latch = new CountDownLatch(1);
        ActionFilterChain chain = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkReq = (BulkShardRequest) request;
                BulkItemRequest item = bulkReq.items()[0];
                assertNull(item.getPrimaryResponse());
                Map<String, Object> source = getIndexRequestOrNull(item.request()).sourceAsMap();
                Map<String, Object> staged = extractStaged(source, fieldName);
                assertThat(staged, notNullValue());
                assertThat(staged.get("text"), equalTo("hello world"));
                assertThat(staged.get("text_length"), equalTo(11));
            } finally {
                latch.countDown();
            }
        };

        applyFilter(filter, fieldName, inferenceId, stageInitValue, chain);
        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testStageAppendsChunks() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // Step 1: stage_init with first chunk
        Map<String, Object> stageInitValue = new LinkedHashMap<>();
        stageInitValue.put("_action", "stage_init");
        stageInitValue.put("text", "hello world");
        stageInitValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3))));

        Map<String, Object>[] capturedSource = new Map[1];
        CountDownLatch latch1 = new CountDownLatch(1);
        ActionFilterChain chain1 = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkReq = (BulkShardRequest) request;
                capturedSource[0] = getIndexRequestOrNull(bulkReq.items()[0].request()).sourceAsMap();
                Map<String, Object> staged = extractStaged(capturedSource[0], fieldName);
                assertThat(staged, notNullValue());
                List<?> chunks = (List<?>) staged.get("chunks");
                assertThat(chunks.size(), equalTo(1));
            } finally {
                latch1.countDown();
            }
        };
        applyFilter(filter, fieldName, inferenceId, stageInitValue, chain1);
        awaitLatch(latch1, 10, TimeUnit.SECONDS);

        // Step 2: stage with second chunk — build source from previous result
        Map<String, Object> stageValue = new LinkedHashMap<>();
        stageValue.put("_action", "stage");
        stageValue.put("chunks", List.of(chunkMap(5, 11, List.of(0.4, 0.5, 0.6))));

        Map<String, Object> docSource = new LinkedHashMap<>(capturedSource[0]);
        docSource.put(fieldName, stageValue);

        CountDownLatch latch2 = new CountDownLatch(1);
        ActionFilterChain chain2 = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkReq = (BulkShardRequest) request;
                BulkItemRequest item = bulkReq.items()[0];
                assertNull(item.getPrimaryResponse());
                Map<String, Object> source = getIndexRequestOrNull(item.request()).sourceAsMap();
                Map<String, Object> staged = extractStaged(source, fieldName);
                assertThat(staged, notNullValue());
                List<?> chunks = (List<?>) staged.get("chunks");
                assertThat(chunks.size(), equalTo(2));
            } finally {
                latch2.countDown();
            }
        };
        applyFilterWithSource(filter, fieldName, inferenceId, docSource, chain2);
        awaitLatch(latch2, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testStageOutOfOrderChunks() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // stage_init with chunk at end
        Map<String, Object> stageInitValue = new LinkedHashMap<>();
        stageInitValue.put("_action", "stage_init");
        stageInitValue.put("text", "hello world");
        stageInitValue.put("chunks", List.of(chunkMap(5, 11, List.of(0.4, 0.5, 0.6))));

        Map<String, Object>[] capturedSource = new Map[1];
        CountDownLatch latch1 = new CountDownLatch(1);
        ActionFilterChain chain1 = (task, action, request, listener) -> {
            try {
                capturedSource[0] = getIndexRequestOrNull(((BulkShardRequest) request).items()[0].request()).sourceAsMap();
            } finally {
                latch1.countDown();
            }
        };
        applyFilter(filter, fieldName, inferenceId, stageInitValue, chain1);
        awaitLatch(latch1, 10, TimeUnit.SECONDS);

        // Stage chunk at beginning
        Map<String, Object> stageValue = new LinkedHashMap<>();
        stageValue.put("_action", "stage");
        stageValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3))));

        Map<String, Object> docSource = new LinkedHashMap<>(capturedSource[0]);
        docSource.put(fieldName, stageValue);

        CountDownLatch latch2 = new CountDownLatch(1);
        ActionFilterChain chain2 = (task, action, request, listener) -> {
            try {
                BulkItemRequest item = ((BulkShardRequest) request).items()[0];
                assertNull(item.getPrimaryResponse());
                Map<String, Object> source = getIndexRequestOrNull(item.request()).sourceAsMap();
                Map<String, Object> staged = extractStaged(source, fieldName);
                List<?> chunks = (List<?>) staged.get("chunks");
                assertThat(chunks.size(), equalTo(2));
            } finally {
                latch2.countDown();
            }
        };
        applyFilterWithSource(filter, fieldName, inferenceId, docSource, chain2);
        awaitLatch(latch2, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testCommitPromotesChunks() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // Step 1: stage_init with all chunks
        Map<String, Object> stageInitValue = new LinkedHashMap<>();
        stageInitValue.put("_action", "stage_init");
        stageInitValue.put("text", "hello world");
        stageInitValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3)), chunkMap(5, 11, List.of(0.4, 0.5, 0.6))));

        Map<String, Object>[] capturedSource = new Map[1];
        CountDownLatch latch1 = new CountDownLatch(1);
        ActionFilterChain chain1 = (task, action, request, listener) -> {
            try {
                capturedSource[0] = getIndexRequestOrNull(((BulkShardRequest) request).items()[0].request()).sourceAsMap();
            } finally {
                latch1.countDown();
            }
        };
        applyFilter(filter, fieldName, inferenceId, stageInitValue, chain1);
        awaitLatch(latch1, 10, TimeUnit.SECONDS);

        // Step 2: commit
        Map<String, Object> commitValue = new LinkedHashMap<>();
        commitValue.put("_action", "commit");

        Map<String, Object> docSource = new LinkedHashMap<>(capturedSource[0]);
        docSource.put(fieldName, commitValue);

        CountDownLatch latch2 = new CountDownLatch(1);
        ActionFilterChain chain2 = (task, action, request, listener) -> {
            try {
                BulkItemRequest item = ((BulkShardRequest) request).items()[0];
                assertNull(item.getPrimaryResponse());
                Map<String, Object> source = getIndexRequestOrNull(item.request()).sourceAsMap();

                // _staged should be gone
                assertThat(extractStaged(source, fieldName), nullValue());

                // Committed inference metadata should be present
                Map<String, Object> inferenceMetadata = (Map<String, Object>) source.get(InferenceMetadataFieldsMapper.NAME);
                assertThat(inferenceMetadata, notNullValue());
                Map<String, Object> fieldMeta = (Map<String, Object>) inferenceMetadata.get(fieldName);
                assertThat(fieldMeta, notNullValue());
                Map<String, Object> inference = (Map<String, Object>) fieldMeta.get("inference");
                assertThat(inference, notNullValue());
                assertThat(inference.get("inference_id"), equalTo(inferenceId));
            } finally {
                latch2.countDown();
            }
        };
        applyFilterWithSource(filter, fieldName, inferenceId, docSource, chain2);
        awaitLatch(latch2, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testCommitWithFinalChunks() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // Step 1: stage_init with first chunk only
        Map<String, Object> stageInitValue = new LinkedHashMap<>();
        stageInitValue.put("_action", "stage_init");
        stageInitValue.put("text", "hello world");
        stageInitValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3))));

        Map<String, Object>[] capturedSource = new Map[1];
        CountDownLatch latch1 = new CountDownLatch(1);
        ActionFilterChain chain1 = (task, action, request, listener) -> {
            try {
                capturedSource[0] = getIndexRequestOrNull(((BulkShardRequest) request).items()[0].request()).sourceAsMap();
            } finally {
                latch1.countDown();
            }
        };
        applyFilter(filter, fieldName, inferenceId, stageInitValue, chain1);
        awaitLatch(latch1, 10, TimeUnit.SECONDS);

        // Step 2: commit with final chunk included
        Map<String, Object> commitValue = new LinkedHashMap<>();
        commitValue.put("_action", "commit");
        commitValue.put("chunks", List.of(chunkMap(5, 11, List.of(0.4, 0.5, 0.6))));

        Map<String, Object> docSource = new LinkedHashMap<>(capturedSource[0]);
        docSource.put(fieldName, commitValue);

        CountDownLatch latch2 = new CountDownLatch(1);
        ActionFilterChain chain2 = (task, action, request, listener) -> {
            try {
                BulkItemRequest item = ((BulkShardRequest) request).items()[0];
                assertNull(item.getPrimaryResponse());
                Map<String, Object> source = getIndexRequestOrNull(item.request()).sourceAsMap();
                assertThat(extractStaged(source, fieldName), nullValue());
                Map<String, Object> inferenceMetadata = (Map<String, Object>) source.get(InferenceMetadataFieldsMapper.NAME);
                assertThat(inferenceMetadata, notNullValue());
            } finally {
                latch2.countDown();
            }
        };
        applyFilterWithSource(filter, fieldName, inferenceId, docSource, chain2);
        awaitLatch(latch2, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testCancelClearsStaged() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // Step 1: stage_init
        Map<String, Object> stageInitValue = new LinkedHashMap<>();
        stageInitValue.put("_action", "stage_init");
        stageInitValue.put("text", "hello world");

        Map<String, Object>[] capturedSource = new Map[1];
        CountDownLatch latch1 = new CountDownLatch(1);
        ActionFilterChain chain1 = (task, action, request, listener) -> {
            try {
                capturedSource[0] = getIndexRequestOrNull(((BulkShardRequest) request).items()[0].request()).sourceAsMap();
                assertThat(extractStaged(capturedSource[0], fieldName), notNullValue());
            } finally {
                latch1.countDown();
            }
        };
        applyFilter(filter, fieldName, inferenceId, stageInitValue, chain1);
        awaitLatch(latch1, 10, TimeUnit.SECONDS);

        // Step 2: cancel
        Map<String, Object> cancelValue = new LinkedHashMap<>();
        cancelValue.put("_action", "cancel");

        Map<String, Object> docSource = new LinkedHashMap<>(capturedSource[0]);
        docSource.put(fieldName, cancelValue);

        CountDownLatch latch2 = new CountDownLatch(1);
        ActionFilterChain chain2 = (task, action, request, listener) -> {
            try {
                BulkItemRequest item = ((BulkShardRequest) request).items()[0];
                assertNull(item.getPrimaryResponse());
                Map<String, Object> source = getIndexRequestOrNull(item.request()).sourceAsMap();
                // _staged should be removed
                Map<String, Object> inferenceMetadata = (Map<String, Object>) source.get(InferenceMetadataFieldsMapper.NAME);
                if (inferenceMetadata != null) {
                    Map<String, Object> fieldMeta = (Map<String, Object>) inferenceMetadata.get(fieldName);
                    if (fieldMeta != null) {
                        assertThat(fieldMeta.get("_staged"), nullValue());
                    }
                }
            } finally {
                latch2.countDown();
            }
        };
        applyFilterWithSource(filter, fieldName, inferenceId, docSource, chain2);
        awaitLatch(latch2, 10, TimeUnit.SECONDS);
    }

    // ---- Error condition tests ----

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testStageWithoutInitFails() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        Map<String, Object> stageValue = new LinkedHashMap<>();
        stageValue.put("_action", "stage");
        stageValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3))));

        CountDownLatch latch = new CountDownLatch(1);
        ActionFilterChain chain = (task, action, request, listener) -> {
            try {
                BulkItemRequest item = ((BulkShardRequest) request).items()[0];
                assertThat(item.getPrimaryResponse(), notNullValue());
                assertTrue(item.getPrimaryResponse().isFailed());
                assertThat(item.getPrimaryResponse().getFailure().getMessage(), containsString("no staged data"));
            } finally {
                latch.countDown();
            }
        };
        applyFilter(filter, fieldName, inferenceId, stageValue, chain);
        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testCommitWithoutInitFails() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        Map<String, Object> commitValue = new LinkedHashMap<>();
        commitValue.put("_action", "commit");

        CountDownLatch latch = new CountDownLatch(1);
        ActionFilterChain chain = (task, action, request, listener) -> {
            try {
                BulkItemRequest item = ((BulkShardRequest) request).items()[0];
                assertThat(item.getPrimaryResponse(), notNullValue());
                assertTrue(item.getPrimaryResponse().isFailed());
                assertThat(item.getPrimaryResponse().getFailure().getMessage(), containsString("no staged data"));
            } finally {
                latch.countDown();
            }
        };
        applyFilter(filter, fieldName, inferenceId, commitValue, chain);
        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testDoubleStageInitFails() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // First stage_init
        Map<String, Object> stageInitValue = new LinkedHashMap<>();
        stageInitValue.put("_action", "stage_init");
        stageInitValue.put("text", "hello world");

        Map<String, Object>[] capturedSource = new Map[1];
        CountDownLatch latch1 = new CountDownLatch(1);
        ActionFilterChain chain1 = (task, action, request, listener) -> {
            try {
                capturedSource[0] = getIndexRequestOrNull(((BulkShardRequest) request).items()[0].request()).sourceAsMap();
            } finally {
                latch1.countDown();
            }
        };
        applyFilter(filter, fieldName, inferenceId, stageInitValue, chain1);
        awaitLatch(latch1, 10, TimeUnit.SECONDS);

        // Second stage_init should fail
        Map<String, Object> stageInit2 = new LinkedHashMap<>();
        stageInit2.put("_action", "stage_init");
        stageInit2.put("text", "different text");

        Map<String, Object> docSource = new LinkedHashMap<>(capturedSource[0]);
        docSource.put(fieldName, stageInit2);

        CountDownLatch latch2 = new CountDownLatch(1);
        ActionFilterChain chain2 = (task, action, request, listener) -> {
            try {
                BulkItemRequest item = ((BulkShardRequest) request).items()[0];
                assertThat(item.getPrimaryResponse(), notNullValue());
                assertTrue(item.getPrimaryResponse().isFailed());
                assertThat(item.getPrimaryResponse().getFailure().getMessage(), containsString("already has staged data"));
            } finally {
                latch2.countDown();
            }
        };
        applyFilterWithSource(filter, fieldName, inferenceId, docSource, chain2);
        awaitLatch(latch2, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testStageOverlappingChunksAcrossBatches() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // stage_init with chunk [0,5)
        Map<String, Object> stageInitValue = new LinkedHashMap<>();
        stageInitValue.put("_action", "stage_init");
        stageInitValue.put("text", "hello world");
        stageInitValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3))));

        Map<String, Object>[] capturedSource = new Map[1];
        CountDownLatch latch1 = new CountDownLatch(1);
        ActionFilterChain chain1 = (task, action, request, listener) -> {
            try {
                capturedSource[0] = getIndexRequestOrNull(((BulkShardRequest) request).items()[0].request()).sourceAsMap();
            } finally {
                latch1.countDown();
            }
        };
        applyFilter(filter, fieldName, inferenceId, stageInitValue, chain1);
        awaitLatch(latch1, 10, TimeUnit.SECONDS);

        // Stage with overlapping chunk [3,11) — overlaps with [0,5)
        Map<String, Object> stageValue = new LinkedHashMap<>();
        stageValue.put("_action", "stage");
        stageValue.put("chunks", List.of(chunkMap(3, 11, List.of(0.4, 0.5, 0.6))));

        Map<String, Object> docSource = new LinkedHashMap<>(capturedSource[0]);
        docSource.put(fieldName, stageValue);

        CountDownLatch latch2 = new CountDownLatch(1);
        ActionFilterChain chain2 = (task, action, request, listener) -> {
            try {
                BulkItemRequest item = ((BulkShardRequest) request).items()[0];
                assertThat(item.getPrimaryResponse(), notNullValue());
                assertTrue(item.getPrimaryResponse().isFailed());
                assertThat(item.getPrimaryResponse().getFailure().getMessage(), containsString("overlaps"));
            } finally {
                latch2.countDown();
            }
        };
        applyFilterWithSource(filter, fieldName, inferenceId, docSource, chain2);
        awaitLatch(latch2, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testSameChunkTwiceInDifferentBatches() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // stage_init with chunk [0,5)
        Map<String, Object> stageInitValue = new LinkedHashMap<>();
        stageInitValue.put("_action", "stage_init");
        stageInitValue.put("text", "hello world");
        stageInitValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3))));

        Map<String, Object>[] capturedSource = new Map[1];
        CountDownLatch latch1 = new CountDownLatch(1);
        ActionFilterChain chain1 = (task, action, request, listener) -> {
            try {
                capturedSource[0] = getIndexRequestOrNull(((BulkShardRequest) request).items()[0].request()).sourceAsMap();
            } finally {
                latch1.countDown();
            }
        };
        applyFilter(filter, fieldName, inferenceId, stageInitValue, chain1);
        awaitLatch(latch1, 10, TimeUnit.SECONDS);

        // Stage exact same chunk [0,5) again — should fail as overlap
        Map<String, Object> stageValue = new LinkedHashMap<>();
        stageValue.put("_action", "stage");
        stageValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.7, 0.8, 0.9))));

        Map<String, Object> docSource = new LinkedHashMap<>(capturedSource[0]);
        docSource.put(fieldName, stageValue);

        CountDownLatch latch2 = new CountDownLatch(1);
        ActionFilterChain chain2 = (task, action, request, listener) -> {
            try {
                BulkItemRequest item = ((BulkShardRequest) request).items()[0];
                assertThat(item.getPrimaryResponse(), notNullValue());
                assertTrue(item.getPrimaryResponse().isFailed());
                assertThat(item.getPrimaryResponse().getFailure().getMessage(), containsString("overlaps"));
            } finally {
                latch2.countDown();
            }
        };
        applyFilterWithSource(filter, fieldName, inferenceId, docSource, chain2);
        awaitLatch(latch2, 10, TimeUnit.SECONDS);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testCommitWithIncompleteCoverageFails() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // stage_init with only first chunk
        Map<String, Object> stageInitValue = new LinkedHashMap<>();
        stageInitValue.put("_action", "stage_init");
        stageInitValue.put("text", "hello world");
        stageInitValue.put("chunks", List.of(chunkMap(0, 5, List.of(0.1, 0.2, 0.3))));

        Map<String, Object>[] capturedSource = new Map[1];
        CountDownLatch latch1 = new CountDownLatch(1);
        ActionFilterChain chain1 = (task, action, request, listener) -> {
            try {
                capturedSource[0] = getIndexRequestOrNull(((BulkShardRequest) request).items()[0].request()).sourceAsMap();
            } finally {
                latch1.countDown();
            }
        };
        applyFilter(filter, fieldName, inferenceId, stageInitValue, chain1);
        awaitLatch(latch1, 10, TimeUnit.SECONDS);

        // Commit without covering [5,11)
        Map<String, Object> commitValue = new LinkedHashMap<>();
        commitValue.put("_action", "commit");

        Map<String, Object> docSource = new LinkedHashMap<>(capturedSource[0]);
        docSource.put(fieldName, commitValue);

        CountDownLatch latch2 = new CountDownLatch(1);
        ActionFilterChain chain2 = (task, action, request, listener) -> {
            try {
                BulkItemRequest item = ((BulkShardRequest) request).items()[0];
                assertThat(item.getPrimaryResponse(), notNullValue());
                assertTrue(item.getPrimaryResponse().isFailed());
                assertThat(item.getPrimaryResponse().getFailure().getMessage(), containsString("Gap"));
            } finally {
                latch2.countDown();
            }
        };
        applyFilterWithSource(filter, fieldName, inferenceId, docSource, chain2);
        awaitLatch(latch2, 10, TimeUnit.SECONDS);
    }

    // ---- Backward compatibility ----

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testStringValueStillTriggersInference() throws Exception {
        String fieldName = "semantic_field";
        String inferenceId = "my_inference";
        StaticModel model = createModel(inferenceId, 3);
        ShardBulkInferenceActionFilter filter = createFilter(Map.of(inferenceId, model));

        // Plain string value — should go through normal inference path, not BYO
        String textValue = "just some text";
        // For non-BYO, the model needs to have a result cached
        model.putResult(textValue, randomChunkedInferenceEmbedding(model, List.of(textValue)));

        CountDownLatch latch = new CountDownLatch(1);
        ActionFilterChain chain = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkReq = (BulkShardRequest) request;
                BulkItemRequest item = bulkReq.items()[0];
                // Should have been processed by normal inference, not BYO
                assertNull(item.getPrimaryResponse());
                Map<String, Object> source = getIndexRequestOrNull(item.request()).sourceAsMap();
                // For non-legacy, inference metadata should be present
                Map<String, Object> inferenceMetadata = (Map<String, Object>) source.get(InferenceMetadataFieldsMapper.NAME);
                assertThat(inferenceMetadata, notNullValue());
            } finally {
                latch.countDown();
            }
        };

        applyFilter(filter, fieldName, inferenceId, textValue, chain);
        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    // ---- Helper methods ----

    private static Map<String, Object> chunkMap(int startOffset, int endOffset, List<Double> values) {
        Map<String, Object> chunk = new LinkedHashMap<>();
        chunk.put("start_offset", startOffset);
        chunk.put("end_offset", endOffset);
        Map<String, Object> embeddings = new LinkedHashMap<>();
        embeddings.put("values", values);
        chunk.put("embeddings", embeddings);
        return chunk;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractStaged(Map<String, Object> source, String fieldName) {
        Map<String, Object> inferenceMetadata = (Map<String, Object>) source.get(InferenceMetadataFieldsMapper.NAME);
        if (inferenceMetadata == null) return null;
        Map<String, Object> fieldMeta = (Map<String, Object>) inferenceMetadata.get(fieldName);
        if (fieldMeta == null) return null;
        return (Map<String, Object>) fieldMeta.get("_staged");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void applyFilter(
        ShardBulkInferenceActionFilter filter,
        String fieldName,
        String inferenceId,
        Object fieldValue,
        ActionFilterChain chain
    ) {
        Map<String, Object> docMap = new LinkedHashMap<>();
        docMap.put(fieldName, fieldValue);
        applyFilterWithSource(filter, fieldName, inferenceId, docMap, chain);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void applyFilterWithSource(
        ShardBulkInferenceActionFilter filter,
        String fieldName,
        String inferenceId,
        Map<String, Object> docSource,
        ActionFilterChain chain
    ) {
        Map<String, InferenceFieldMetadata> inferenceFieldMap = Map.of(
            fieldName,
            new InferenceFieldMetadata(fieldName, inferenceId, new String[] { fieldName }, null)
        );
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, new IndexRequest("test").source(docSource, XContentType.JSON)) };
        BulkShardRequest request = new BulkShardRequest(new ShardId("test", "test", 0), WriteRequest.RefreshPolicy.NONE, items);
        request.setInferenceFieldMap(inferenceFieldMap);

        ActionListener actionListener = mock(ActionListener.class);
        Task task = mock(Task.class);
        filter.apply(task, TransportShardBulkAction.ACTION_NAME, request, actionListener, chain);
    }

    private ShardBulkInferenceActionFilter createFilter(Map<String, StaticModel> modelMap) {
        return createFilter(threadPool, modelMap);
    }

    @SuppressWarnings("unchecked")
    private static ShardBulkInferenceActionFilter createFilter(ThreadPool threadPool, Map<String, StaticModel> modelMap) {
        InferenceStats inferenceStats = InferenceStatsTests.mockInferenceStats();
        MockLicenseState licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(InferencePlugin.INFERENCE_API_FEATURE)).thenReturn(true);

        ModelRegistry modelRegistry = mock(ModelRegistry.class);
        doAnswer(invocation -> {
            String id = (String) invocation.getArguments()[0];
            ActionListener<UnparsedModel> listener = (ActionListener<UnparsedModel>) invocation.getArguments()[1];
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
        }).when(modelRegistry).getModelWithSecrets(any(), any());

        doAnswer(invocation -> {
            String inferenceId = (String) invocation.getArguments()[0];
            var model = modelMap.get(inferenceId);
            if (model == null) {
                throw new ResourceNotFoundException("model id [{}] not found", inferenceId);
            }
            return new MinimalServiceSettings(model);
        }).when(modelRegistry).getMinimalServiceSettings(anyString());

        doAnswer(invocation -> {
            Set<String> inferenceIds = (Set<String>) invocation.getArguments()[0];
            Map<String, MinimalServiceSettings> result = new HashMap<>();
            for (String id : inferenceIds) {
                var model = modelMap.get(id);
                if (model != null) {
                    result.put(id, new MinimalServiceSettings(model));
                }
            }
            return result;
        }).when(modelRegistry).getMinimalServiceSettings(any(Set.class), anyBoolean());

        InferenceService inferenceService = mock(InferenceService.class);
        doAnswer(invocation -> {
            StaticModel model = (StaticModel) invocation.getArguments()[0];
            List<ChunkInferenceInput> inputs = (List<ChunkInferenceInput>) invocation.getArguments()[2];
            ActionListener<List<ChunkedInference>> listener = (ActionListener<List<ChunkedInference>>) invocation.getArguments()[6];

            List<ChunkedInference> results = new ArrayList<>();
            try {
                for (ChunkInferenceInput input : inputs) {
                    results.add(model.getResults(input.inputText()));
                }
            } catch (Exception e) {
                listener.onFailure(e);
                return null;
            }
            listener.onResponse(results);
            return null;
        }).when(inferenceService).chunkedInfer(any(), any(), any(), any(), any(), any(), any());

        doAnswer(invocation -> {
            UnparsedModel unparsedModel = invocation.getArgument(0);
            return modelMap.get(unparsedModel.inferenceEntityId());
        }).when(inferenceService).parsePersistedConfig(any(UnparsedModel.class));

        InferenceServiceRegistry inferenceServiceRegistry = mock(InferenceServiceRegistry.class);
        when(inferenceServiceRegistry.getService(any())).thenReturn(Optional.of(inferenceService));

        // Non-legacy format only for BYO tests
        ClusterService clusterService = createClusterService(false);

        return new ShardBulkInferenceActionFilter(
            clusterService,
            inferenceServiceRegistry,
            modelRegistry,
            licenseState,
            NOOP_INDEXING_PRESSURE,
            inferenceStats
        );
    }

    private static ClusterService createClusterService(boolean useLegacyFormat) {
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        var indexSettings = useLegacyFormat
            ? SemanticInferenceMetadataFieldsMapperTests.randomIndexSettings(true)
            : Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current()).build();
        when(indexMetadata.getSettings()).thenReturn(indexSettings);
        when(indexMetadata.getCreationVersion()).thenReturn(IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings));

        ProjectMetadata project = spy(ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID).build());
        when(project.index(anyString())).thenReturn(indexMetadata);

        Metadata metadata = mock(Metadata.class);
        when(metadata.getProject()).thenReturn(project);

        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        Settings settings = Settings.builder().put(INDICES_INFERENCE_BATCH_SIZE.getKey(), ByteSizeValue.ofKb(1)).build();
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings, Set.of(INDICES_INFERENCE_BATCH_SIZE)));
        return clusterService;
    }

    private static StaticModel createModel(String inferenceId, int dimensions) {
        return new StaticModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            randomAlphaOfLength(10),
            new TestModel.TestServiceSettings("model_name", dimensions, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT),
            new TestModel.TestTaskSettings(0),
            new TestModel.TestSecretSettings(randomAlphaOfLength(4))
        );
    }

    private static ChunkedInference randomChunkedInferenceEmbedding(StaticModel model, List<String> inputs) {
        return org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomChunkedInferenceEmbedding(model, inputs);
    }

    /**
     * Same as in ShardBulkInferenceActionFilterTests — reuses TestModel infrastructure
     * to provide deterministic inference results when needed.
     */
    private static class StaticModel extends TestModel {
        private final Map<String, ChunkedInference> chunkedResultMap;

        StaticModel(
            String inferenceEntityId,
            TaskType taskType,
            String service,
            TestServiceSettings serviceSettings,
            TestTaskSettings taskSettings,
            TestSecretSettings secretSettings
        ) {
            super(inferenceEntityId, taskType, service, serviceSettings, taskSettings, secretSettings);
            this.chunkedResultMap = new HashMap<>();
        }

        ChunkedInference getResults(String text) {
            ChunkedInference result = chunkedResultMap.get(text);
            if (result == null) {
                throw new IllegalArgumentException("No chunked text inference result cached for input [" + text + "]");
            }
            return result;
        }

        void putResult(String text, ChunkedInference result) {
            chunkedResultMap.put(text, result);
        }
    }

    private static class NoopIndexingPressure extends IndexingPressure {
        private NoopIndexingPressure() {
            super(Settings.EMPTY);
        }

        @Override
        public Coordinating createCoordinatingOperation(boolean forceExecution) {
            return new NoopCoordinating(forceExecution);
        }

        private class NoopCoordinating extends Coordinating {
            private NoopCoordinating(boolean forceExecution) {
                super(forceExecution);
            }

            @Override
            public void increment(int operations, long bytes) {}
        }
    }
}
