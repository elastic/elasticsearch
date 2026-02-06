/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.filter;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.ChunkedInferenceEmbedding;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.elasticsearch.xpack.inference.InferencePlugin.INDICES_INFERENCE_BULK_TIMEOUT;
import static org.elasticsearch.xpack.inference.action.filter.ShardBulkInferenceActionFilter.INDICES_INFERENCE_BATCH_SIZE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Abstract base class for {@link ShardBulkInferenceActionFilter} tests, providing shared
 * infrastructure: parameterized test setup, thread pool lifecycle, {@link StaticModel},
 * {@link NoopIndexingPressure}, cluster service creation, and common test helpers.
 */
public abstract class AbstractShardBulkInferenceActionFilterTestCase extends ESTestCase {

    protected static final IndexingPressure NOOP_INDEXING_PRESSURE = new NoopIndexingPressure();

    protected final boolean useLegacyFormat;
    protected ThreadPool threadPool;

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    protected AbstractShardBulkInferenceActionFilterTestCase(boolean useLegacyFormat) {
        this.useLegacyFormat = useLegacyFormat;
    }

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() throws Exception {
        terminate(threadPool);
    }

    // ========== Cluster Service ==========

    protected static ClusterService createClusterService(boolean useLegacyFormat) {
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
        long batchSizeInBytes = randomLongBetween(1, ByteSizeValue.ofKb(1).getBytes());
        Settings settings = Settings.builder().put(INDICES_INFERENCE_BATCH_SIZE.getKey(), ByteSizeValue.ofBytes(batchSizeInBytes)).build();
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.of(INDICES_INFERENCE_BATCH_SIZE, INDICES_INFERENCE_BULK_TIMEOUT))
        );
        return clusterService;
    }

    protected static DocWriteRequest<?> randomDocWriteRequest(String field, String value) {
        if (randomBoolean()) {
            return new IndexRequest("index").source(field, value);
        }
        return new UpdateRequest().doc(new IndexRequest("index").source(field, value));
    }

    protected static DocWriteRequest<?> randomDocWriteRequest(Map<String, Object> source) {
        if (randomBoolean()) {
            return new IndexRequest("index").source(source);
        }
        return new UpdateRequest().doc(new IndexRequest("index").source(source));
    }

    protected BulkItemRequest bulkItemRequest(int id, String field, String value) {
        return new BulkItemRequest(id, randomDocWriteRequest(field, value));
    }

    protected InferenceFieldMetadata inferenceFieldMetadata(String field, StaticModel model) {
        return new InferenceFieldMetadata(field, model.getInferenceEntityId(), new String[] { field }, null);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void runFilterAndVerify(
        ShardBulkInferenceActionFilter filter,
        Map<String, InferenceFieldMetadata> fieldMap,
        BulkItemRequest[] items,
        Consumer<BulkItemRequest[]> verifier
    ) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        ActionFilterChain chain = (task, action, request, listener) -> {
            try {
                BulkShardRequest bulkRequest = (BulkShardRequest) request;
                verifier.accept(bulkRequest.items());
            } finally {
                latch.countDown();
            }
        };

        BulkShardRequest request = new BulkShardRequest(new ShardId("test", "test", 0), WriteRequest.RefreshPolicy.NONE, items);
        request.setInferenceFieldMap(fieldMap);

        filter.apply(mock(Task.class), TransportShardBulkAction.ACTION_NAME, request, mock(ActionListener.class), chain);
        awaitLatch(latch, 30, TimeUnit.SECONDS);
    }

    protected static class StaticModel extends TestModel {
        private final Map<String, ChunkedInference> resultMap;

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
            return createRandomInstance(randomFrom(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING));
        }

        public static StaticModel createRandomInstance(TaskType taskType) {
            TestModel testModel = TestModel.createRandomInstance(taskType);
            return new StaticModel(
                testModel.getInferenceEntityId(),
                testModel.getTaskType(),
                randomAlphaOfLength(10),
                testModel.getServiceSettings(),
                testModel.getTaskSettings(),
                testModel.getSecretSettings()
            );
        }

        ChunkedInference getResults(String text) {
            return resultMap.getOrDefault(text, new ChunkedInferenceEmbedding(List.of()));
        }

        void putResult(String text, ChunkedInference result) {
            resultMap.put(text, result);
        }

        boolean hasResult(String text) {
            return resultMap.containsKey(text);
        }
    }

    protected static class NoopIndexingPressure extends IndexingPressure {
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
