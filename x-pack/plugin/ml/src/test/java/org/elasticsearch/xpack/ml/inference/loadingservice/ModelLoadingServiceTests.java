/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ModelLoadingServiceTests extends ESTestCase {

    private TrainedModelProvider trainedModelProvider;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private InferenceAuditor auditor;
    private TrainedModelStatsService trainedModelStatsService;
    private CircuitBreaker circuitBreaker;

    @Before
    public void setUpComponents() {
        threadPool = new TestThreadPool(
            "ModelLoadingServiceTests",
            new ScalingExecutorBuilder(
                UTILITY_THREAD_POOL_NAME,
                1,
                4,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.ml.utility_thread_pool"
            )
        );
        trainedModelProvider = mock(TrainedModelProvider.class);
        clusterService = mock(ClusterService.class);
        auditor = mock(InferenceAuditor.class);
        trainedModelStatsService = mock(TrainedModelStatsService.class);
        doAnswer(a -> null).when(auditor).error(any(String.class), any(String.class));
        doAnswer(a -> null).when(auditor).info(any(String.class), any(String.class));
        doAnswer(a -> null).when(auditor).warning(any(String.class), any(String.class));
        doAnswer((invocationOnMock) -> null).when(clusterService).addListener(any(ClusterStateListener.class));
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("_name")).build());
        circuitBreaker = new CustomCircuitBreaker(1000);
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testGetCachedModels() throws Exception {
        String model1 = "test-load-model-1";
        String model2 = "test-load-model-2";
        String model3 = "test-load-model-3";
        withTrainedModel(model1, 1L);
        withTrainedModel(model2, 1L);
        withTrainedModel(model3, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        modelLoadingService.clusterChanged(ingestChangedEvent(model1, model2, model3));

        String[] modelIds = new String[] { model1, model2, model3 };
        for (int i = 0; i < 10; i++) {
            String model = modelIds[i % 3];
            PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(model1), eq(false), any());
        verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(model2), eq(false), any());
        verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(model3), eq(false), any());

        assertTrue(modelLoadingService.isModelCached(model1));
        assertTrue(modelLoadingService.isModelCached(model2));
        assertTrue(modelLoadingService.isModelCached(model3));

        // Test invalidate cache for model3
        modelLoadingService.clusterChanged(ingestChangedEvent(model1, model2));
        for (int i = 0; i < 10; i++) {
            String model = modelIds[i % 3];
            PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(model1), eq(false), any());
        verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(model2), eq(false), any());
        // It is not referenced, so called eagerly
        verify(trainedModelProvider, times(4)).getTrainedModelForInference(eq(model3), eq(false), any());
    }

    public void testMaxCachedLimitReached() throws Exception {
        String model1 = "test-cached-limit-load-model-1";
        String model2 = "test-cached-limit-load-model-2";
        String model3 = "test-cached-limit-load-model-3";
        String[] modelIds = new String[] { model1, model2, model3 };
        withTrainedModel(model1, 10L);
        withTrainedModel(model2, 6L);
        withTrainedModel(model3, 15L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.builder().put(ModelLoadingService.INFERENCE_MODEL_CACHE_SIZE.getKey(), ByteSizeValue.ofBytes(20L)).build(),
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        // We want to be notified when the models are loaded which happens in a background thread
        ModelLoadedTracker loadedTracker = new ModelLoadedTracker(Arrays.asList(modelIds));
        for (String modelId : modelIds) {
            modelLoadingService.addModelLoadedListener(modelId, loadedTracker.actionListener());
        }

        modelLoadingService.clusterChanged(ingestChangedEvent(model1, model2, model3));

        // Should have been loaded from the cluster change event but it is unknown in what order
        // the loading occurred or which models are currently in the cache due to evictions.
        // Verify that we have at least loaded all three
        assertBusy(() -> {
            verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(model1), eq(false), any());
            verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(model2), eq(false), any());
            verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(model3), eq(false), any());
        });

        // all models loaded put in the cache
        assertBusy(() -> assertTrue(loadedTracker.allModelsLoaded()), 2, TimeUnit.SECONDS);

        for (int i = 0; i < 10; i++) {
            // Only reference models 1 and 2, so that cache is only invalidated once for model3 (after initial load)
            String model = modelIds[i % 2];
            PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        // Depending on the order the models were first loaded in the first step
        // models 1 & 2 may have been evicted by model 3 in which case they have
        // been loaded at most twice
        verify(trainedModelProvider, atMost(2)).getTrainedModelForInference(eq(model1), eq(false), any());
        verify(trainedModelProvider, atMost(2)).getTrainedModelForInference(eq(model2), eq(false), any());
        // Only loaded requested once on the initial load from the change event
        verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(model3), eq(false), any());

        // model 3 has been loaded and evicted exactly once
        verify(trainedModelStatsService, times(1)).queueStats(argThat(o -> o.getModelId().equals(model3)), anyBoolean());

        // Load model 3, should invalidate 1 and 2
        for (int i = 0; i < 10; i++) {
            PlainActionFuture<LocalModel> future3 = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(model3, future3);
            assertThat(future3.get(), is(not(nullValue())));
        }
        verify(trainedModelProvider, times(2)).getTrainedModelForInference(eq(model3), eq(false), any());

        verify(trainedModelStatsService, atMost(2)).queueStats(argThat(o -> o.getModelId().equals(model1)), anyBoolean());
        verify(trainedModelStatsService, atMost(2)).queueStats(argThat(o -> o.getModelId().equals(model2)), anyBoolean());

        // Load model 1, should invalidate 3
        for (int i = 0; i < 10; i++) {
            PlainActionFuture<LocalModel> future1 = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(model1, future1);
            assertThat(future1.get(), is(not(nullValue())));
        }
        verify(trainedModelProvider, atMost(3)).getTrainedModelForInference(eq(model1), eq(false), any());
        verify(trainedModelStatsService, times(2)).queueStats(argThat(o -> o.getModelId().equals(model3)), anyBoolean());

        // Load model 2
        for (int i = 0; i < 10; i++) {
            PlainActionFuture<LocalModel> future2 = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(model2, future2);
            assertThat(future2.get(), is(not(nullValue())));
        }
        verify(trainedModelProvider, atMost(3)).getTrainedModelForInference(eq(model2), eq(false), any());

        // Test invalidate cache for model3
        // Now both model 1 and 2 should fit in cache without issues
        modelLoadingService.clusterChanged(ingestChangedEvent(model1, model2));
        for (int i = 0; i < 10; i++) {
            String model = modelIds[i % 3];
            PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, atMost(3)).getTrainedModelForInference(eq(model1), eq(false), any());
        verify(trainedModelProvider, atMost(3)).getTrainedModelForInference(eq(model2), eq(false), any());
        verify(trainedModelProvider, times(5)).getTrainedModelForInference(eq(model3), eq(false), any());
    }

    public void testWhenCacheEnabledButNotIngestNode() throws Exception {
        String model1 = "test-uncached-not-ingest-model-1";
        withTrainedModel(model1, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        modelLoadingService.clusterChanged(ingestChangedEvent(false, model1));

        for (int i = 0; i < 10; i++) {
            PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(model1, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        assertFalse(modelLoadingService.isModelCached(model1));
        verify(trainedModelProvider, times(10)).getTrainedModelForInference(eq(model1), eq(false), any());
        verify(trainedModelStatsService, never()).queueStats(any(InferenceStats.class), anyBoolean());
    }

    public void testGetCachedMissingModel() throws Exception {
        String model = "test-load-cached-missing-model";
        withMissingModel(model);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );
        modelLoadingService.clusterChanged(ingestChangedEvent(model));

        PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
        modelLoadingService.getModelForPipeline(model, future);

        try {
            future.get();
            fail("Should not have succeeded in loaded model");
        } catch (Exception ex) {
            assertThat(ex.getCause().getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, model)));
        }
        assertFalse(modelLoadingService.isModelCached(model));

        verify(trainedModelProvider, atMost(2)).getTrainedModelForInference(eq(model), eq(false), any());
        verify(trainedModelStatsService, never()).queueStats(any(InferenceStats.class), anyBoolean());
    }

    public void testGetMissingModel() {
        String model = "test-load-missing-model";
        withMissingModel(model);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
        modelLoadingService.getModelForPipeline(model, future);
        try {
            future.get();
            fail("Should not have succeeded");
        } catch (Exception ex) {
            assertThat(ex.getCause().getMessage(), containsString(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, model)));
        }
        assertFalse(modelLoadingService.isModelCached(model));
    }

    public void testGetModelEagerly() throws Exception {
        String model = "test-get-model-eagerly";
        withTrainedModel(model, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        for (int i = 0; i < 3; i++) {
            PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(3)).getTrainedModelForInference(eq(model), eq(false), any());
        assertFalse(modelLoadingService.isModelCached(model));
        verify(trainedModelStatsService, never()).queueStats(any(InferenceStats.class), anyBoolean());
    }

    public void testGetModelForSearch() throws Exception {
        String modelId = "test-get-model-for-search";
        withTrainedModel(modelId, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        for (int i = 0; i < 3; i++) {
            PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
            modelLoadingService.getModelForSearch(modelId, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        assertTrue(modelLoadingService.isModelCached(modelId));

        verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(modelId), eq(false), any());
        verify(trainedModelStatsService, never()).queueStats(any(InferenceStats.class), anyBoolean());
    }

    public void testCircuitBreakerBreak() throws Exception {
        String model1 = "test-circuit-break-model-1";
        String model2 = "test-circuit-break-model-2";
        String model3 = "test-circuit-break-model-3";
        withTrainedModel(model1, 5L);
        withTrainedModel(model2, 5L);
        withTrainedModel(model3, 12L);
        CircuitBreaker circuitBreaker = new CustomCircuitBreaker(11);
        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        modelLoadingService.addModelLoadedListener(
            model3,
            ActionListener.wrap(
                r -> fail("Should not have succeeded to load model as breaker should be reached"),
                e -> assertThat(e, instanceOf(CircuitBreakingException.class))
            )
        );

        modelLoadingService.clusterChanged(ingestChangedEvent(model1, model2, model3));

        // Should have been loaded from the cluster change event but it is unknown in what order
        // the loading occurred or which models are currently in the cache due to evictions.
        // Verify that we have at least loaded all three
        assertBusy(() -> {
            verify(trainedModelProvider, times(1)).getTrainedModel(eq(model1), eq(GetTrainedModelsAction.Includes.empty()), any());
            verify(trainedModelProvider, times(1)).getTrainedModel(eq(model2), eq(GetTrainedModelsAction.Includes.empty()), any());
            verify(trainedModelProvider, times(1)).getTrainedModel(eq(model3), eq(GetTrainedModelsAction.Includes.empty()), any());
        });
        assertBusy(() -> {
            assertThat(circuitBreaker.getUsed(), equalTo(10L));
            assertThat(circuitBreaker.getTrippedCount(), equalTo(1L));
        });

        modelLoadingService.clusterChanged(ingestChangedEvent(model1));

        assertBusy(() -> { assertThat(circuitBreaker.getUsed(), equalTo(5L)); });
    }

    public void testReferenceCounting() throws Exception {
        String modelId = "test-reference-counting";
        withTrainedModel(modelId, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        modelLoadingService.clusterChanged(ingestChangedEvent(modelId));

        PlainActionFuture<LocalModel> forPipeline = new PlainActionFuture<>();
        modelLoadingService.getModelForPipeline(modelId, forPipeline);
        final LocalModel model = forPipeline.get();
        assertBusy(() -> assertEquals(2, model.getReferenceCount()));

        PlainActionFuture<LocalModel> forSearch = new PlainActionFuture<>();
        modelLoadingService.getModelForPipeline(modelId, forSearch);
        forSearch.get();
        assertBusy(() -> assertEquals(3, model.getReferenceCount()));

        model.release();
        assertBusy(() -> assertEquals(2, model.getReferenceCount()));

        PlainActionFuture<LocalModel> forSearch2 = new PlainActionFuture<>();
        modelLoadingService.getModelForPipeline(modelId, forSearch2);
        forSearch2.get();
        assertBusy(() -> assertEquals(3, model.getReferenceCount()));
    }

    public void testReferenceCountingForPipeline() throws Exception {
        String modelId = "test-reference-counting-for-pipeline";
        withTrainedModel(modelId, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        modelLoadingService.clusterChanged(ingestChangedEvent(modelId));

        PlainActionFuture<LocalModel> forPipeline = new PlainActionFuture<>();
        modelLoadingService.getModelForPipeline(modelId, forPipeline);
        final LocalModel model = forPipeline.get();
        assertBusy(() -> assertEquals(2, model.getReferenceCount()));

        PlainActionFuture<LocalModel> forPipeline2 = new PlainActionFuture<>();
        modelLoadingService.getModelForPipeline(modelId, forPipeline2);
        forPipeline2.get();
        assertBusy(() -> assertEquals(3, model.getReferenceCount()));

        // will cause the model to be evicted
        modelLoadingService.clusterChanged(ingestChangedEvent());
        assertBusy(() -> assertEquals(2, model.getReferenceCount()));
    }

    public void testReferenceCounting_ModelIsNotCached() throws ExecutionException, InterruptedException {
        String modelId = "test-reference-counting-not-cached";
        withTrainedModel(modelId, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
        modelLoadingService.getModelForPipeline(modelId, future);
        LocalModel model = future.get();
        assertEquals(1, model.getReferenceCount());
    }

    public void testGetCachedModelViaModelAliases() throws Exception {
        String model1 = "test-load-model-1";
        String model2 = "test-load-model-2";
        withTrainedModel(model1, 1L);
        withTrainedModel(model2, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        modelLoadingService.clusterChanged(
            aliasChangeEvent(true, new String[] { "loaded_model" }, true, Arrays.asList(Tuple.tuple(model1, "loaded_model")))
        );

        String[] modelIds = new String[] { model1, "loaded_model" };
        for (int i = 0; i < 10; i++) {
            String model = modelIds[i % 2];
            PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(model1), eq(false), any());

        assertTrue(modelLoadingService.isModelCached(model1));
        assertTrue(modelLoadingService.isModelCached("loaded_model"));

        // alias change only
        modelLoadingService.clusterChanged(
            aliasChangeEvent(true, new String[] { "loaded_model" }, false, Arrays.asList(Tuple.tuple(model2, "loaded_model")))
        );

        modelIds = new String[] { model2, "loaded_model" };
        for (int i = 0; i < 10; i++) {
            String model = modelIds[i % 2];
            PlainActionFuture<LocalModel> future = new PlainActionFuture<>();
            modelLoadingService.getModelForPipeline(model, future);
            assertThat(future.get(), is(not(nullValue())));
        }

        verify(trainedModelProvider, times(1)).getTrainedModelForInference(eq(model2), eq(false), any());
        assertTrue(modelLoadingService.isModelCached(model2));
        assertTrue(modelLoadingService.isModelCached("loaded_model"));
    }

    public void testAliasesGetUpdatedEvenWhenNotIngestNode() throws IOException {
        String model1 = "test-load-model-1";
        withTrainedModel(model1, 1L);
        String model2 = "test-load-model-2";
        withTrainedModel(model2, 1L);

        ModelLoadingService modelLoadingService = new ModelLoadingService(
            trainedModelProvider,
            auditor,
            threadPool,
            clusterService,
            trainedModelStatsService,
            Settings.EMPTY,
            "test-node",
            circuitBreaker,
            mock(XPackLicenseState.class)
        );

        modelLoadingService.clusterChanged(
            aliasChangeEvent(false, new String[0], false, Arrays.asList(Tuple.tuple(model1, "loaded_model")))
        );

        assertThat(modelLoadingService.getModelId("loaded_model"), equalTo(model1));

        modelLoadingService.clusterChanged(
            aliasChangeEvent(
                false,
                new String[0],
                false,
                Arrays.asList(
                    Tuple.tuple(model1, "loaded_model_again"),
                    Tuple.tuple(model1, "loaded_model_foo"),
                    Tuple.tuple(model2, "loaded_model")
                )
            )
        );
        assertThat(modelLoadingService.getModelId("loaded_model"), equalTo(model2));
        assertThat(modelLoadingService.getModelId("loaded_model_foo"), equalTo(model1));
        assertThat(modelLoadingService.getModelId("loaded_model_again"), equalTo(model1));
    }

    @SuppressWarnings("unchecked")
    private void withTrainedModel(String modelId, long size) {
        InferenceDefinition definition = mock(InferenceDefinition.class);
        when(definition.ramBytesUsed()).thenReturn(size);
        TrainedModelConfig trainedModelConfig = mock(TrainedModelConfig.class);
        when(trainedModelConfig.getModelId()).thenReturn(modelId);
        when(trainedModelConfig.getInferenceConfig()).thenReturn(ClassificationConfig.EMPTY_PARAMS);
        when(trainedModelConfig.getInput()).thenReturn(new TrainedModelInput(Arrays.asList("foo", "bar", "baz")));
        when(trainedModelConfig.getModelSize()).thenReturn(size);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onResponse(definition);
            return null;
        }).when(trainedModelProvider).getTrainedModelForInference(eq(modelId), eq(false), any());
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onResponse(trainedModelConfig);
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq(modelId), eq(GetTrainedModelsAction.Includes.empty()), any());
    }

    @SuppressWarnings("unchecked")
    private void withMissingModel(String modelId) {
        if (randomBoolean()) {
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("rawtypes")
                ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
                listener.onFailure(new ResourceNotFoundException(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId)));
                return null;
            }).when(trainedModelProvider).getTrainedModel(eq(modelId), eq(GetTrainedModelsAction.Includes.empty()), any());
        } else {
            TrainedModelConfig trainedModelConfig = mock(TrainedModelConfig.class);
            when(trainedModelConfig.getModelSize()).thenReturn(0L);
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("rawtypes")
                ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
                listener.onResponse(trainedModelConfig);
                return null;
            }).when(trainedModelProvider).getTrainedModel(eq(modelId), eq(GetTrainedModelsAction.Includes.empty()), any());
            doAnswer(invocationOnMock -> {
                @SuppressWarnings("rawtypes")
                ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
                listener.onFailure(new ResourceNotFoundException(Messages.getMessage(Messages.MODEL_DEFINITION_NOT_FOUND, modelId)));
                return null;
            }).when(trainedModelProvider).getTrainedModelForInference(eq(modelId), eq(false), any());
        }
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("rawtypes")
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onFailure(new ResourceNotFoundException(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, modelId)));
            return null;
        }).when(trainedModelProvider).getTrainedModelForInference(eq(modelId), eq(false), any());
    }

    private static ClusterChangedEvent ingestChangedEvent(String... modelId) throws IOException {
        return ingestChangedEvent(true, modelId);
    }

    private static ClusterChangedEvent aliasChangeEvent(
        boolean isIngestNode,
        String[] modelId,
        boolean ingestToo,
        List<Tuple<String, String>> modelIdAndAliases
    ) throws IOException {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        Set<String> set = new HashSet<>();
        set.add(ModelAliasMetadata.NAME);
        if (ingestToo) {
            set.add(IngestMetadata.TYPE);
        }
        when(event.changedCustomMetadataSet()).thenReturn(set);
        when(event.state()).thenReturn(withModelReferencesAndAliasChange(isIngestNode, modelId, modelIdAndAliases));
        return event;
    }

    private static ClusterChangedEvent ingestChangedEvent(boolean isIngestNode, String... modelId) throws IOException {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.changedCustomMetadataSet()).thenReturn(Collections.singleton(IngestMetadata.TYPE));
        when(event.state()).thenReturn(buildClusterStateWithModelReferences(isIngestNode, modelId));
        return event;
    }

    private static ClusterState buildClusterStateWithModelReferences(boolean isIngestNode, String... modelId) throws IOException {
        return builder(isIngestNode).metadata(addIngest(Metadata.builder(), modelId)).build();
    }

    private static ClusterState withModelReferencesAndAliasChange(
        boolean isIngestNode,
        String[] modelId,
        List<Tuple<String, String>> modelIdAndAliases
    ) throws IOException {
        return builder(isIngestNode).metadata(addAliases(addIngest(Metadata.builder(), modelId), modelIdAndAliases)).build();
    }

    private static ClusterState.Builder builder(boolean isIngestNode) {
        return ClusterState.builder(new ClusterName("_name"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(
                        new DiscoveryNode(
                            "node_name",
                            "node_id",
                            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                            Collections.emptyMap(),
                            isIngestNode ? Collections.singleton(DiscoveryNodeRole.INGEST_ROLE) : Collections.emptySet(),
                            Version.CURRENT
                        )
                    )
                    .localNodeId("node_id")
                    .build()
            );
    }

    private static Metadata.Builder addIngest(Metadata.Builder builder, String... modelId) throws IOException {
        Map<String, PipelineConfiguration> configurations = Maps.newMapWithExpectedSize(modelId.length);
        for (String id : modelId) {
            configurations.put("pipeline_with_model_" + id, newConfigurationWithInferenceProcessor(id));
        }
        IngestMetadata ingestMetadata = new IngestMetadata(configurations);
        return builder.putCustom(IngestMetadata.TYPE, ingestMetadata);
    }

    private static Metadata.Builder addAliases(Metadata.Builder builder, List<Tuple<String, String>> modelIdAndAliases) {
        ModelAliasMetadata modelAliasMetadata = new ModelAliasMetadata(
            modelIdAndAliases.stream().collect(Collectors.toMap(Tuple::v2, t -> new ModelAliasMetadata.ModelAliasEntry(t.v1())))
        );
        return builder.putCustom(ModelAliasMetadata.NAME, modelAliasMetadata);
    }

    private static PipelineConfiguration newConfigurationWithInferenceProcessor(String modelId) throws IOException {
        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .map(
                    Collections.singletonMap(
                        "processors",
                        Collections.singletonList(
                            Collections.singletonMap(
                                InferenceProcessor.TYPE,
                                Collections.singletonMap(InferenceResults.MODEL_ID_RESULTS_FIELD, modelId)
                            )
                        )
                    )
                )
        ) {
            return new PipelineConfiguration("pipeline_with_model_" + modelId, BytesReference.bytes(xContentBuilder), XContentType.JSON);
        }
    }

    private static class CustomCircuitBreaker implements CircuitBreaker {

        private final long maxBytes;
        private long currentBytes = 0;
        private long trippedCount = 0;

        CustomCircuitBreaker(long maxBytes) {
            this.maxBytes = maxBytes;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            throw new CircuitBreakingException(fieldName, Durability.TRANSIENT);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            synchronized (this) {
                if (bytes + currentBytes >= maxBytes) {
                    trippedCount++;
                    circuitBreak(label, bytes);
                }
                currentBytes += bytes;
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            synchronized (this) {
                currentBytes += bytes;
            }
        }

        @Override
        public long getUsed() {
            return currentBytes;
        }

        @Override
        public long getLimit() {
            return maxBytes;
        }

        @Override
        public double getOverhead() {
            return 1.0;
        }

        @Override
        public long getTrippedCount() {
            synchronized (this) {
                return trippedCount;
            }
        }

        @Override
        public String getName() {
            return MachineLearning.TRAINED_MODEL_CIRCUIT_BREAKER_NAME;
        }

        @Override
        public Durability getDurability() {
            return Durability.TRANSIENT;
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            throw new UnsupportedOperationException("boom");
        }
    }

    private static class ModelLoadedTracker {
        private final Set<String> expectedModelIds;

        ModelLoadedTracker(Collection<String> expectedModelIds) {
            this.expectedModelIds = new HashSet<>(expectedModelIds);
        }

        private synchronized boolean allModelsLoaded() {
            return expectedModelIds.isEmpty();
        }

        private synchronized void onModelLoaded(LocalModel model) {
            expectedModelIds.remove(model.getModelId());
        }

        private void onFailure(Exception e) {
            fail(e.getMessage());
        }

        ActionListener<LocalModel> actionListener() {
            return ActionListener.wrap(this::onModelLoaded, this::onFailure);
        }
    }
}
