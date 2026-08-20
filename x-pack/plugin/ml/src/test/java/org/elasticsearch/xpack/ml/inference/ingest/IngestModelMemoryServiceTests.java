/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ingest;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.IngestModelMemoryProvider;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IngestModelMemoryServiceTests extends ESTestCase {

    private static final ProjectId PROJECT_A = ProjectId.fromId("project-a");
    private static final ProjectId PROJECT_B = ProjectId.fromId("project-b");

    private TrainedModelProvider trainedModelProvider;
    private ThreadPool threadPool;
    private IngestModelMemoryService service;

    @Before
    public void setUpComponents() {
        trainedModelProvider = mock(TrainedModelProvider.class);
        threadPool = new TestThreadPool(
            "IngestModelMemoryServiceTests",
            new ScalingExecutorBuilder(MachineLearning.UTILITY_THREAD_POOL_NAME, 0, 1, TimeValue.timeValueMinutes(10), false)
        );
        service = new IngestModelMemoryService(trainedModelProvider, threadPool);
        service.setUnresolvedModelSizeRetryIntervalForTests(TimeValue.timeValueHours(1));
    }

    @After
    public void tearDownComponents() throws Exception {
        service.clusterChanged(
            new ClusterChangedEvent(
                "test",
                nonMasterClusterState(ClusterState.EMPTY_STATE.metadata()),
                masterClusterState(ClusterState.EMPTY_STATE.metadata())
            )
        );
        terminate(threadPool);
    }

    public void testMasterFetchPopulatesHeapRequirement() throws Exception {
        stubModelConfig("model-a", 100L);
        ClusterState previous = masterClusterState(ClusterState.EMPTY_STATE.metadata());
        ClusterState current = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", current, previous));

        assertBusy(() -> {
            IngestModelMemoryProvider.HeapRequirement requirement = service.getRequiredHeapBytes();
            assertThat(requirement.heapBytes(), equalTo(100L));
            assertThat(requirement.isExact(), is(true));
        });
        verify(trainedModelProvider, times(1)).getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());
    }

    public void testPipelineDeletionRemovesModelContribution() throws Exception {
        stubModelConfig("model-a", 100L);
        ClusterState withModel = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", withModel, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertBusy(() -> assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(100L)));

        ClusterState withoutModel = masterClusterState(withIngestModels(Map.of()));
        service.clusterChanged(new ClusterChangedEvent("test", withoutModel, withModel));

        assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(0L));
        assertThat(service.getRequiredHeapBytes().isExact(), is(true));
    }

    public void testLateModelFetchAfterPipelineRemovalDoesNotRestoreHeap() throws Exception {
        TrainedModelConfig trainedModelConfig = mock(TrainedModelConfig.class);
        when(trainedModelConfig.getModelSize()).thenReturn(100L);
        java.util.concurrent.atomic.AtomicReference<ActionListener<TrainedModelConfig>> pendingListener =
            new java.util.concurrent.atomic.AtomicReference<>();
        doAnswer(invocation -> {
            pendingListener.set(invocation.getArgument(3));
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());

        ClusterState withModel = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", withModel, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertBusy(() -> assertThat(pendingListener.get(), notNullValue()));

        ClusterState withoutModel = masterClusterState(withIngestModels(Map.of()));
        service.clusterChanged(new ClusterChangedEvent("test", withoutModel, withModel));

        assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(0L));
        assertThat(pendingListener.get(), notNullValue());
        pendingListener.get().onResponse(trainedModelConfig);
        assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(0L));
        assertThat(service.getGlobalModelSizeForTests("model-a"), nullValue());
    }

    public void testProjectRemovalDropsTrackedModelsWithoutGlobalPipelineWalk() throws Exception {
        stubModelConfig("model-a", 100L);
        stubModelConfig("model-b", 200L);
        ClusterState twoProjects = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a", PROJECT_B, "model-b")));
        service.clusterChanged(new ClusterChangedEvent("test", twoProjects, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertBusy(() -> assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(300L)));

        ClusterState projectRemoved = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", projectRemoved, twoProjects));

        assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(100L));
        assertThat(service.isTrackingModelForProjectForTests(PROJECT_B, "model-b"), is(false));
        verify(trainedModelProvider, times(1)).getTrainedModel(eq("model-b"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());
    }

    public void testUnchangedProjectsDoNotTriggerAdditionalFetches() throws Exception {
        stubModelConfig("model-a", 100L);
        stubModelConfig("model-b", 200L);
        ClusterState initial = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a", PROJECT_B, "model-b")));
        service.clusterChanged(new ClusterChangedEvent("test", initial, masterClusterState(ClusterState.EMPTY_STATE.metadata())));
        assertBusy(() -> assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(300L)));

        ClusterState projectAOnlyChange = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a,model-a2", PROJECT_B, "model-b")));
        stubModelConfig("model-a2", 50L);
        service.clusterChanged(new ClusterChangedEvent("test", projectAOnlyChange, initial));

        assertBusy(() -> assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(350L)));
        verify(trainedModelProvider, times(1)).getTrainedModel(eq("model-a2"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());
        verify(trainedModelProvider, times(1)).getTrainedModel(eq("model-b"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());
    }

    public void testZeroModelSizeStoredAsExact() throws Exception {
        stubModelConfig("model-a", 0L);
        ClusterState current = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", current, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertBusy(() -> {
            IngestModelMemoryProvider.HeapRequirement requirement = service.getRequiredHeapBytes();
            assertThat(requirement.heapBytes(), equalTo(0L));
            assertThat(requirement.isExact(), is(true));
        });

        reconcileWhenIdle();
        verify(trainedModelProvider, times(2)).getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());
    }

    public void testNonMasterClearsTrackedState() throws Exception {
        stubModelConfig("model-a", 100L);
        ClusterState masterState = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", masterState, masterClusterState(ClusterState.EMPTY_STATE.metadata())));
        assertBusy(() -> assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(100L)));

        ClusterState nonMasterState = nonMasterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", nonMasterState, masterState));

        assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(0L));
        assertThat(service.getRequiredHeapBytes().isExact(), is(true));
    }

    public void testWarmupReturnsInexactUntilFetchCompletes() throws Exception {
        doAnswer(invocation -> null).when(trainedModelProvider)
            .getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());

        ClusterState current = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", current, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        IngestModelMemoryProvider.HeapRequirement warmup = service.getRequiredHeapBytes();
        assertThat(warmup.heapBytes(), equalTo(0L));
        assertThat(warmup.isExact(), is(false));
    }

    public void testAliasResolutionUsesCanonicalModelId() throws Exception {
        stubModelConfig("canonical-model", 150L);
        ClusterState current = masterClusterState(
            withIngestModelsAndAliases(Map.of(PROJECT_A, "my-alias"), List.of(new Tuple<>("canonical-model", "my-alias")))
        );
        service.clusterChanged(new ClusterChangedEvent("test", current, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertBusy(() -> assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(150L)));
        verify(trainedModelProvider).getTrainedModel(eq("canonical-model"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());
    }

    public void testDuplicateModelAcrossProjectsCountedOnce() throws Exception {
        stubModelConfig("shared-model", 100L);
        ClusterState current = masterClusterState(withIngestModels(Map.of(PROJECT_A, "shared-model", PROJECT_B, "shared-model")));
        service.clusterChanged(new ClusterChangedEvent("test", current, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertBusy(() -> assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(100L)));
        verify(trainedModelProvider, times(1)).getTrainedModel(
            eq("shared-model"),
            eq(GetTrainedModelsAction.Includes.empty()),
            any(),
            any()
        );
    }

    public void testConcurrentModelSizeUpdatesShouldReflectBothSizes() throws Exception {
        stubModelConfig("model-a", 100L);
        stubModelConfig("model-b", 200L);
        ClusterState current = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a,model-b")));
        service.clusterChanged(new ClusterChangedEvent("test", current, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertBusy(() -> assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(300L)));

        CyclicBarrier barrier = new CyclicBarrier(2);
        Thread threadA = new Thread(() -> {
            safeAwait(barrier);
            service.propagateModelSizeForTests("model-a", OptionalLong.of(150L));
        });
        Thread threadB = new Thread(() -> {
            safeAwait(barrier);
            service.propagateModelSizeForTests("model-b", OptionalLong.of(250L));
        });
        threadA.start();
        threadB.start();
        threadA.join();
        threadB.join();

        IngestModelMemoryProvider.HeapRequirement requirement = service.getRequiredHeapBytes();
        assertThat(requirement.heapBytes(), equalTo(400L));
        assertThat(requirement.isExact(), is(true));
    }

    public void testFailedModelFetchShouldRetryAndEventuallyResolve() throws Exception {
        TrainedModelConfig trainedModelConfig = mock(TrainedModelConfig.class);
        when(trainedModelConfig.getModelSize()).thenReturn(100L);
        AtomicInteger fetchAttempts = new AtomicInteger();
        doAnswer(invocation -> {
            if (fetchAttempts.incrementAndGet() == 1) {
                ActionListener<TrainedModelConfig> listener = invocation.getArgument(3);
                listener.onFailure(new RuntimeException("transient failure"));
            } else {
                ActionListener<TrainedModelConfig> listener = invocation.getArgument(3);
                listener.onResponse(trainedModelConfig);
            }
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());

        ClusterState current = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", current, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertBusy(() -> {
            assertThat(service.getRequiredHeapBytes().isExact(), is(false));
            assertThat(service.isFetchScheduledForTests("model-a"), is(false));
            verify(trainedModelProvider, times(1)).getTrainedModel(
                eq("model-a"),
                eq(GetTrainedModelsAction.Includes.empty()),
                any(),
                any()
            );
        });

        reconcileWhenIdle();

        assertBusy(() -> {
            IngestModelMemoryProvider.HeapRequirement requirement = service.getRequiredHeapBytes();
            assertThat(requirement.heapBytes(), equalTo(100L));
            assertThat(requirement.isExact(), is(true));
        });
        verify(trainedModelProvider, times(2)).getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());
    }

    public void testModelDeletedWhilePipelineRemainsDropsStaleHeap() throws Exception {
        TrainedModelConfig trainedModelConfig = mock(TrainedModelConfig.class);
        when(trainedModelConfig.getModelSize()).thenReturn(100L);
        AtomicInteger fetchAttempts = new AtomicInteger();
        doAnswer(invocation -> {
            ActionListener<TrainedModelConfig> listener = invocation.getArgument(3);
            if (fetchAttempts.incrementAndGet() == 1) {
                listener.onResponse(trainedModelConfig);
            } else {
                listener.onFailure(new RuntimeException("model not found"));
            }
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());

        ClusterState current = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", current, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertBusy(() -> {
            IngestModelMemoryProvider.HeapRequirement requirement = service.getRequiredHeapBytes();
            assertThat(requirement.heapBytes(), equalTo(100L));
            assertThat(requirement.isExact(), is(true));
        });

        reconcileWhenIdle();

        assertBusy(() -> {
            IngestModelMemoryProvider.HeapRequirement requirement = service.getRequiredHeapBytes();
            assertThat(requirement.heapBytes(), equalTo(0L));
            assertThat(requirement.isExact(), is(false));
        });
    }

    public void testUnresolvableModelReferenceBecomesExactZeroAfterThreshold() throws Exception {
        TrainedModelConfig trainedModelConfig = mock(TrainedModelConfig.class);
        when(trainedModelConfig.getModelSize()).thenReturn(100L);
        AtomicBoolean allowResolution = new AtomicBoolean(false);
        doAnswer(invocation -> {
            ActionListener<TrainedModelConfig> listener = invocation.getArgument(3);
            if (allowResolution.get()) {
                listener.onResponse(trainedModelConfig);
            } else {
                listener.onFailure(new RuntimeException("model not found"));
            }
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());

        ClusterState current = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", current, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertBusy(() -> {
            assertThat(service.getRequiredHeapBytes().isExact(), is(false));
            assertThat(service.isFetchScheduledForTests("model-a"), is(false));
            verify(trainedModelProvider, times(1)).getTrainedModel(
                eq("model-a"),
                eq(GetTrainedModelsAction.Includes.empty()),
                any(),
                any()
            );
        });

        long staleSince = threadPool.relativeTimeInNanos() - IngestModelMemoryService.STALE_MODEL_SIZE_WARN_THRESHOLD.nanos() - 1;
        service.setUnresolvedSinceNanosForTests("model-a", staleSince);

        reconcileWhenIdle();

        IngestModelMemoryProvider.HeapRequirement staleRequirement = service.getRequiredHeapBytes();
        assertThat(staleRequirement.heapBytes(), equalTo(0L));
        assertThat(staleRequirement.isExact(), is(true));

        assertBusy(
            () -> verify(trainedModelProvider, times(2)).getTrainedModel(
                eq("model-a"),
                eq(GetTrainedModelsAction.Includes.empty()),
                any(),
                any()
            )
        );

        allowResolution.set(true);
        reconcileWhenIdle();

        assertBusy(() -> {
            IngestModelMemoryProvider.HeapRequirement requirement = service.getRequiredHeapBytes();
            assertThat(requirement.heapBytes(), equalTo(100L));
            assertThat(requirement.isExact(), is(true));
        });
        verify(trainedModelProvider, times(3)).getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());
    }

    public void testUnresolvedModelSizeShouldWarnAfterStalenessThreshold() throws Exception {
        doAnswer(invocation -> null).when(trainedModelProvider)
            .getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());

        ClusterState current = masterClusterState(withIngestModels(Map.of(PROJECT_A, "model-a")));
        service.clusterChanged(new ClusterChangedEvent("test", current, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        long staleSince = threadPool.relativeTimeInNanos() - IngestModelMemoryService.STALE_MODEL_SIZE_WARN_THRESHOLD.nanos() - 1;
        service.setUnresolvedSinceNanosForTests("model-a", staleSince);

        try (var mockLog = MockLog.capture(IngestModelMemoryService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "stale model size",
                    IngestModelMemoryService.class.getCanonicalName(),
                    Level.WARN,
                    "Ingest model [model-a] heap size has been unresolved for over *; heap contribution is now treated as exact zero*"
                )
            );
            service.reconcileModelSizesForTests();
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testBecomingMasterWhileStateNotRecoveredShouldInitializeAfterBlockLifts() throws Exception {
        stubModelConfig("model-a", 100L);
        Metadata metadata = withIngestModels(Map.of(PROJECT_A, "model-a"));
        ClusterState blockedMaster = blockedMasterClusterState(metadata);
        service.clusterChanged(new ClusterChangedEvent("test", blockedMaster, nonMasterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(0L));
        assertThat(service.getRequiredHeapBytes().isExact(), is(true));
        assertThat(service.isPeriodicRetryRunningForTests(), is(false));
        verify(trainedModelProvider, times(0)).getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());

        ClusterState recoveredMaster = masterClusterState(metadata);
        service.clusterChanged(new ClusterChangedEvent("test", recoveredMaster, blockedMaster));

        assertBusy(() -> {
            IngestModelMemoryProvider.HeapRequirement requirement = service.getRequiredHeapBytes();
            assertThat(requirement.heapBytes(), equalTo(100L));
            assertThat(requirement.isExact(), is(true));
        });
        assertThat(service.isPeriodicRetryRunningForTests(), is(true));
        verify(trainedModelProvider, times(1)).getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());
    }

    public void testMasterDemotionShouldStopPeriodicRetryAndReElectionShouldRepopulate() throws Exception {
        stubModelConfig("model-a", 100L);
        Metadata metadata = withIngestModels(Map.of(PROJECT_A, "model-a"));
        ClusterState masterState = masterClusterState(metadata);
        service.clusterChanged(new ClusterChangedEvent("test", masterState, masterClusterState(ClusterState.EMPTY_STATE.metadata())));

        assertBusy(() -> {
            assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(100L));
            assertThat(service.isPeriodicRetryRunningForTests(), is(true));
        });

        ClusterState nonMasterState = nonMasterClusterState(metadata);
        service.clusterChanged(new ClusterChangedEvent("test", nonMasterState, masterState));

        assertThat(service.getRequiredHeapBytes().heapBytes(), equalTo(0L));
        assertThat(service.getRequiredHeapBytes().isExact(), is(true));
        assertThat(service.isPeriodicRetryRunningForTests(), is(false));

        ClusterState reElectedMaster = masterClusterState(metadata);
        service.clusterChanged(new ClusterChangedEvent("test", reElectedMaster, nonMasterState));

        assertBusy(() -> {
            IngestModelMemoryProvider.HeapRequirement requirement = service.getRequiredHeapBytes();
            assertThat(requirement.heapBytes(), equalTo(100L));
            assertThat(requirement.isExact(), is(true));
            assertThat(service.isPeriodicRetryRunningForTests(), is(true));
        });
        verify(trainedModelProvider, times(2)).getTrainedModel(eq("model-a"), eq(GetTrainedModelsAction.Includes.empty()), any(), any());
    }

    private void reconcileWhenIdle() throws Exception {
        assertBusy(() -> assertThat(service.hasScheduledFetchesForTests(), is(false)));
        service.reconcileModelSizesForTests();
        assertBusy(() -> assertThat(service.hasScheduledFetchesForTests(), is(false)));
    }

    private void stubModelConfig(String modelId, long modelSize) {
        TrainedModelConfig trainedModelConfig = mock(TrainedModelConfig.class);
        when(trainedModelConfig.getModelSize()).thenReturn(modelSize);
        doAnswer(invocation -> {
            ActionListener<TrainedModelConfig> listener = invocation.getArgument(3);
            listener.onResponse(trainedModelConfig);
            return null;
        }).when(trainedModelProvider).getTrainedModel(eq(modelId), eq(GetTrainedModelsAction.Includes.empty()), any(), any());
    }

    private static Metadata withIngestModels(Map<ProjectId, String> projectToModelId) throws IOException {
        Metadata.Builder metadata = Metadata.builder();
        for (Map.Entry<ProjectId, String> entry : projectToModelId.entrySet()) {
            metadata.put(
                ProjectMetadata.builder(entry.getKey()).putCustom(IngestMetadata.TYPE, ingestMetadataForModels(entry.getValue().split(",")))
            );
        }
        return metadata.build();
    }

    private static Metadata withIngestModelsAndAliases(Map<ProjectId, String> projectToModelKey, List<Tuple<String, String>> aliases)
        throws IOException {
        Metadata.Builder metadata = Metadata.builder();
        for (Map.Entry<ProjectId, String> entry : projectToModelKey.entrySet()) {
            ProjectMetadata.Builder project = ProjectMetadata.builder(entry.getKey())
                .putCustom(IngestMetadata.TYPE, ingestMetadataForModels(entry.getValue()));
            if (entry.getKey().equals(PROJECT_A)) {
                project.putCustom(
                    ModelAliasMetadata.NAME,
                    new ModelAliasMetadata(
                        aliases.stream().collect(Collectors.toMap(Tuple::v2, t -> new ModelAliasMetadata.ModelAliasEntry(t.v1())))
                    )
                );
            }
            metadata.put(project);
        }
        return metadata.build();
    }

    private static IngestMetadata ingestMetadataForModels(String... modelIds) throws IOException {
        Map<String, PipelineConfiguration> pipelines = Maps.newMapWithExpectedSize(modelIds.length);
        for (String modelId : modelIds) {
            pipelines.put("pipeline_" + modelId, pipelineWithModel(modelId));
        }
        return new IngestMetadata(pipelines);
    }

    private static PipelineConfiguration pipelineWithModel(String modelId) throws IOException {
        try (
            XContentBuilder builder = XContentFactory.jsonBuilder()
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
            return new PipelineConfiguration("pipeline_" + modelId, BytesReference.bytes(builder), XContentType.JSON);
        }
    }

    private static ClusterState masterClusterState(Metadata metadata) {
        DiscoveryNode masterNode = DiscoveryNodeUtils.create("master");
        return ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(DiscoveryNodes.builder().add(masterNode).masterNodeId("master").localNodeId("master").build())
            .metadata(metadata)
            .build();
    }

    private static ClusterState blockedMasterClusterState(Metadata metadata) {
        return ClusterState.builder(masterClusterState(metadata))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
            .build();
    }

    private static ClusterState nonMasterClusterState(Metadata metadata) {
        DiscoveryNode masterNode = DiscoveryNodeUtils.create("master");
        DiscoveryNode localNode = DiscoveryNodeUtils.create("data");
        return ClusterState.builder(new ClusterName("test-cluster"))
            .nodes(DiscoveryNodes.builder().add(masterNode).add(localNode).masterNodeId("master").localNodeId("data").build())
            .metadata(metadata)
            .build();
    }
}
