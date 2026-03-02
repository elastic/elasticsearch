/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.bulk.FailureStoreMetrics;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelSizeStats;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.junit.Before;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetTrainedModelsStatsActionTests extends ESTestCase {

    private static class NotInferenceProcessor implements Processor {

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            return ingestDocument;
        }

        @Override
        public String getType() {
            return "not_inference";
        }

        @Override
        public String getTag() {
            return null;
        }

        @Override
        public String getDescription() {
            return null;
        }

        static class Factory implements Processor.Factory {

            @Override
            public Processor create(
                Map<String, Processor.Factory> processorFactories,
                String tag,
                String description,
                Map<String, Object> config,
                ProjectId projectId
            ) {
                return new NotInferenceProcessor();
            }
        }
    }

    private static final IngestPlugin SKINNY_INGEST_PLUGIN = new IngestPlugin() {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> factoryMap = new HashMap<>();
            MockLicenseState licenseState = mock(MockLicenseState.class);
            when(licenseState.isAllowed(MachineLearningField.ML_API_FEATURE)).thenReturn(true);
            factoryMap.put(
                InferenceProcessor.TYPE,
                new InferenceProcessor.Factory(
                    parameters.client,
                    parameters.ingestService.getClusterService(),
                    Settings.EMPTY,
                    new SetOnce<>(mock(InferenceAuditor.class))
                )
            );

            factoryMap.put("not_inference", new NotInferenceProcessor.Factory());

            return factoryMap;
        }
    };

    private ClusterService clusterService;
    private IngestService ingestService;
    private Client client;

    @Before
    public void setUpVariables() {
        ThreadPool tp = mock(ThreadPool.class);
        when(tp.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(tp.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        Settings settings = Settings.builder().put("node.name", "InferenceProcessorFactoryTests_node").build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            new HashSet<>(
                Arrays.asList(
                    InferenceProcessor.MAX_INFERENCE_PROCESSORS,
                    MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                    ClusterService.USER_DEFINED_METADATA,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_THREAD_DUMP_TIMEOUT_SETTING,
                    ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL,
                    ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_QUIET_TIME
                )
            )
        );
        clusterService = new ClusterService(settings, clusterSettings, tp, null);
        ingestService = new IngestService(
            clusterService,
            tp,
            null,
            null,
            null,
            Collections.singletonList(SKINNY_INGEST_PLUGIN),
            client,
            null,
            FailureStoreMetrics.NOOP,
            TestProjectResolvers.alwaysThrow(),
            new FeatureService(List.of()) {
                @Override
                public boolean clusterHasFeature(ClusterState state, NodeFeature feature) {
                    return DataStream.DATA_STREAM_FAILURE_STORE_FEATURE.equals(feature);
                }
            },
            mock(SamplingService.class)
        );
    }

    public void testInferenceIngestStatsByModelId() {
        List<NodeStats> nodeStatsList = Arrays.asList(
            buildNodeStats(
                new IngestStats.Stats(2, 2, 3, 4),
                Arrays.asList(
                    new IngestStats.PipelineStat(
                        ProjectId.DEFAULT,
                        "pipeline1",
                        new IngestStats.Stats(0, 0, 3, 1),
                        new IngestStats.ByteStats(789, 0)
                    ),
                    new IngestStats.PipelineStat(
                        ProjectId.DEFAULT,
                        "pipeline2",
                        new IngestStats.Stats(1, 1, 0, 1),
                        new IngestStats.ByteStats(123, 123)
                    ),
                    new IngestStats.PipelineStat(
                        ProjectId.DEFAULT,
                        "pipeline3",
                        new IngestStats.Stats(2, 1, 1, 1),
                        new IngestStats.ByteStats(1234, 5678)
                    )
                ),
                Arrays.asList(
                    Arrays.asList(
                        new IngestStats.ProcessorStat(InferenceProcessor.TYPE, InferenceProcessor.TYPE, new IngestStats.Stats(10, 1, 0, 0)),
                        new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0)),
                        new IngestStats.ProcessorStat(
                            InferenceProcessor.TYPE,
                            InferenceProcessor.TYPE,
                            new IngestStats.Stats(100, 10, 0, 1)
                        )
                    ),
                    Arrays.asList(
                        new IngestStats.ProcessorStat(InferenceProcessor.TYPE, InferenceProcessor.TYPE, new IngestStats.Stats(5, 1, 0, 0)),
                        new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0))
                    ),
                    Arrays.asList(new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0)))
                )
            ),
            buildNodeStats(
                new IngestStats.Stats(15, 5, 3, 4),
                Arrays.asList(
                    new IngestStats.PipelineStat(
                        ProjectId.DEFAULT,
                        "pipeline1",
                        new IngestStats.Stats(10, 1, 3, 1),
                        new IngestStats.ByteStats(5678, 123456)
                    ),
                    new IngestStats.PipelineStat(
                        ProjectId.DEFAULT,
                        "pipeline2",
                        new IngestStats.Stats(1, 1, 0, 1),
                        new IngestStats.ByteStats(111, 222)
                    ),
                    new IngestStats.PipelineStat(
                        ProjectId.DEFAULT,
                        "pipeline3",
                        new IngestStats.Stats(2, 1, 1, 1),
                        new IngestStats.ByteStats(555, 777)
                    )
                ),
                Arrays.asList(
                    Arrays.asList(
                        new IngestStats.ProcessorStat(InferenceProcessor.TYPE, InferenceProcessor.TYPE, new IngestStats.Stats(0, 0, 0, 0)),
                        new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(0, 0, 0, 0)),
                        new IngestStats.ProcessorStat(InferenceProcessor.TYPE, InferenceProcessor.TYPE, new IngestStats.Stats(10, 1, 0, 0))
                    ),
                    Arrays.asList(
                        new IngestStats.ProcessorStat(InferenceProcessor.TYPE, InferenceProcessor.TYPE, new IngestStats.Stats(5, 1, 0, 0)),
                        new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0))
                    ),
                    Arrays.asList(new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0)))
                )
            )
        );

        NodesStatsResponse response = new NodesStatsResponse(new ClusterName("_name"), nodeStatsList, Collections.emptyList());

        Map<String, Set<String>> pipelineIdsByModelIds = new HashMap<>() {
            {
                put("trained_model_1", Collections.singleton("pipeline1"));
                put("trained_model_2", new HashSet<>(Arrays.asList("pipeline1", "pipeline2")));
            }
        };
        Map<String, IngestStats> ingestStatsMap = TransportGetTrainedModelsStatsAction.inferenceIngestStatsByModelId(
            response,
            ModelAliasMetadata.EMPTY,
            pipelineIdsByModelIds
        );

        assertThat(ingestStatsMap.keySet(), equalTo(new HashSet<>(Arrays.asList("trained_model_1", "trained_model_2"))));

        IngestStats expectedStatsModel1 = new IngestStats(
            new IngestStats.Stats(10, 1, 6, 2),
            Collections.singletonList(
                new IngestStats.PipelineStat(
                    ProjectId.DEFAULT,
                    "pipeline1",
                    new IngestStats.Stats(10, 1, 6, 2),
                    new IngestStats.ByteStats(6467, 123456)
                )
            ),
            Map.of(
                ProjectId.DEFAULT,
                Collections.singletonMap(
                    "pipeline1",
                    Arrays.asList(
                        new IngestStats.ProcessorStat("inference", "inference", new IngestStats.Stats(120, 12, 0, 1)),
                        new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0))
                    )
                )
            )
        );

        IngestStats expectedStatsModel2 = new IngestStats(
            new IngestStats.Stats(12, 3, 6, 4),
            Arrays.asList(
                new IngestStats.PipelineStat(
                    ProjectId.DEFAULT,
                    "pipeline1",
                    new IngestStats.Stats(10, 1, 6, 2),
                    new IngestStats.ByteStats(6467, 123456)
                ),
                new IngestStats.PipelineStat(
                    ProjectId.DEFAULT,
                    "pipeline2",
                    new IngestStats.Stats(2, 2, 0, 2),
                    new IngestStats.ByteStats(234, 345)
                )
            ),
            Map.of(ProjectId.DEFAULT, new HashMap<>() {
                {
                    put(
                        "pipeline2",
                        Arrays.asList(
                            new IngestStats.ProcessorStat("inference", "inference", new IngestStats.Stats(10, 2, 0, 0)),
                            new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(20, 2, 0, 0))
                        )
                    );
                    put(
                        "pipeline1",
                        Arrays.asList(
                            new IngestStats.ProcessorStat("inference", "inference", new IngestStats.Stats(120, 12, 0, 1)),
                            new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0))
                        )
                    );
                }
            })
        );

        assertThat(ingestStatsMap, hasEntry("trained_model_1", expectedStatsModel1));
        assertThat(ingestStatsMap, hasEntry("trained_model_2", expectedStatsModel2));
    }

    private static NodeStats buildNodeStats(
        IngestStats.Stats overallStats,
        List<IngestStats.PipelineStat> pipelineNames,
        List<List<IngestStats.ProcessorStat>> processorStats
    ) {
        List<String> pipelineids = pipelineNames.stream().map(IngestStats.PipelineStat::pipelineId).collect(Collectors.toList());
        IngestStats ingestStats = new IngestStats(
            overallStats,
            pipelineNames,
            Map.of(
                ProjectId.DEFAULT,
                IntStream.range(0, pipelineids.size()).boxed().collect(Collectors.toMap(pipelineids::get, processorStats::get))
            )
        );
        return new NodeStats(
            mock(DiscoveryNode.class),
            Instant.now().toEpochMilli(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            ingestStats,
            null,
            null,
            null,
            null,
            null
        );

    }

    /**
     * Tests that deployed models use deployment-specific memory statistics rather than shared values.
     * When multiple models are deployed with different allocation counts, each model's
     * required_native_memory_bytes should reflect its own deployment's allocation count,
     * not an aggregate value across all deployments.
     */
    public void testBuilderResolvesModelSizeStatsByDeploymentId() {
        AssignmentStats deployment1 = new AssignmentStats(
            "dep1",
            "modelA",
            1,
            2,
            null,
            null,
            null,
            Instant.now(),
            List.of(),
            Priority.NORMAL
        );
        AssignmentStats deployment2 = new AssignmentStats(
            "dep2",
            "modelB",
            1,
            3,
            null,
            null,
            null,
            Instant.now(),
            List.of(),
            Priority.NORMAL
        );

        Map<String, AssignmentStats> assignmentStatsMap = Map.of("dep1", deployment1, "dep2", deployment2);

        Map<String, TrainedModelSizeStats> modelSizeStatsMap = Map.of(
            "dep1",
            new TrainedModelSizeStats(1000L, 2000L),
            "dep2",
            new TrainedModelSizeStats(1000L, 3000L)
        );

        Map<String, Set<String>> expandedModelIdsWithAliases = Map.of("modelA", Set.of(), "modelB", Set.of());

        Map<String, Set<String>> modelToDeploymentIds = Map.of("modelA", Set.of("dep1"), "modelB", Set.of("dep2"));

        GetTrainedModelsStatsAction.Response.Builder builder = new GetTrainedModelsStatsAction.Response.Builder();
        builder.setTotalModelCount(2);
        builder.setExpandedModelIdsWithAliases(expandedModelIdsWithAliases);
        builder.setModelSizeStatsByModelId(modelSizeStatsMap);
        builder.setDeploymentStatsByDeploymentId(assignmentStatsMap);

        GetTrainedModelsStatsAction.Response response = builder.build(modelToDeploymentIds);

        List<GetTrainedModelsStatsAction.Response.TrainedModelStats> results = response.getResources().results();
        assertThat(results.size(), equalTo(2));

        GetTrainedModelsStatsAction.Response.TrainedModelStats statsA = results.stream()
            .filter(s -> s.getModelId().equals("modelA"))
            .findFirst()
            .orElse(null);
        assertThat(statsA, notNullValue());
        assertThat(statsA.getModelSizeStats(), notNullValue());
        assertThat(statsA.getModelSizeStats().getRequiredNativeMemoryBytes(), equalTo(2000L));

        GetTrainedModelsStatsAction.Response.TrainedModelStats statsB = results.stream()
            .filter(s -> s.getModelId().equals("modelB"))
            .findFirst()
            .orElse(null);
        assertThat(statsB, notNullValue());
        assertThat(statsB.getModelSizeStats(), notNullValue());
        assertThat(statsB.getModelSizeStats().getRequiredNativeMemoryBytes(), equalTo(3000L));
    }

    /**
     * Tests that undeployed models use model ID for size stats lookup.
     * When a model is not deployed, there are no deployment-specific statistics available,
     * so the system should use the model ID as the key to retrieve size information.
     */
    public void testBuilderFallsBackToModelIdForUndeployedModels() {
        Map<String, TrainedModelSizeStats> modelSizeStatsMap = Map.of("modelUndeployed", new TrainedModelSizeStats(1000L, 5000L));

        Map<String, Set<String>> expandedModelIdsWithAliases = Map.of("modelUndeployed", Set.of());

        Map<String, Set<String>> modelToDeploymentIds = Map.of();

        GetTrainedModelsStatsAction.Response.Builder builder = new GetTrainedModelsStatsAction.Response.Builder();
        builder.setTotalModelCount(1);
        builder.setExpandedModelIdsWithAliases(expandedModelIdsWithAliases);
        builder.setModelSizeStatsByModelId(modelSizeStatsMap);

        GetTrainedModelsStatsAction.Response response = builder.build(modelToDeploymentIds);

        List<GetTrainedModelsStatsAction.Response.TrainedModelStats> results = response.getResources().results();
        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0).getModelId(), equalTo("modelUndeployed"));
        assertThat(results.get(0).getModelSizeStats(), notNullValue());
        assertThat(results.get(0).getModelSizeStats().getRequiredNativeMemoryBytes(), equalTo(5000L));
    }

    /**
     * Tests that response results are sorted consistently by model ID first, then by deployment ID.
     * When a single model has multiple deployments, this ensures deterministic API responses
     * regardless of the order in which deployments are processed internally.
     */
    public void testBuilderSortsByModelIdThenDeploymentId() {
        AssignmentStats dep1 = new AssignmentStats("dep1", "modelA", 1, 2, null, null, null, Instant.now(), List.of(), Priority.NORMAL);
        AssignmentStats dep2 = new AssignmentStats("dep2", "modelA", 1, 3, null, null, null, Instant.now(), List.of(), Priority.NORMAL);

        Map<String, AssignmentStats> assignmentStatsMap = Map.of("dep1", dep1, "dep2", dep2);

        Map<String, TrainedModelSizeStats> modelSizeStatsMap = Map.of(
            "dep1",
            new TrainedModelSizeStats(1000L, 2000L),
            "dep2",
            new TrainedModelSizeStats(1000L, 3000L)
        );

        Map<String, Set<String>> expandedModelIdsWithAliases = Map.of("modelA", Set.of());
        Map<String, Set<String>> modelToDeploymentIds = Map.of("modelA", Set.of("dep1", "dep2"));

        GetTrainedModelsStatsAction.Response.Builder builder = new GetTrainedModelsStatsAction.Response.Builder();
        builder.setTotalModelCount(1);
        builder.setExpandedModelIdsWithAliases(expandedModelIdsWithAliases);
        builder.setModelSizeStatsByModelId(modelSizeStatsMap);
        builder.setDeploymentStatsByDeploymentId(assignmentStatsMap);

        GetTrainedModelsStatsAction.Response response = builder.build(modelToDeploymentIds);

        List<GetTrainedModelsStatsAction.Response.TrainedModelStats> results = response.getResources().results();
        assertThat(results.size(), equalTo(2));
        assertThat(results.get(0).getModelId(), equalTo("modelA"));
        assertThat(results.get(0).getDeploymentStats().getDeploymentId(), equalTo("dep1"));
        assertThat(results.get(1).getModelId(), equalTo("modelA"));
        assertThat(results.get(1).getDeploymentStats().getDeploymentId(), equalTo("dep2"));
    }

    /**
     * Tests the stats lookup fallback behavior for deployed models.
     * When a deployment ID is not found in the size stats map (such as for non-PyTorch models
     * that don't require per-deployment memory calculations), the system should fall back to
     * using the model ID for stats lookup.
     */
    public void testBuilderFallsBackToModelIdWhenDeploymentIdNotInSizeStatsMap() {
        AssignmentStats deployment = new AssignmentStats(
            "dep1",
            "modelNonPytorch",
            1,
            1,
            null,
            null,
            null,
            Instant.now(),
            List.of(),
            Priority.NORMAL
        );

        Map<String, AssignmentStats> assignmentStatsMap = Map.of("dep1", deployment);

        Map<String, TrainedModelSizeStats> modelSizeStatsMap = Map.of("modelNonPytorch", new TrainedModelSizeStats(500L, 0L));

        Map<String, Set<String>> expandedModelIdsWithAliases = Map.of("modelNonPytorch", Set.of());

        Map<String, Set<String>> modelToDeploymentIds = Map.of("modelNonPytorch", Set.of("dep1"));

        GetTrainedModelsStatsAction.Response.Builder builder = new GetTrainedModelsStatsAction.Response.Builder();
        builder.setTotalModelCount(1);
        builder.setExpandedModelIdsWithAliases(expandedModelIdsWithAliases);
        builder.setModelSizeStatsByModelId(modelSizeStatsMap);
        builder.setDeploymentStatsByDeploymentId(assignmentStatsMap);

        GetTrainedModelsStatsAction.Response response = builder.build(modelToDeploymentIds);

        List<GetTrainedModelsStatsAction.Response.TrainedModelStats> results = response.getResources().results();
        assertThat(results.size(), equalTo(1));
        assertThat(results.get(0).getModelId(), equalTo("modelNonPytorch"));
        assertThat(results.get(0).getModelSizeStats(), notNullValue());
        assertThat(results.get(0).getModelSizeStats().getRequiredNativeMemoryBytes(), equalTo(0L));
    }

    /**
     * When {@code AssignmentStats.getNumberOfAllocations()} returns {@code null} (mixed-cluster or adaptive
     * allocation scenarios), {@code buildModelSizeStatsByKey} must treat it as 0 rather than NPE.
     */
    public void testBuildModelSizeStatsByKeyNullAllocationsDefaultsToZero() {
        TrainedModelConfig pytorchModel = TrainedModelConfig.builder()
            .setModelId("model-null-alloc")
            .setModelType(TrainedModelType.PYTORCH)
            .setInput(new TrainedModelInput(List.of()))
            .build();

        AssignmentStats statsNullAlloc = new AssignmentStats(
            "dep-null-alloc",
            "model-null-alloc",
            1,
            null, // numberOfAllocations = null
            null,
            null,
            null,
            Instant.now(),
            List.of(),
            Priority.NORMAL
        );

        Map<String, AssignmentStats> deploymentStats = Map.of("dep-null-alloc", statsNullAlloc);
        Map<String, Long> definitionLengths = Map.of("model-null-alloc", 1000L);

        Map<String, TrainedModelSizeStats> result = TransportGetTrainedModelsStatsAction.buildModelSizeStatsByKey(
            List.of(pytorchModel),
            definitionLengths,
            deploymentStats
        );

        assertThat(result.containsKey("dep-null-alloc"), equalTo(true));
        // With 0 allocations the estimator must still return a non-negative value (no NPE, no negative)
        assertThat(result.get("dep-null-alloc").getRequiredNativeMemoryBytes() >= 0, equalTo(true));
    }

    /** An undeployed PyTorch model (no entry in deploymentStats) must be keyed by model ID with 0 allocations. */
    public void testBuildModelSizeStatsByKeyUndeployedPytorchKeyedByModelId() {
        TrainedModelConfig pytorchModel = TrainedModelConfig.builder()
            .setModelId("undeployed-pytorch")
            .setModelType(TrainedModelType.PYTORCH)
            .setInput(new TrainedModelInput(List.of()))
            .build();

        Map<String, Long> definitionLengths = Map.of("undeployed-pytorch", 2000L);

        Map<String, TrainedModelSizeStats> result = TransportGetTrainedModelsStatsAction.buildModelSizeStatsByKey(
            List.of(pytorchModel),
            definitionLengths,
            Map.of() // no deployments
        );

        assertThat(result.containsKey("undeployed-pytorch"), equalTo(true));
        assertThat(result.containsKey("dep1"), equalTo(false)); // must NOT be keyed by a deployment id
    }

    /** A non-PyTorch model must always be keyed by model ID regardless of deployments. */
    public void testBuildModelSizeStatsByKeyNonPytorchKeyedByModelId() {
        TrainedModelConfig nonPytorchModel = TrainedModelConfig.builder()
            .setModelId("tree-model")
            .setModelType(TrainedModelType.TREE_ENSEMBLE)
            .setModelSize(512L)
            .setInput(new TrainedModelInput(List.of()))
            .build();

        Map<String, TrainedModelSizeStats> result = TransportGetTrainedModelsStatsAction.buildModelSizeStatsByKey(
            List.of(nonPytorchModel),
            Map.of(), // no definition lengths for non-pytorch
            Map.of()  // no deployments
        );

        assertThat(result.containsKey("tree-model"), equalTo(true));
        assertThat(result.get("tree-model").getModelSizeBytes(), equalTo(512L));
        assertThat(result.get("tree-model").getRequiredNativeMemoryBytes(), equalTo(0L));
    }

}
