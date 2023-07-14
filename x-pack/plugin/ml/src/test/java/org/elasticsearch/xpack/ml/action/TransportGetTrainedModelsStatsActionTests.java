/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.internal.metering.EmptyMeteringCallback;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
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
                Map<String, Object> config
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
                new InferenceProcessor.Factory(parameters.client, parameters.ingestService.getClusterService(), Settings.EMPTY, true)
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
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING
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
                EmptyMeteringCallback.INSTANCE);
    }

    public void testInferenceIngestStatsByModelId() {
        List<NodeStats> nodeStatsList = Arrays.asList(
            buildNodeStats(
                new IngestStats.Stats(2, 2, 3, 4),
                Arrays.asList(
                    new IngestStats.PipelineStat("pipeline1", new IngestStats.Stats(0, 0, 3, 1)),
                    new IngestStats.PipelineStat("pipeline2", new IngestStats.Stats(1, 1, 0, 1)),
                    new IngestStats.PipelineStat("pipeline3", new IngestStats.Stats(2, 1, 1, 1))
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
                    new IngestStats.PipelineStat("pipeline1", new IngestStats.Stats(10, 1, 3, 1)),
                    new IngestStats.PipelineStat("pipeline2", new IngestStats.Stats(1, 1, 0, 1)),
                    new IngestStats.PipelineStat("pipeline3", new IngestStats.Stats(2, 1, 1, 1))
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
            Collections.singletonList(new IngestStats.PipelineStat("pipeline1", new IngestStats.Stats(10, 1, 6, 2))),
            Collections.singletonMap(
                "pipeline1",
                Arrays.asList(
                    new IngestStats.ProcessorStat("inference", "inference", new IngestStats.Stats(120, 12, 0, 1)),
                    new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0))
                )
            )
        );

        IngestStats expectedStatsModel2 = new IngestStats(
            new IngestStats.Stats(12, 3, 6, 4),
            Arrays.asList(
                new IngestStats.PipelineStat("pipeline1", new IngestStats.Stats(10, 1, 6, 2)),
                new IngestStats.PipelineStat("pipeline2", new IngestStats.Stats(2, 2, 0, 2))
            ),
            new HashMap<>() {
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
            }
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
            IntStream.range(0, pipelineids.size()).boxed().collect(Collectors.toMap(pipelineids::get, processorStats::get))
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
            null
        );

    }

}
