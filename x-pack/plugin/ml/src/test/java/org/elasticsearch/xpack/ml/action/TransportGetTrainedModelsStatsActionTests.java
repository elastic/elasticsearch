/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;
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

    private Client client;

    @Before
    public void setUpVariables() {
        ThreadPool tp = mock(ThreadPool.class);
        when(tp.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
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

        Map<String, Set<String>> pipelineIdsByModelIds = new HashMap<String, Set<String>>() {
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
            new HashMap<String, List<IngestStats.ProcessorStat>>() {
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
        List<String> pipelineids = pipelineNames.stream().map(IngestStats.PipelineStat::getPipelineId).collect(Collectors.toList());
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
            null
        );

    }

}
