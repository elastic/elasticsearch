/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.ml.utils.InferenceProcessorInfoExtractor.pipelineIdsByModelIdsOrAliases;

public class TransportGetTrainedModelsStatsAction extends HandledTransportAction<
    GetTrainedModelsStatsAction.Request,
    GetTrainedModelsStatsAction.Response> {

    private final Client client;
    private final ClusterService clusterService;
    private final TrainedModelProvider trainedModelProvider;

    @Inject
    public TransportGetTrainedModelsStatsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        TrainedModelProvider trainedModelProvider,
        Client client
    ) {
        super(GetTrainedModelsStatsAction.NAME, transportService, actionFilters, GetTrainedModelsStatsAction.Request::new);
        this.client = client;
        this.clusterService = clusterService;
        this.trainedModelProvider = trainedModelProvider;
    }

    @Override
    protected void doExecute(
        Task task,
        GetTrainedModelsStatsAction.Request request,
        ActionListener<GetTrainedModelsStatsAction.Response> listener
    ) {

        final ModelAliasMetadata currentMetadata = ModelAliasMetadata.fromState(clusterService.state());
        GetTrainedModelsStatsAction.Response.Builder responseBuilder = new GetTrainedModelsStatsAction.Response.Builder();

        ActionListener<List<InferenceStats>> inferenceStatsListener = ActionListener.wrap(
            inferenceStats -> listener.onResponse(
                responseBuilder.setInferenceStatsByModelId(
                    inferenceStats.stream().collect(Collectors.toMap(InferenceStats::getModelId, Function.identity()))
                ).build()
            ),
            listener::onFailure
        );

        ActionListener<NodesStatsResponse> nodesStatsListener = ActionListener.wrap(nodesStatsResponse -> {
            Set<String> allPossiblePipelineReferences = responseBuilder.getExpandedIdsWithAliases()
                .entrySet()
                .stream()
                .flatMap(entry -> Stream.concat(entry.getValue().stream(), Stream.of(entry.getKey())))
                .collect(Collectors.toSet());
            Map<String, Set<String>> pipelineIdsByModelIdsOrAliases = pipelineIdsByModelIdsOrAliases(
                clusterService.state(),
                allPossiblePipelineReferences
            );
            Map<String, IngestStats> modelIdIngestStats = inferenceIngestStatsByModelId(
                nodesStatsResponse,
                currentMetadata,
                pipelineIdsByModelIdsOrAliases
            );
            responseBuilder.setIngestStatsByModelId(modelIdIngestStats);
            trainedModelProvider.getInferenceStats(
                responseBuilder.getExpandedIdsWithAliases().keySet().toArray(new String[0]),
                inferenceStatsListener
            );
        }, listener::onFailure);

        ActionListener<Tuple<Long, Map<String, Set<String>>>> idsListener = ActionListener.wrap(tuple -> {
            responseBuilder.setExpandedIdsWithAliases(tuple.v2()).setTotalModelCount(tuple.v1());
            String[] ingestNodes = ingestNodes(clusterService.state());
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(ingestNodes).clear()
                .addMetric(NodesStatsRequest.Metric.INGEST.metricName());
            executeAsyncWithOrigin(client, ML_ORIGIN, NodesStatsAction.INSTANCE, nodesStatsRequest, nodesStatsListener);
        }, listener::onFailure);
        trainedModelProvider.expandIds(
            request.getResourceId(),
            request.isAllowNoResources(),
            request.getPageParams(),
            Collections.emptySet(),
            currentMetadata,
            idsListener
        );
    }

    static Map<String, IngestStats> inferenceIngestStatsByModelId(
        NodesStatsResponse response,
        ModelAliasMetadata currentMetadata,
        Map<String, Set<String>> modelIdToPipelineId
    ) {

        Map<String, IngestStats> ingestStatsMap = new HashMap<>();
        Map<String, Set<String>> trueModelIdToPipelines = modelIdToPipelineId.entrySet().stream().collect(Collectors.toMap(entry -> {
            String maybeModelId = currentMetadata.getModelId(entry.getKey());
            return maybeModelId == null ? entry.getKey() : maybeModelId;
        }, Map.Entry::getValue, Sets::union));
        trueModelIdToPipelines.forEach((modelId, pipelineIds) -> {
            List<IngestStats> collectedStats = response.getNodes()
                .stream()
                .map(nodeStats -> ingestStatsForPipelineIds(nodeStats, pipelineIds))
                .collect(Collectors.toList());
            ingestStatsMap.put(modelId, mergeStats(collectedStats));
        });
        return ingestStatsMap;
    }

    static String[] ingestNodes(final ClusterState clusterState) {
        return clusterState.nodes().getIngestNodes().keySet().toArray(new String[0]);
    }

    static IngestStats ingestStatsForPipelineIds(NodeStats nodeStats, Set<String> pipelineIds) {
        IngestStats fullNodeStats = nodeStats.getIngestStats();
        Map<String, List<IngestStats.ProcessorStat>> filteredProcessorStats = new HashMap<>(fullNodeStats.getProcessorStats());
        filteredProcessorStats.keySet().retainAll(pipelineIds);
        List<IngestStats.PipelineStat> filteredPipelineStats = fullNodeStats.getPipelineStats()
            .stream()
            .filter(pipelineStat -> pipelineIds.contains(pipelineStat.getPipelineId()))
            .collect(Collectors.toList());
        CounterMetric ingestCount = new CounterMetric();
        CounterMetric ingestTimeInMillis = new CounterMetric();
        CounterMetric ingestCurrent = new CounterMetric();
        CounterMetric ingestFailedCount = new CounterMetric();

        filteredPipelineStats.forEach(pipelineStat -> {
            IngestStats.Stats stats = pipelineStat.getStats();
            ingestCount.inc(stats.getIngestCount());
            ingestTimeInMillis.inc(stats.getIngestTimeInMillis());
            ingestCurrent.inc(stats.getIngestCurrent());
            ingestFailedCount.inc(stats.getIngestFailedCount());
        });

        return new IngestStats(
            new IngestStats.Stats(ingestCount.count(), ingestTimeInMillis.count(), ingestCurrent.count(), ingestFailedCount.count()),
            filteredPipelineStats,
            filteredProcessorStats
        );
    }

    private static IngestStats mergeStats(List<IngestStats> ingestStatsList) {

        Map<String, IngestStatsAccumulator> pipelineStatsAcc = new LinkedHashMap<>(ingestStatsList.size());
        Map<String, Map<String, IngestStatsAccumulator>> processorStatsAcc = new LinkedHashMap<>(ingestStatsList.size());
        IngestStatsAccumulator totalStats = new IngestStatsAccumulator();
        ingestStatsList.forEach(ingestStats -> {

            ingestStats.getPipelineStats()
                .forEach(
                    pipelineStat -> pipelineStatsAcc.computeIfAbsent(pipelineStat.getPipelineId(), p -> new IngestStatsAccumulator())
                        .inc(pipelineStat.getStats())
                );

            ingestStats.getProcessorStats().forEach((pipelineId, processorStat) -> {
                Map<String, IngestStatsAccumulator> processorAcc = processorStatsAcc.computeIfAbsent(
                    pipelineId,
                    k -> new LinkedHashMap<>()
                );
                processorStat.forEach(
                    p -> processorAcc.computeIfAbsent(p.getName(), k -> new IngestStatsAccumulator(p.getType())).inc(p.getStats())
                );
            });

            totalStats.inc(ingestStats.getTotalStats());
        });

        List<IngestStats.PipelineStat> pipelineStatList = new ArrayList<>(pipelineStatsAcc.size());
        pipelineStatsAcc.forEach(
            (pipelineId, accumulator) -> pipelineStatList.add(new IngestStats.PipelineStat(pipelineId, accumulator.build()))
        );

        Map<String, List<IngestStats.ProcessorStat>> processorStatList = new LinkedHashMap<>(processorStatsAcc.size());
        processorStatsAcc.forEach((pipelineId, accumulatorMap) -> {
            List<IngestStats.ProcessorStat> processorStats = new ArrayList<>(accumulatorMap.size());
            accumulatorMap.forEach(
                (processorName, acc) -> processorStats.add(new IngestStats.ProcessorStat(processorName, acc.type, acc.build()))
            );
            processorStatList.put(pipelineId, processorStats);
        });

        return new IngestStats(totalStats.build(), pipelineStatList, processorStatList);
    }

    private static class IngestStatsAccumulator {
        CounterMetric ingestCount = new CounterMetric();
        CounterMetric ingestTimeInMillis = new CounterMetric();
        CounterMetric ingestCurrent = new CounterMetric();
        CounterMetric ingestFailedCount = new CounterMetric();

        String type;

        IngestStatsAccumulator() {}

        IngestStatsAccumulator(String type) {
            this.type = type;
        }

        void inc(IngestStats.Stats s) {
            ingestCount.inc(s.getIngestCount());
            ingestTimeInMillis.inc(s.getIngestTimeInMillis());
            ingestCurrent.inc(s.getIngestCurrent());
            ingestFailedCount.inc(s.getIngestFailedCount());
        }

        IngestStats.Stats build() {
            return new IngestStats.Stats(ingestCount.count(), ingestTimeInMillis.count(), ingestCurrent.count(), ingestFailedCount.count());
        }
    }

}
