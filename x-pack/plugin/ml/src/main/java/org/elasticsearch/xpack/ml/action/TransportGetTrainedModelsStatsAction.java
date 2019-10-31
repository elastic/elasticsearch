/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;


public class TransportGetTrainedModelsStatsAction extends HandledTransportAction<GetTrainedModelsStatsAction.Request,
                                                                                 GetTrainedModelsStatsAction.Response> {

    private final Client client;
    private final ClusterService clusterService;
    private final IngestService ingestService;
    private final TrainedModelProvider trainedModelProvider;

    @Inject
    public TransportGetTrainedModelsStatsAction(TransportService transportService,
                                                ActionFilters actionFilters,
                                                ClusterService clusterService,
                                                IngestService ingestService,
                                                TrainedModelProvider trainedModelProvider,
                                                Client client) {
        super(GetTrainedModelsStatsAction.NAME, transportService, actionFilters, GetTrainedModelsStatsAction.Request::new);
        this.client = client;
        this.clusterService = clusterService;
        this.ingestService = ingestService;
        this.trainedModelProvider = trainedModelProvider;
    }

    @Override
    protected void doExecute(Task task,
                             GetTrainedModelsStatsAction.Request request,
                             ActionListener<GetTrainedModelsStatsAction.Response> listener) {

        GetTrainedModelsStatsAction.Response.Builder responseBuilder = new GetTrainedModelsStatsAction.Response.Builder();

        ActionListener<NodesStatsResponse> nodesStatsListener = ActionListener.wrap(
            nodesStatsResponse -> {
                Map<String, IngestStats> modelIdIngestStats = inferenceIngestStatsByPipelineId(nodesStatsResponse,
                    pipelineIdsByModelIds(clusterService.state(),
                        ingestService,
                        responseBuilder.getExpandedIds()));
                listener.onResponse(responseBuilder.setIngestStatsByModelId(modelIdIngestStats).build());
            },
            listener::onFailure
        );

        ActionListener<Tuple<Long, Set<String>>> idsListener = ActionListener.wrap(
            tuple -> {
                responseBuilder.setExpandedIds(tuple.v2())
                    .setTotalModelCount(tuple.v1());
                String[] ingestNodes = ingestNodes(clusterService.state());
                NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(ingestNodes).clear().ingest(true);
                executeAsyncWithOrigin(client, ML_ORIGIN, NodesStatsAction.INSTANCE, nodesStatsRequest, nodesStatsListener);
            },
            listener::onFailure
        );

        trainedModelProvider.expandIds(request.getResourceId(), request.isAllowNoResources(), request.getPageParams(), idsListener);
    }

    static Map<String, IngestStats> inferenceIngestStatsByPipelineId(NodesStatsResponse response,
                                                                     Map<String, Set<String>> modelIdToPipelineId) {

        Map<String, IngestStats> ingestStatsMap = new HashMap<>();

        modelIdToPipelineId.forEach((modelId, pipelineIds) -> {
            List<IngestStats> collectedStats = response.getNodes()
                .stream()
                .map(nodeStats -> ingestStatsForPipelineIds(nodeStats, pipelineIds))
                .collect(Collectors.toList());
            ingestStatsMap.put(modelId, mergeStats(collectedStats));
        });

        return ingestStatsMap;
    }

    static String[] ingestNodes(final ClusterState clusterState) {
        String[] ingestNodes = new String[clusterState.nodes().getIngestNodes().size()];
        Iterator<String> nodeIterator = clusterState.nodes().getIngestNodes().keysIt();
        int i = 0;
        while(nodeIterator.hasNext()) {
            ingestNodes[i++] = nodeIterator.next();
        }
        return ingestNodes;
    }

    static Map<String, Set<String>> pipelineIdsByModelIds(ClusterState state, IngestService ingestService, Set<String> modelIds) {
        IngestMetadata ingestMetadata = state.metaData().custom(IngestMetadata.TYPE);
        Map<String, Set<String>> pipelineIdsByModelIds = new HashMap<>();
        if (ingestMetadata == null) {
            return pipelineIdsByModelIds;
        }

        ingestMetadata.getPipelines().forEach((pipelineId, pipelineConfiguration) -> {
            try {
                Pipeline pipeline = Pipeline.create(pipelineId,
                    pipelineConfiguration.getConfigAsMap(),
                    ingestService.getProcessorFactories(),
                    ingestService.getScriptService());
                pipeline.getProcessors().forEach(processor -> {
                    if (processor instanceof InferenceProcessor) {
                        InferenceProcessor inferenceProcessor = (InferenceProcessor) processor;
                        if (modelIds.contains(inferenceProcessor.getModelId())) {
                            pipelineIdsByModelIds.computeIfAbsent(inferenceProcessor.getModelId(),
                                                                  m -> new LinkedHashSet<>()).add(pipelineId);
                        }
                    }
                });
            } catch (Exception ex) {
                throw new ElasticsearchException("unexpected failure gathering pipeline information", ex);
            }
        });

        return pipelineIdsByModelIds;
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
            filteredProcessorStats);
    }

    private static IngestStats mergeStats(List<IngestStats> ingestStatsList) {

        Map<String, IngestStatsAccumulator> pipelineStatsAcc = new LinkedHashMap<>(ingestStatsList.size());
        Map<String, Map<String, IngestStatsAccumulator>> processorStatsAcc = new LinkedHashMap<>(ingestStatsList.size());
        IngestStatsAccumulator totalStats = new IngestStatsAccumulator();
        ingestStatsList.forEach(ingestStats -> {

            ingestStats.getPipelineStats()
                .forEach(pipelineStat ->
                    pipelineStatsAcc.computeIfAbsent(pipelineStat.getPipelineId(),
                                                     p -> new IngestStatsAccumulator()).inc(pipelineStat.getStats()));

            ingestStats.getProcessorStats()
                .forEach((pipelineId, processorStat) -> {
                    Map<String, IngestStatsAccumulator> processorAcc = processorStatsAcc.computeIfAbsent(pipelineId,
                                                                                                         k -> new LinkedHashMap<>());
                        processorStat.forEach(p ->
                            processorAcc.computeIfAbsent(p.getName(),
                                                         k -> new IngestStatsAccumulator(p.getType())).inc(p.getStats()));
                    });

            totalStats.inc(ingestStats.getTotalStats());
        });

        List<IngestStats.PipelineStat> pipelineStatList = new ArrayList<>(pipelineStatsAcc.size());
        pipelineStatsAcc.forEach((pipelineId, accumulator) ->
            pipelineStatList.add(new IngestStats.PipelineStat(pipelineId, accumulator.build())));

        Map<String, List<IngestStats.ProcessorStat>> processorStatList = new LinkedHashMap<>(processorStatsAcc.size());
        processorStatsAcc.forEach((pipelineId, accumulatorMap) -> {
            List<IngestStats.ProcessorStat> processorStats = new ArrayList<>(accumulatorMap.size());
            accumulatorMap.forEach((processorName, acc) ->
                processorStats.add(new IngestStats.ProcessorStat(processorName, acc.type, acc.build())));
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

        IngestStatsAccumulator inc(IngestStats.Stats s) {
            ingestCount.inc(s.getIngestCount());
            ingestTimeInMillis.inc(s.getIngestTimeInMillis());
            ingestCurrent.inc(s.getIngestCurrent());
            ingestFailedCount.inc(s.getIngestFailedCount());
            return this;
        }

        IngestStats.Stats build() {
            return new IngestStats.Stats(ingestCount.count(), ingestTimeInMillis.count(), ingestCurrent.count(), ingestFailedCount.count());
        }
    }

}
