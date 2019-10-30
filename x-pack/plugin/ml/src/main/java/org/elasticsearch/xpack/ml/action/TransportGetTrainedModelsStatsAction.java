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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;

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

    @Inject
    public TransportGetTrainedModelsStatsAction(TransportService transportService,
                                                ActionFilters actionFilters,
                                                ClusterService clusterService,
                                                IngestService ingestService,
                                                Client client) {
        super(GetTrainedModelsStatsAction.NAME, transportService, actionFilters, GetTrainedModelsStatsAction.Request::new);
        this.client = client;
        this.clusterService = clusterService;
        this.ingestService = ingestService;
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

        expandIds(request, idsListener);
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


    private void expandIds(GetTrainedModelsStatsAction.Request request, ActionListener<Tuple<Long, Set<String>>> idsListener) {
        String[] tokens = Strings.tokenizeToStringArray(request.getResourceId(), ",");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
            .sort(SortBuilders.fieldSort(request.getResourceIdField())
                // If there are no resources, there might be no mapping for the id field.
                // This makes sure we don't get an error if that happens.
                .unmappedType("long"))
            .query(buildQuery(tokens, request.getResourceIdField()));
        if (request.getPageParams() != null) {
            sourceBuilder.from(request.getPageParams().getFrom())
                .size(request.getPageParams().getSize());
        }
        sourceBuilder.trackTotalHits(true)
            // we only care about the item id's, there is no need to load large model definitions.
            .fetchSource(TrainedModelConfig.MODEL_ID.getPreferredName(), null);

        IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;
        SearchRequest searchRequest = new SearchRequest(InferenceIndexConstants.INDEX_PATTERN)
            .indicesOptions(IndicesOptions.fromOptions(true,
                indicesOptions.allowNoIndices(),
                indicesOptions.expandWildcardsOpen(),
                indicesOptions.expandWildcardsClosed(),
                indicesOptions))
            .source(sourceBuilder);

        executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(
                response -> {
                    Set<String> foundResourceIds = new LinkedHashSet<>();
                    long totalHitCount = response.getHits().getTotalHits().value;
                    for (SearchHit hit : response.getHits().getHits()) {
                        Map<String, Object> docSource = hit.getSourceAsMap();
                        if (docSource == null) {
                            continue;
                        }
                        Object idValue = docSource.get(TrainedModelConfig.MODEL_ID.getPreferredName());
                        if (idValue instanceof String) {
                            foundResourceIds.add(idValue.toString());
                        }
                    }
                    ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, request.isAllowNoResources());
                    requiredMatches.filterMatchedIds(foundResourceIds);
                    if (requiredMatches.hasUnmatchedIds()) {
                        idsListener.onFailure(ExceptionsHelper.missingTrainedModel(requiredMatches.unmatchedIdsString()));
                    } else {
                        idsListener.onResponse(Tuple.tuple(totalHitCount, foundResourceIds));
                    }
                },
                idsListener::onFailure
            ),
            client::search);

    }

    private QueryBuilder buildQuery(String[] tokens, String resourceIdField) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(InferenceIndexConstants.DOC_TYPE.getPreferredName(), TrainedModelConfig.NAME));

        if (Strings.isAllOrWildcard(tokens)) {
            return boolQuery;
        }
        // If the resourceId is not _all or *, we should see if it is a comma delimited string with wild-cards
        // e.g. id1,id2*,id3
        BoolQueryBuilder shouldQueries = new BoolQueryBuilder();
        List<String> terms = new ArrayList<>();
        for (String token : tokens) {
            if (Regex.isSimpleMatchPattern(token)) {
                shouldQueries.should(QueryBuilders.wildcardQuery(resourceIdField, token));
            } else {
                terms.add(token);
            }
        }
        if (terms.isEmpty() == false) {
            shouldQueries.should(QueryBuilders.termsQuery(resourceIdField, terms));
        }

        if (shouldQueries.should().isEmpty() == false) {
            boolQuery.filter(shouldQueries);
        }
        return boolQuery;
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
