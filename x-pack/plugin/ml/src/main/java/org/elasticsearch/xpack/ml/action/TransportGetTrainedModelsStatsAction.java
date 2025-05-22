/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.ModelAliasMetadata;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelSizeStats;
import org.elasticsearch.xpack.core.ml.utils.TransportVersionUtils;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelDefinitionDoc;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.utils.InferenceProcessorInfoExtractor.pipelineIdsByResource;

public class TransportGetTrainedModelsStatsAction extends TransportAction<
    GetTrainedModelsStatsAction.Request,
    GetTrainedModelsStatsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetTrainedModelsStatsAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final TrainedModelProvider trainedModelProvider;
    private final Executor executor;

    @Inject
    public TransportGetTrainedModelsStatsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        TrainedModelProvider trainedModelProvider,
        Client client
    ) {
        this(
            transportService,
            actionFilters,
            clusterService,
            trainedModelProvider,
            client,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
        );
    }

    private TransportGetTrainedModelsStatsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        TrainedModelProvider trainedModelProvider,
        Client client,
        Executor executor
    ) {
        super(GetTrainedModelsStatsAction.NAME, actionFilters, transportService.getTaskManager(), executor);
        this.client = client;
        this.clusterService = clusterService;
        this.trainedModelProvider = trainedModelProvider;
        this.executor = executor;
    }

    @Override
    protected void doExecute(
        Task task,
        GetTrainedModelsStatsAction.Request request,
        ActionListener<GetTrainedModelsStatsAction.Response> listener
    ) {
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        final ModelAliasMetadata modelAliasMetadata = ModelAliasMetadata.fromState(clusterService.state());
        final TrainedModelAssignmentMetadata assignmentMetadata = TrainedModelAssignmentMetadata.fromState(clusterService.state());
        final Set<String> matchedDeploymentIds = matchedDeploymentIds(request.getResourceId(), assignmentMetadata);

        GetTrainedModelsStatsAction.Response.Builder responseBuilder = new GetTrainedModelsStatsAction.Response.Builder();

        SubscribableListener

            .<Tuple<Long, Map<String, Set<String>>>>newForked(l -> {
                // When the request resource is a deployment find the model used in that deployment for the model stats
                final var idExpression = addModelsUsedInMatchingDeployments(request.getResourceId(), assignmentMetadata);

                logger.debug("Expanded models/deployment Ids request [{}]", idExpression);

                // the request id may contain deployment ids
                // It is not an error if these don't match a model id but
                // they need to be included in case the deployment id is also
                // a model id. Hence, the `matchedDeploymentIds` parameter
                trainedModelProvider.expandIds(
                    idExpression,
                    request.isAllowNoResources(),
                    request.getPageParams(),
                    Collections.emptySet(),
                    modelAliasMetadata,
                    parentTaskId,
                    matchedDeploymentIds,
                    l
                );
            })
            .andThenAccept(tuple -> responseBuilder.setExpandedModelIdsWithAliases(tuple.v2()).setTotalModelCount(tuple.v1()))

            .<NodesStatsResponse>andThen(
                l -> executeAsyncWithOrigin(
                    client,
                    ML_ORIGIN,
                    TransportNodesStatsAction.TYPE,
                    nodeStatsRequest(clusterService.state(), parentTaskId),
                    l
                )
            )
            .<List<InferenceStats>>andThen(executor, null, (l, nodesStatsResponse) -> {
                // find all pipelines whether using the model id, alias or deployment id.
                Set<String> allPossiblePipelineReferences = responseBuilder.getExpandedModelIdsWithAliases()
                    .entrySet()
                    .stream()
                    .flatMap(entry -> Stream.concat(entry.getValue().stream(), Stream.of(entry.getKey())))
                    .collect(Collectors.toSet());
                allPossiblePipelineReferences.addAll(matchedDeploymentIds);

                Map<String, Set<String>> pipelineIdsByResource = pipelineIdsByResource(
                    clusterService.state(),
                    allPossiblePipelineReferences
                );
                Map<String, IngestStats> modelIdIngestStats = inferenceIngestStatsByModelId(
                    nodesStatsResponse,
                    modelAliasMetadata,
                    pipelineIdsByResource
                );
                responseBuilder.setIngestStatsByModelId(modelIdIngestStats);
                trainedModelProvider.getInferenceStats(
                    responseBuilder.getExpandedModelIdsWithAliases().keySet().toArray(new String[0]),
                    parentTaskId,
                    l
                );
            })
            .andThenAccept(
                // inference stats are per model and are only persisted for boosted tree models
                inferenceStats -> responseBuilder.setInferenceStatsByModelId(
                    inferenceStats.stream().collect(Collectors.toMap(InferenceStats::getModelId, Function.identity()))
                )
            )

            .<GetDeploymentStatsAction.Response>andThen(
                executor,
                null,
                (l, ignored) -> getDeploymentStats(client, request.getResourceId(), parentTaskId, assignmentMetadata, l)
            )
            .andThenApply(deploymentStats -> {
                // deployment stats for each matching deployment not necessarily for all models
                responseBuilder.setDeploymentStatsByDeploymentId(
                    deploymentStats.getStats()
                        .results()
                        .stream()
                        .collect(Collectors.toMap(AssignmentStats::getDeploymentId, Function.identity()))
                );
                return deploymentStats.getStats().results().stream().mapToInt(AssignmentStats::getNumberOfAllocations).sum();
            })

            .<Map<String, TrainedModelSizeStats>>andThen(
                executor,
                null,
                (l, numberOfAllocations) -> modelSizeStats(
                    responseBuilder.getExpandedModelIdsWithAliases(),
                    request.isAllowNoResources(),
                    parentTaskId,
                    l,
                    numberOfAllocations
                )
            )
            .andThenAccept(responseBuilder::setModelSizeStatsByModelId)

            .andThenApply(
                ignored -> responseBuilder.build(
                    modelToDeployments(responseBuilder.getExpandedModelIdsWithAliases().keySet(), assignmentMetadata)
                )
            )

            .addListener(listener, executor, null);
    }

    static String addModelsUsedInMatchingDeployments(String idExpression, TrainedModelAssignmentMetadata assignmentMetadata) {
        if (Strings.isAllOrWildcard(idExpression)) {
            return idExpression;
        } else {
            var tokens = new HashSet<>(Arrays.asList(ExpandedIdsMatcher.tokenizeExpression(idExpression)));
            var modelsUsedByMatchingDeployments = modelsUsedByMatchingDeploymentId(idExpression, assignmentMetadata);
            tokens.addAll(modelsUsedByMatchingDeployments);
            return String.join(",", tokens);
        }
    }

    static Map<String, Set<String>> modelToDeployments(Set<String> modelIds, TrainedModelAssignmentMetadata assignments) {
        var modelToDeploymentMap = new HashMap<String, Set<String>>();
        for (var assignment : assignments.allAssignments().values()) {
            if (modelIds.contains(assignment.getModelId())) {
                modelToDeploymentMap.computeIfAbsent(assignment.getModelId(), k -> new HashSet<>()).add(assignment.getDeploymentId());
            }
        }
        return modelToDeploymentMap;
    }

    static Set<String> matchedDeploymentIds(String resourceId, TrainedModelAssignmentMetadata assignments) {
        var deploymentIds = new HashSet<String>();
        var matcher = new ExpandedIdsMatcher.SimpleIdsMatcher(resourceId);
        for (var assignment : assignments.allAssignments().values()) {
            if (matcher.idMatches(assignment.getDeploymentId())) {
                deploymentIds.add(assignment.getDeploymentId());
            }
        }
        return deploymentIds;
    }

    static Set<String> modelsUsedByMatchingDeploymentId(String resourceId, TrainedModelAssignmentMetadata assignments) {
        var modelIds = new HashSet<String>();
        var matcher = new ExpandedIdsMatcher.SimpleIdsMatcher(resourceId);
        for (var assignment : assignments.allAssignments().values()) {
            if (matcher.idMatches(assignment.getDeploymentId())) {
                modelIds.add(assignment.getModelId());
            }
        }
        return modelIds;
    }

    static void getDeploymentStats(
        Client client,
        String resourceId,
        TaskId parentTaskId,
        TrainedModelAssignmentMetadata assignments,
        ActionListener<GetDeploymentStatsAction.Response> deploymentStatsListener
    ) {
        // include all matched deployments and models
        var matcher = new ExpandedIdsMatcher.SimpleIdsMatcher(resourceId);
        var matchedDeployments = new HashSet<String>();
        for (var assignment : assignments.allAssignments().values()) {
            if (matcher.idMatches(assignment.getDeploymentId())) {
                matchedDeployments.add(assignment.getDeploymentId());
            } else if (matcher.idMatches(assignment.getModelId())) {
                matchedDeployments.add(assignment.getDeploymentId());
            }
        }
        String deployments = String.join(",", matchedDeployments);

        logger.debug("Fetching stats for deployments [{}]", deployments);

        GetDeploymentStatsAction.Request getDeploymentStatsRequest = new GetDeploymentStatsAction.Request(deployments);
        getDeploymentStatsRequest.setParentTask(parentTaskId);
        executeAsyncWithOrigin(client, ML_ORIGIN, GetDeploymentStatsAction.INSTANCE, getDeploymentStatsRequest, deploymentStatsListener);
    }

    private void modelSizeStats(
        Map<String, Set<String>> expandedIdsWithAliases,
        boolean allowNoResources,
        TaskId parentTaskId,
        ActionListener<Map<String, TrainedModelSizeStats>> listener,
        int numberOfAllocations
    ) {
        ActionListener<List<TrainedModelConfig>> modelsListener = ActionListener.wrap(models -> {
            final List<String> pytorchModelIds = models.stream()
                .filter(m -> m.getModelType() == TrainedModelType.PYTORCH)
                .map(TrainedModelConfig::getModelId)
                .toList();
            definitionLengths(pytorchModelIds, parentTaskId, ActionListener.wrap(pytorchTotalDefinitionLengthsByModelId -> {
                Map<String, TrainedModelSizeStats> modelSizeStatsByModelId = new HashMap<>();
                for (TrainedModelConfig model : models) {
                    if (model.getModelType() == TrainedModelType.PYTORCH) {
                        long totalDefinitionLength = pytorchTotalDefinitionLengthsByModelId.getOrDefault(model.getModelId(), 0L);
                        // We ensure that in the mixed cluster state trained model stats uses the same values for memory estimation
                        // as the rebalancer.
                        boolean useNewMemoryFields = TrainedModelAssignment.useNewMemoryFields(
                            TransportVersionUtils.getMinTransportVersion(clusterService.state())
                        );
                        long estimatedMemoryUsageBytes = totalDefinitionLength > 0L
                            ? StartTrainedModelDeploymentAction.estimateMemoryUsageBytes(
                                model.getModelId(),
                                totalDefinitionLength,
                                useNewMemoryFields ? model.getPerDeploymentMemoryBytes() : 0,
                                useNewMemoryFields ? model.getPerAllocationMemoryBytes() : 0,
                                numberOfAllocations
                            )
                            : 0L;
                        modelSizeStatsByModelId.put(
                            model.getModelId(),
                            new TrainedModelSizeStats(totalDefinitionLength, estimatedMemoryUsageBytes)
                        );
                    } else {
                        modelSizeStatsByModelId.put(model.getModelId(), new TrainedModelSizeStats(model.getModelSize(), 0));
                    }
                }
                listener.onResponse(modelSizeStatsByModelId);
            }, listener::onFailure));
        }, listener::onFailure);

        trainedModelProvider.getTrainedModels(
            expandedIdsWithAliases,
            GetTrainedModelsAction.Includes.empty(),
            allowNoResources,
            parentTaskId,
            modelsListener
        );
    }

    private void definitionLengths(List<String> modelIds, TaskId parentTaskId, ActionListener<Map<String, Long>> listener) {
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(InferenceIndexConstants.DOC_TYPE.getPreferredName(), TrainedModelDefinitionDoc.NAME))
            .filter(QueryBuilders.termsQuery(TrainedModelConfig.MODEL_ID.getPreferredName(), modelIds))
            .filter(QueryBuilders.termQuery(TrainedModelDefinitionDoc.DOC_NUM.getPreferredName(), 0));
        SearchRequest searchRequest = client.prepareSearch(InferenceIndexConstants.INDEX_PATTERN)
            .setQuery(QueryBuilders.constantScoreQuery(query))
            .setFetchSource(false)
            .addDocValueField(TrainedModelConfig.MODEL_ID.getPreferredName())
            .addDocValueField(TrainedModelDefinitionDoc.TOTAL_DEFINITION_LENGTH.getPreferredName())
            // First find the latest index
            .addSort("_index", SortOrder.DESC)
            .request();
        searchRequest.setParentTask(parentTaskId);

        executeAsyncWithOrigin(client, ML_ORIGIN, TransportSearchAction.TYPE, searchRequest, ActionListener.wrap(searchResponse -> {
            Map<String, Long> totalDefinitionLengthByModelId = new HashMap<>();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                DocumentField modelIdField = hit.field(TrainedModelConfig.MODEL_ID.getPreferredName());
                if (modelIdField != null && modelIdField.getValue() instanceof String modelId) {
                    DocumentField totalDefinitionLengthField = hit.field(
                        TrainedModelDefinitionDoc.TOTAL_DEFINITION_LENGTH.getPreferredName()
                    );
                    if (totalDefinitionLengthField != null && totalDefinitionLengthField.getValue() instanceof Long totalDefinitionLength) {
                        totalDefinitionLengthByModelId.put(modelId, totalDefinitionLength);
                    }
                }
            }
            listener.onResponse(totalDefinitionLengthByModelId);
        }, listener::onFailure));
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

    static NodesStatsRequest nodeStatsRequest(ClusterState state, TaskId parentTaskId) {
        String[] ingestNodes = state.nodes().getIngestNodes().keySet().toArray(String[]::new);
        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(ingestNodes).clear()
            .addMetric(NodesStatsRequestParameters.Metric.INGEST);
        nodesStatsRequest.setIncludeShardsStats(false);
        nodesStatsRequest.setParentTask(parentTaskId);
        return nodesStatsRequest;
    }

    @FixForMultiProject // do not use default project
    static IngestStats ingestStatsForPipelineIds(NodeStats nodeStats, Set<String> pipelineIds) {
        IngestStats fullNodeStats = nodeStats.getIngestStats();
        Map<String, List<IngestStats.ProcessorStat>> filteredProcessorStats = new HashMap<>(
            fullNodeStats.processorStats().getOrDefault(ProjectId.DEFAULT, Map.of())
        );
        filteredProcessorStats.keySet().retainAll(pipelineIds);
        List<IngestStats.PipelineStat> filteredPipelineStats = fullNodeStats.pipelineStats()
            .stream()
            .filter(pipelineStat -> pipelineStat.projectId().equals(ProjectId.DEFAULT))
            .filter(pipelineStat -> pipelineIds.contains(pipelineStat.pipelineId()))
            .collect(Collectors.toList());
        IngestStatsAccumulator accumulator = new IngestStatsAccumulator();

        filteredPipelineStats.forEach(pipelineStat -> accumulator.inc(pipelineStat.stats()));

        return new IngestStats(accumulator.build(), filteredPipelineStats, Map.of(ProjectId.DEFAULT, filteredProcessorStats));
    }

    @FixForMultiProject // don't use default project
    private static IngestStats mergeStats(List<IngestStats> ingestStatsList) {

        Map<String, PipelineStatsAccumulator> pipelineStatsAcc = Maps.newLinkedHashMapWithExpectedSize(ingestStatsList.size());
        Map<String, Map<String, IngestStatsAccumulator>> processorStatsAcc = Maps.newLinkedHashMapWithExpectedSize(ingestStatsList.size());
        IngestStatsAccumulator totalStats = new IngestStatsAccumulator();
        ingestStatsList.forEach(ingestStats -> {

            ingestStats.pipelineStats()
                .forEach(
                    pipelineStat -> pipelineStatsAcc.computeIfAbsent(pipelineStat.pipelineId(), p -> new PipelineStatsAccumulator())
                        .inc(pipelineStat)
                );

            ingestStats.processorStats().getOrDefault(ProjectId.DEFAULT, Map.of()).forEach((pipelineId, processorStat) -> {
                Map<String, IngestStatsAccumulator> processorAcc = processorStatsAcc.computeIfAbsent(
                    pipelineId,
                    k -> new LinkedHashMap<>()
                );
                processorStat.forEach(
                    p -> processorAcc.computeIfAbsent(p.name(), k -> new IngestStatsAccumulator(p.type())).inc(p.stats())
                );
            });

            totalStats.inc(ingestStats.totalStats());
        });

        List<IngestStats.PipelineStat> pipelineStatList = new ArrayList<>(pipelineStatsAcc.size());
        pipelineStatsAcc.forEach(
            (pipelineId, accumulator) -> pipelineStatList.add(
                new IngestStats.PipelineStat(
                    Metadata.DEFAULT_PROJECT_ID,
                    pipelineId,
                    accumulator.buildStats(),
                    accumulator.buildByteStats()
                )
            )
        );

        Map<String, List<IngestStats.ProcessorStat>> processorStatList = Maps.newLinkedHashMapWithExpectedSize(processorStatsAcc.size());
        processorStatsAcc.forEach((pipelineId, accumulatorMap) -> {
            List<IngestStats.ProcessorStat> processorStats = new ArrayList<>(accumulatorMap.size());
            accumulatorMap.forEach(
                (processorName, acc) -> processorStats.add(new IngestStats.ProcessorStat(processorName, acc.type, acc.build()))
            );
            processorStatList.put(pipelineId, processorStats);
        });

        return new IngestStats(totalStats.build(), pipelineStatList, Map.of(ProjectId.DEFAULT, processorStatList));
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
            ingestCount.inc(s.ingestCount());
            ingestTimeInMillis.inc(s.ingestTimeInMillis());
            ingestCurrent.inc(s.ingestCurrent());
            ingestFailedCount.inc(s.ingestFailedCount());
        }

        IngestStats.Stats build() {
            IngestStats.Stats stats = new IngestStats.Stats(
                ingestCount.count(),
                ingestTimeInMillis.count(),
                ingestCurrent.count(),
                ingestFailedCount.count()
            );
            return stats.equals(IngestStats.Stats.IDENTITY) ? IngestStats.Stats.IDENTITY : stats;
        }
    }

    private static class PipelineStatsAccumulator {
        IngestStatsAccumulator ingestStatsAccumulator = new IngestStatsAccumulator();
        CounterMetric ingestBytesConsumed = new CounterMetric();
        CounterMetric ingestBytesProduced = new CounterMetric();

        void inc(IngestStats.PipelineStat s) {
            ingestStatsAccumulator.inc(s.stats());
            ingestBytesConsumed.inc(s.byteStats().bytesIngested());
            ingestBytesProduced.inc(s.byteStats().bytesProduced());
        }

        IngestStats.Stats buildStats() {
            return ingestStatsAccumulator.build();
        }

        IngestStats.ByteStats buildByteStats() {
            IngestStats.ByteStats byteStats = new IngestStats.ByteStats(ingestBytesConsumed.count(), ingestBytesProduced.count());
            return byteStats.equals(IngestStats.ByteStats.IDENTITY) ? IngestStats.ByteStats.IDENTITY : byteStats;
        }

    }

}
