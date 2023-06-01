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
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction.Response.Stats;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.dataframe.stats.AnalysisStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.Fields;
import org.elasticsearch.xpack.core.ml.dataframe.stats.classification.ClassificationStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection.OutlierDetectionStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.regression.RegressionStats;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.StoredProgress;
import org.elasticsearch.xpack.ml.dataframe.stats.ProgressTracker;
import org.elasticsearch.xpack.ml.dataframe.stats.StatsHolder;
import org.elasticsearch.xpack.ml.utils.persistence.MlParserUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportGetDataFrameAnalyticsStatsAction extends TransportTasksAction<
    DataFrameAnalyticsTask,
    GetDataFrameAnalyticsStatsAction.Request,
    GetDataFrameAnalyticsStatsAction.Response,
    QueryPage<Stats>> {

    private static final Logger logger = LogManager.getLogger(TransportGetDataFrameAnalyticsStatsAction.class);

    private final Client client;

    @Inject
    public TransportGetDataFrameAnalyticsStatsAction(
        TransportService transportService,
        ClusterService clusterService,
        Client client,
        ActionFilters actionFilters
    ) {
        super(
            GetDataFrameAnalyticsStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            GetDataFrameAnalyticsStatsAction.Request::new,
            GetDataFrameAnalyticsStatsAction.Response::new,
            in -> new QueryPage<>(in, GetDataFrameAnalyticsStatsAction.Response.Stats::new),
            ThreadPool.Names.MANAGEMENT
        );
        this.client = client;
    }

    @Override
    protected GetDataFrameAnalyticsStatsAction.Response newResponse(
        GetDataFrameAnalyticsStatsAction.Request request,
        List<QueryPage<Stats>> tasks,
        List<TaskOperationFailure> taskFailures,
        List<FailedNodeException> nodeFailures
    ) {
        List<Stats> stats = new ArrayList<>();
        for (QueryPage<Stats> task : tasks) {
            stats.addAll(task.results());
        }
        stats.sort(Comparator.comparing(Stats::getId));
        return new GetDataFrameAnalyticsStatsAction.Response(
            taskFailures,
            nodeFailures,
            new QueryPage<>(stats, stats.size(), GetDataFrameAnalyticsAction.Response.RESULTS_FIELD)
        );
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        GetDataFrameAnalyticsStatsAction.Request request,
        DataFrameAnalyticsTask task,
        ActionListener<QueryPage<Stats>> listener
    ) {
        logger.debug("Get stats for running task [{}]", task.getParams().getId());

        ActionListener<Void> updateProgressListener = ActionListener.wrap(aVoid -> {
            StatsHolder statsHolder = task.getStatsHolder();
            if (statsHolder == null) {
                // The task has just been assigned and has not been initialized with its stats holder yet.
                // We return empty result here so that we treat it as a stopped task and return its stored stats.
                listener.onResponse(new QueryPage<>(Collections.emptyList(), 0, GetDataFrameAnalyticsAction.Response.RESULTS_FIELD));
                return;
            }
            Stats stats = buildStats(
                task.getParams().getId(),
                statsHolder.getProgressTracker().report(),
                statsHolder.getDataCountsTracker().report(),
                statsHolder.getMemoryUsage(),
                statsHolder.getAnalysisStats()
            );
            listener.onResponse(new QueryPage<>(Collections.singletonList(stats), 1, GetDataFrameAnalyticsAction.Response.RESULTS_FIELD));
        }, listener::onFailure);

        // We must update the progress of the reindexing task as it might be stale
        task.updateTaskProgress(updateProgressListener);
    }

    @Override
    protected void doExecute(
        Task task,
        GetDataFrameAnalyticsStatsAction.Request request,
        ActionListener<GetDataFrameAnalyticsStatsAction.Response> listener
    ) {
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        logger.debug("Get stats for data frame analytics [{}]", request.getId());

        ActionListener<GetDataFrameAnalyticsAction.Response> getResponseListener = ActionListener.wrap(getResponse -> {
            List<String> expandedIds = getResponse.getResources()
                .results()
                .stream()
                .map(DataFrameAnalyticsConfig::getId)
                .collect(Collectors.toList());
            request.setExpandedIds(expandedIds);
            ActionListener<GetDataFrameAnalyticsStatsAction.Response> runningTasksStatsListener = ActionListener.wrap(
                runningTasksStatsResponse -> gatherStatsForStoppedTasks(
                    getResponse.getResources().results(),
                    runningTasksStatsResponse,
                    parentTaskId,
                    ActionListener.wrap(finalResponse -> {

                        // While finalResponse has all the stats objects we need, we should report the count
                        // from the get response
                        QueryPage<Stats> finalStats = new QueryPage<>(
                            finalResponse.getResponse().results(),
                            getResponse.getResources().count(),
                            GetDataFrameAnalyticsAction.Response.RESULTS_FIELD
                        );
                        listener.onResponse(new GetDataFrameAnalyticsStatsAction.Response(finalStats));
                    }, listener::onFailure)
                ),
                listener::onFailure
            );
            super.doExecute(task, request, runningTasksStatsListener);
        }, listener::onFailure);

        GetDataFrameAnalyticsAction.Request getRequest = new GetDataFrameAnalyticsAction.Request();
        getRequest.setResourceId(request.getId());
        getRequest.setAllowNoResources(request.isAllowNoMatch());
        getRequest.setPageParams(request.getPageParams());
        getRequest.setParentTask(parentTaskId);
        executeAsyncWithOrigin(client, ML_ORIGIN, GetDataFrameAnalyticsAction.INSTANCE, getRequest, getResponseListener);
    }

    void gatherStatsForStoppedTasks(
        List<DataFrameAnalyticsConfig> configs,
        GetDataFrameAnalyticsStatsAction.Response runningTasksResponse,
        TaskId parentTaskId,
        ActionListener<GetDataFrameAnalyticsStatsAction.Response> listener
    ) {
        List<DataFrameAnalyticsConfig> stoppedConfigs = determineStoppedConfigs(configs, runningTasksResponse.getResponse().results());
        if (stoppedConfigs.isEmpty()) {
            listener.onResponse(runningTasksResponse);
            return;
        }

        AtomicInteger counter = new AtomicInteger(stoppedConfigs.size());
        AtomicArray<Stats> jobStats = new AtomicArray<>(stoppedConfigs.size());
        AtomicReference<Exception> searchException = new AtomicReference<>();
        for (int i = 0; i < stoppedConfigs.size(); i++) {
            final int slot = i;
            DataFrameAnalyticsConfig config = stoppedConfigs.get(i);
            searchStats(config, parentTaskId, ActionListener.wrap(stats -> {
                jobStats.set(slot, stats);
                if (counter.decrementAndGet() == 0) {
                    if (searchException.get() != null) {
                        listener.onFailure(searchException.get());
                        return;
                    }
                    List<Stats> allTasksStats = new ArrayList<>(runningTasksResponse.getResponse().results());
                    allTasksStats.addAll(jobStats.asList());
                    allTasksStats.sort(Comparator.comparing(Stats::getId));
                    listener.onResponse(
                        new GetDataFrameAnalyticsStatsAction.Response(
                            new QueryPage<>(allTasksStats, allTasksStats.size(), GetDataFrameAnalyticsAction.Response.RESULTS_FIELD)
                        )
                    );
                }
            }, e -> {
                // take the first error
                searchException.compareAndSet(null, e);
                if (counter.decrementAndGet() == 0) {
                    listener.onFailure(e);
                }
            }));
        }
    }

    static List<DataFrameAnalyticsConfig> determineStoppedConfigs(List<DataFrameAnalyticsConfig> configs, List<Stats> runningTasksStats) {
        Set<String> startedTasksIds = runningTasksStats.stream().map(Stats::getId).collect(Collectors.toSet());
        return configs.stream().filter(config -> startedTasksIds.contains(config.getId()) == false).collect(Collectors.toList());
    }

    private void searchStats(DataFrameAnalyticsConfig config, TaskId parentTaskId, ActionListener<Stats> listener) {
        logger.debug("[{}] Gathering stats for stopped task", config.getId());

        RetrievedStatsHolder retrievedStatsHolder = new RetrievedStatsHolder(
            ProgressTracker.fromZeroes(config.getAnalysis().getProgressPhases(), config.getAnalysis().supportsInference()).report()
        );

        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.add(buildStoredProgressSearch(config.getId()));
        multiSearchRequest.add(buildStatsDocSearch(config.getId(), DataCounts.TYPE_VALUE));
        multiSearchRequest.add(buildStatsDocSearch(config.getId(), MemoryUsage.TYPE_VALUE));
        multiSearchRequest.add(buildStatsDocSearch(config.getId(), OutlierDetectionStats.TYPE_VALUE));
        multiSearchRequest.add(buildStatsDocSearch(config.getId(), ClassificationStats.TYPE_VALUE));
        multiSearchRequest.add(buildStatsDocSearch(config.getId(), RegressionStats.TYPE_VALUE));
        multiSearchRequest.setParentTask(parentTaskId);

        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            MultiSearchAction.INSTANCE,
            multiSearchRequest,
            ActionListener.wrap(multiSearchResponse -> {
                MultiSearchResponse.Item[] itemResponses = multiSearchResponse.getResponses();
                for (int i = 0; i < itemResponses.length; ++i) {
                    MultiSearchResponse.Item itemResponse = itemResponses[i];
                    if (itemResponse.isFailure()) {
                        SearchRequest itemRequest = multiSearchRequest.requests().get(i);
                        logger.error(
                            () -> format(
                                "[%s] Item failure encountered during multi search for request [indices=%s, source=%s]: %s",
                                config.getId(),
                                itemRequest.indices(),
                                itemRequest.source(),
                                itemResponse.getFailureMessage()
                            ),
                            itemResponse.getFailure()
                        );
                        listener.onFailure(ExceptionsHelper.serverError(itemResponse.getFailureMessage(), itemResponse.getFailure()));
                        return;
                    } else {
                        SearchHit[] hits = itemResponse.getResponse().getHits().getHits();
                        if (hits.length == 0) {
                            // Not found
                        } else if (hits.length == 1) {
                            parseHit(hits[0], config.getId(), retrievedStatsHolder);
                        } else {
                            throw ExceptionsHelper.serverError("Found [" + hits.length + "] hits when just one was requested");
                        }
                    }
                }
                listener.onResponse(
                    buildStats(
                        config.getId(),
                        retrievedStatsHolder.progress.get(),
                        retrievedStatsHolder.dataCounts,
                        retrievedStatsHolder.memoryUsage,
                        retrievedStatsHolder.analysisStats
                    )
                );
            }, e -> listener.onFailure(ExceptionsHelper.serverError("Error searching for stats", e)))
        );
    }

    private static SearchRequest buildStoredProgressSearch(String configId) {
        SearchRequest searchRequest = new SearchRequest(AnomalyDetectorsIndex.jobStateIndexPattern());
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        searchRequest.source().size(1);
        searchRequest.source().query(QueryBuilders.idsQuery().addIds(StoredProgress.documentId(configId)));
        return searchRequest;
    }

    private static SearchRequest buildStatsDocSearch(String configId, String statsType) {
        SearchRequest searchRequest = new SearchRequest(MlStatsIndex.indexPattern());
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        searchRequest.source().size(1);
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(Fields.JOB_ID.getPreferredName(), configId))
            .filter(QueryBuilders.termQuery(Fields.TYPE.getPreferredName(), statsType));
        searchRequest.source().query(query);
        searchRequest.source()
            .sort(
                SortBuilders.fieldSort(Fields.TIMESTAMP.getPreferredName())
                    .order(SortOrder.DESC)
                    // We need this for the search not to fail when there are no mappings yet in the index
                    .unmappedType("long")
            );
        return searchRequest;
    }

    private static void parseHit(SearchHit hit, String configId, RetrievedStatsHolder retrievedStatsHolder) {
        String hitId = hit.getId();
        if (StoredProgress.documentId(configId).equals(hitId)) {
            retrievedStatsHolder.progress = MlParserUtils.parse(hit, StoredProgress.PARSER);
        } else if (DataCounts.documentId(configId).equals(hitId)) {
            retrievedStatsHolder.dataCounts = MlParserUtils.parse(hit, DataCounts.LENIENT_PARSER);
        } else if (hitId.startsWith(MemoryUsage.documentIdPrefix(configId))) {
            retrievedStatsHolder.memoryUsage = MlParserUtils.parse(hit, MemoryUsage.LENIENT_PARSER);
        } else if (hitId.startsWith(OutlierDetectionStats.documentIdPrefix(configId))) {
            retrievedStatsHolder.analysisStats = MlParserUtils.parse(hit, OutlierDetectionStats.LENIENT_PARSER);
        } else if (hitId.startsWith(ClassificationStats.documentIdPrefix(configId))) {
            retrievedStatsHolder.analysisStats = MlParserUtils.parse(hit, ClassificationStats.LENIENT_PARSER);
        } else if (hitId.startsWith(RegressionStats.documentIdPrefix(configId))) {
            retrievedStatsHolder.analysisStats = MlParserUtils.parse(hit, RegressionStats.LENIENT_PARSER);
        } else {
            throw ExceptionsHelper.serverError("unexpected doc id [" + hitId + "]");
        }
    }

    private GetDataFrameAnalyticsStatsAction.Response.Stats buildStats(
        String concreteAnalyticsId,
        List<PhaseProgress> progress,
        DataCounts dataCounts,
        MemoryUsage memoryUsage,
        AnalysisStats analysisStats
    ) {
        ClusterState clusterState = clusterService.state();
        PersistentTasksCustomMetadata tasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata.PersistentTask<?> analyticsTask = MlTasks.getDataFrameAnalyticsTask(concreteAnalyticsId, tasks);
        DataFrameAnalyticsState analyticsState = MlTasks.getDataFrameAnalyticsState(concreteAnalyticsId, tasks);
        String failureReason = null;
        if (analyticsState == DataFrameAnalyticsState.FAILED) {
            DataFrameAnalyticsTaskState taskState = (DataFrameAnalyticsTaskState) analyticsTask.getState();
            failureReason = taskState.getReason();
        }
        DiscoveryNode node = null;
        String assignmentExplanation = null;
        if (analyticsTask != null) {
            node = analyticsTask.getExecutorNode() != null ? clusterState.nodes().get(analyticsTask.getExecutorNode()) : null;
            assignmentExplanation = analyticsTask.getAssignment().getExplanation();
        }
        return new GetDataFrameAnalyticsStatsAction.Response.Stats(
            concreteAnalyticsId,
            analyticsState,
            failureReason,
            progress,
            dataCounts,
            memoryUsage,
            analysisStats,
            node,
            assignmentExplanation
        );
    }

    private static class RetrievedStatsHolder {

        private volatile StoredProgress progress;
        private volatile DataCounts dataCounts;
        private volatile MemoryUsage memoryUsage;
        private volatile AnalysisStats analysisStats;

        private RetrievedStatsHolder(List<PhaseProgress> defaultProgress) {
            progress = new StoredProgress(defaultProgress);
        }
    }
}
