/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
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
import org.elasticsearch.xpack.core.ml.dataframe.stats.MemoryUsage;
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
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportGetDataFrameAnalyticsStatsAction
    extends TransportTasksAction<DataFrameAnalyticsTask, GetDataFrameAnalyticsStatsAction.Request,
        GetDataFrameAnalyticsStatsAction.Response, QueryPage<Stats>> {

    private static final Logger logger = LogManager.getLogger(TransportGetDataFrameAnalyticsStatsAction.class);

    private final Client client;

    @Inject
    public TransportGetDataFrameAnalyticsStatsAction(TransportService transportService, ClusterService clusterService, Client client,
                                                     ActionFilters actionFilters) {
        super(GetDataFrameAnalyticsStatsAction.NAME, clusterService, transportService, actionFilters,
            GetDataFrameAnalyticsStatsAction.Request::new, GetDataFrameAnalyticsStatsAction.Response::new,
            in -> new QueryPage<>(in, GetDataFrameAnalyticsStatsAction.Response.Stats::new), ThreadPool.Names.MANAGEMENT);
        this.client = client;
    }

    @Override
    protected GetDataFrameAnalyticsStatsAction.Response newResponse(GetDataFrameAnalyticsStatsAction.Request request,
                                                                    List<QueryPage<Stats>> tasks,
                                                                    List<TaskOperationFailure> taskFailures,
                                                                    List<FailedNodeException> nodeFailures) {
        List<Stats> stats = new ArrayList<>();
        for (QueryPage<Stats> task : tasks) {
            stats.addAll(task.results());
        }
        Collections.sort(stats, Comparator.comparing(Stats::getId));
        return new GetDataFrameAnalyticsStatsAction.Response(taskFailures, nodeFailures, new QueryPage<>(stats, stats.size(),
            GetDataFrameAnalyticsAction.Response.RESULTS_FIELD));
    }

    @Override
    protected void taskOperation(GetDataFrameAnalyticsStatsAction.Request request, DataFrameAnalyticsTask task,
                                 ActionListener<QueryPage<Stats>> listener) {
        logger.debug("Get stats for running task [{}]", task.getParams().getId());

        ActionListener<StatsHolder> statsHolderListener = ActionListener.wrap(
            statsHolder -> {
                Stats stats = buildStats(
                    task.getParams().getId(),
                    statsHolder.getProgressTracker().report(),
                    statsHolder.getMemoryUsage()
                );
                listener.onResponse(new QueryPage<>(Collections.singletonList(stats), 1,
                    GetDataFrameAnalyticsAction.Response.RESULTS_FIELD));
            }, listener::onFailure
        );

        ActionListener<Void> reindexingProgressListener = ActionListener.wrap(
            aVoid -> statsHolderListener.onResponse(task.getStatsHolder()),
            listener::onFailure
        );

        task.updateReindexTaskProgress(reindexingProgressListener);
    }

    @Override
    protected void doExecute(Task task, GetDataFrameAnalyticsStatsAction.Request request,
                             ActionListener<GetDataFrameAnalyticsStatsAction.Response> listener) {
        logger.debug("Get stats for data frame analytics [{}]", request.getId());

        ActionListener<GetDataFrameAnalyticsAction.Response> getResponseListener = ActionListener.wrap(
            getResponse -> {
                List<String> expandedIds = getResponse.getResources().results().stream().map(DataFrameAnalyticsConfig::getId)
                    .collect(Collectors.toList());
                request.setExpandedIds(expandedIds);
                ActionListener<GetDataFrameAnalyticsStatsAction.Response> runningTasksStatsListener = ActionListener.wrap(
                    runningTasksStatsResponse -> gatherStatsForStoppedTasks(request.getExpandedIds(), runningTasksStatsResponse,
                        ActionListener.wrap(
                            finalResponse -> {
                                // While finalResponse has all the stats objects we need, we should report the count
                                // from the get response
                                QueryPage<Stats> finalStats = new QueryPage<>(finalResponse.getResponse().results(),
                                    getResponse.getResources().count(), GetDataFrameAnalyticsAction.Response.RESULTS_FIELD);
                                listener.onResponse(new GetDataFrameAnalyticsStatsAction.Response(finalStats));
                            },
                            listener::onFailure)),
                    listener::onFailure
                );
                super.doExecute(task, request, runningTasksStatsListener);
            },
            listener::onFailure
        );

        GetDataFrameAnalyticsAction.Request getRequest = new GetDataFrameAnalyticsAction.Request();
        getRequest.setResourceId(request.getId());
        getRequest.setAllowNoResources(request.isAllowNoMatch());
        getRequest.setPageParams(request.getPageParams());
        executeAsyncWithOrigin(client, ML_ORIGIN, GetDataFrameAnalyticsAction.INSTANCE, getRequest, getResponseListener);
    }

    void gatherStatsForStoppedTasks(List<String> expandedIds, GetDataFrameAnalyticsStatsAction.Response runningTasksResponse,
                                    ActionListener<GetDataFrameAnalyticsStatsAction.Response> listener) {
        List<String> stoppedTasksIds = determineStoppedTasksIds(expandedIds, runningTasksResponse.getResponse().results());
        if (stoppedTasksIds.isEmpty()) {
            listener.onResponse(runningTasksResponse);
            return;
        }

        AtomicInteger counter = new AtomicInteger(stoppedTasksIds.size());
        AtomicArray<Stats> jobStats = new AtomicArray<>(stoppedTasksIds.size());
        for (int i = 0; i < stoppedTasksIds.size(); i++) {
            final int slot = i;
            String jobId = stoppedTasksIds.get(i);
            searchStats(jobId, ActionListener.wrap(
                stats -> {
                    jobStats.set(slot, stats);
                    if (counter.decrementAndGet() == 0) {
                        List<Stats> allTasksStats = new ArrayList<>(runningTasksResponse.getResponse().results());
                        allTasksStats.addAll(jobStats.asList());
                        Collections.sort(allTasksStats, Comparator.comparing(Stats::getId));
                        listener.onResponse(new GetDataFrameAnalyticsStatsAction.Response(new QueryPage<>(
                            allTasksStats, allTasksStats.size(), GetDataFrameAnalyticsAction.Response.RESULTS_FIELD)));
                    }
                },
                listener::onFailure)
            );
        }
    }

    static List<String> determineStoppedTasksIds(List<String> expandedIds, List<Stats> runningTasksStats) {
        Set<String> startedTasksIds = runningTasksStats.stream().map(Stats::getId).collect(Collectors.toSet());
        return expandedIds.stream().filter(id -> startedTasksIds.contains(id) == false).collect(Collectors.toList());
    }

    private void searchStats(String configId, ActionListener<Stats> listener) {
        RetrievedStatsHolder retrievedStatsHolder = new RetrievedStatsHolder();

        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.add(buildStoredProgressSearch(configId));
        multiSearchRequest.add(buildMemoryUsageSearch(configId));

        executeAsyncWithOrigin(client, ML_ORIGIN, MultiSearchAction.INSTANCE, multiSearchRequest, ActionListener.wrap(
            multiSearchResponse -> {
                for (MultiSearchResponse.Item itemResponse : multiSearchResponse.getResponses()) {
                    if (itemResponse.isFailure()) {
                        listener.onFailure(ExceptionsHelper.serverError(itemResponse.getFailureMessage(), itemResponse.getFailure()));
                        return;
                    } else {
                        SearchHit[] hits = itemResponse.getResponse().getHits().getHits();
                        if (hits.length == 0) {
                            // Not found
                        } else if (hits.length == 1) {
                            parseHit(hits[0], configId, retrievedStatsHolder);
                        } else {
                            throw ExceptionsHelper.serverError("Found [" + hits.length + "] hits when just one was requested");
                        }
                    }
                }
                listener.onResponse(buildStats(configId,
                    retrievedStatsHolder.progress.get(),
                    retrievedStatsHolder.memoryUsage
                ));
            },
            e -> listener.onFailure(ExceptionsHelper.serverError("Error searching for stats", e))
        ));
    }

    private static SearchRequest buildStoredProgressSearch(String configId) {
        SearchRequest searchRequest = new SearchRequest(AnomalyDetectorsIndex.jobStateIndexPattern());
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        searchRequest.source().size(1);
        searchRequest.source().query(QueryBuilders.idsQuery().addIds(StoredProgress.documentId(configId)));
        return searchRequest;
    }

    private static SearchRequest buildMemoryUsageSearch(String configId) {
        SearchRequest searchRequest = new SearchRequest(MlStatsIndex.indexPattern());
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        searchRequest.source().size(1);
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(MemoryUsage.JOB_ID.getPreferredName(), configId))
            .filter(QueryBuilders.termQuery(MemoryUsage.TYPE.getPreferredName(), MemoryUsage.TYPE_VALUE));
        searchRequest.source().query(query);
        searchRequest.source().sort(SortBuilders.fieldSort(MemoryUsage.TIMESTAMP.getPreferredName()).order(SortOrder.DESC)
            // We need this for the search not to fail when there are no mappings yet in the index
            .unmappedType("long"));
        searchRequest.source().sort(MemoryUsage.TIMESTAMP.getPreferredName(), SortOrder.DESC);
        return searchRequest;
    }

    private static void parseHit(SearchHit hit, String configId, RetrievedStatsHolder retrievedStatsHolder) {
        String hitId = hit.getId();
        if (StoredProgress.documentId(configId).equals(hitId)) {
            retrievedStatsHolder.progress = MlParserUtils.parse(hit, StoredProgress.PARSER);
        } else if (hitId.startsWith(MemoryUsage.documentIdPrefix(configId))) {
            retrievedStatsHolder.memoryUsage = MlParserUtils.parse(hit, MemoryUsage.LENIENT_PARSER);
        } else {
            throw ExceptionsHelper.serverError("unexpected doc id [" + hitId + "]");
        }
    }

    private GetDataFrameAnalyticsStatsAction.Response.Stats buildStats(String concreteAnalyticsId,
                                                                       List<PhaseProgress> progress,
                                                                       MemoryUsage memoryUsage) {
        ClusterState clusterState = clusterService.state();
        PersistentTasksCustomMetaData tasks = clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData.PersistentTask<?> analyticsTask = MlTasks.getDataFrameAnalyticsTask(concreteAnalyticsId, tasks);
        DataFrameAnalyticsState analyticsState = MlTasks.getDataFrameAnalyticsState(concreteAnalyticsId, tasks);
        String failureReason = null;
        if (analyticsState == DataFrameAnalyticsState.FAILED) {
            DataFrameAnalyticsTaskState taskState = (DataFrameAnalyticsTaskState) analyticsTask.getState();
            failureReason = taskState.getReason();
        }
        DiscoveryNode node = null;
        String assignmentExplanation = null;
        if (analyticsTask != null) {
            node = clusterState.nodes().get(analyticsTask.getExecutorNode());
            assignmentExplanation = analyticsTask.getAssignment().getExplanation();
        }
        return new GetDataFrameAnalyticsStatsAction.Response.Stats(
            concreteAnalyticsId,
            analyticsState,
            failureReason,
            progress,
            memoryUsage,
            node,
            assignmentExplanation
        );
    }

    private static class RetrievedStatsHolder {

        private volatile StoredProgress progress = new StoredProgress(new ProgressTracker().report());
        private volatile MemoryUsage memoryUsage;
    }
}
