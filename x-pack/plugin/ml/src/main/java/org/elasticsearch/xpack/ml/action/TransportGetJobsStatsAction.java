/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TransportGetJobsStatsAction extends TransportTasksAction<TransportOpenJobAction.JobTask, GetJobsStatsAction.Request,
        GetJobsStatsAction.Response, QueryPage<JobStats>> {

    private final ClusterService clusterService;
    private final AutodetectProcessManager processManager;
    private final JobResultsProvider jobResultsProvider;

    @Inject
    public TransportGetJobsStatsAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService,
                                       AutodetectProcessManager processManager, JobResultsProvider jobResultsProvider) {
        super(GetJobsStatsAction.NAME, clusterService, transportService, actionFilters, GetJobsStatsAction.Request::new,
            GetJobsStatsAction.Response::new, in -> new QueryPage<>(in, JobStats::new), ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.processManager = processManager;
        this.jobResultsProvider = jobResultsProvider;
    }

    @Override
    protected void doExecute(Task task, GetJobsStatsAction.Request request, ActionListener<GetJobsStatsAction.Response> listener) {
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterService.state());
        request.setExpandedJobsIds(new ArrayList<>(mlMetadata.expandJobIds(request.getJobId(), request.allowNoJobs())));
        ActionListener<GetJobsStatsAction.Response> finalListener = listener;
        listener = ActionListener.wrap(response -> gatherStatsForClosedJobs(mlMetadata,
                request, response, finalListener), listener::onFailure);
        super.doExecute(task, request, listener);
    }

    @Override
    protected GetJobsStatsAction.Response newResponse(GetJobsStatsAction.Request request,
                                                      List<QueryPage<JobStats>> tasks,
                                                      List<TaskOperationFailure> taskOperationFailures,
                                                      List<FailedNodeException> failedNodeExceptions) {
        List<JobStats> stats = new ArrayList<>();
        for (QueryPage<JobStats> task : tasks) {
            stats.addAll(task.results());
        }
        return new GetJobsStatsAction.Response(taskOperationFailures, failedNodeExceptions, new QueryPage<>(stats, stats.size(),
                Job.RESULTS_FIELD));
    }

    @Override
    protected void taskOperation(GetJobsStatsAction.Request request, TransportOpenJobAction.JobTask task,
                                 ActionListener<QueryPage<JobStats>> listener) {
        String jobId = task.getJobId();
        logger.debug("Get stats for job [{}]", jobId);
        ClusterState state = clusterService.state();
        PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        Optional<Tuple<DataCounts, ModelSizeStats>> stats = processManager.getStatistics(task);
        if (stats.isPresent()) {
            PersistentTasksCustomMetaData.PersistentTask<?> pTask = MlTasks.getJobTask(jobId, tasks);
            DiscoveryNode node = state.nodes().get(pTask.getExecutorNode());
            JobState jobState = MlTasks.getJobState(jobId, tasks);
            String assignmentExplanation = pTask.getAssignment().getExplanation();
            TimeValue openTime = durationToTimeValue(processManager.jobOpenTime(task));
            gatherForecastStats(jobId, forecastStats -> {
                JobStats jobStats = new JobStats(jobId, stats.get().v1(),
                        stats.get().v2(), forecastStats, jobState, node, assignmentExplanation, openTime);
                listener.onResponse(new QueryPage<>(Collections.singletonList(jobStats), 1, Job.RESULTS_FIELD));
            }, listener::onFailure);

        } else {
            listener.onResponse(new QueryPage<>(Collections.emptyList(), 0, Job.RESULTS_FIELD));
        }
    }

    // Up until now we gathered the stats for jobs that were open,
    // This method will fetch the stats for missing jobs, that was stored in the jobs index
    void gatherStatsForClosedJobs(MlMetadata mlMetadata, GetJobsStatsAction.Request request, GetJobsStatsAction.Response response,
                                  ActionListener<GetJobsStatsAction.Response> listener) {
        List<String> jobIds = determineNonDeletedJobIdsWithoutLiveStats(mlMetadata,
                request.getExpandedJobsIds(), response.getResponse().results());
        if (jobIds.isEmpty()) {
            listener.onResponse(response);
            return;
        }

        AtomicInteger counter = new AtomicInteger(jobIds.size());
        AtomicArray<JobStats> jobStats = new AtomicArray<>(jobIds.size());
        PersistentTasksCustomMetaData tasks = clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        for (int i = 0; i < jobIds.size(); i++) {
            int slot = i;
            String jobId = jobIds.get(i);
            gatherForecastStats(jobId, forecastStats -> {
                gatherDataCountsAndModelSizeStats(jobId, (dataCounts, modelSizeStats) -> {
                    JobState jobState = MlTasks.getJobState(jobId, tasks);
                    PersistentTasksCustomMetaData.PersistentTask<?> pTask = MlTasks.getJobTask(jobId, tasks);
                    String assignmentExplanation = null;
                    if (pTask != null) {
                        assignmentExplanation = pTask.getAssignment().getExplanation();
                    }
                    jobStats.set(slot, new JobStats(jobId, dataCounts, modelSizeStats, forecastStats, jobState,
                            null, assignmentExplanation, null));
                    if (counter.decrementAndGet() == 0) {
                        List<JobStats> results = response.getResponse().results();
                        results.addAll(jobStats.asList());
                        listener.onResponse(new GetJobsStatsAction.Response(response.getTaskFailures(), response.getNodeFailures(),
                                new QueryPage<>(results, results.size(), Job.RESULTS_FIELD)));
                    }
                }, listener::onFailure);
            }, listener::onFailure);
        }
    }

    void gatherForecastStats(String jobId, Consumer<ForecastStats> handler, Consumer<Exception> errorHandler) {
        jobResultsProvider.getForecastStats(jobId, handler, errorHandler);
    }

    void gatherDataCountsAndModelSizeStats(String jobId, BiConsumer<DataCounts, ModelSizeStats> handler,
                                                   Consumer<Exception> errorHandler) {
        jobResultsProvider.dataCounts(jobId, dataCounts -> {
            jobResultsProvider.modelSizeStats(jobId, modelSizeStats -> {
                handler.accept(dataCounts, modelSizeStats);
            }, errorHandler);
        }, errorHandler);
    }

    static TimeValue durationToTimeValue(Optional<Duration> duration) {
        if (duration.isPresent()) {
            return TimeValue.timeValueSeconds(duration.get().getSeconds());
        } else {
            return null;
        }
    }

    static List<String> determineNonDeletedJobIdsWithoutLiveStats(MlMetadata mlMetadata,
                                                                  List<String> requestedJobIds,
                                                                  List<JobStats> stats) {
        Set<String> excludeJobIds = stats.stream().map(JobStats::getJobId).collect(Collectors.toSet());
        return requestedJobIds.stream().filter(jobId -> !excludeJobIds.contains(jobId) &&
                !mlMetadata.isJobDeleting(jobId)).collect(Collectors.toList());
    }
}
