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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Response.JobStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.task.JobTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TransportGetJobsStatsAction extends TransportTasksAction<
    JobTask,
    GetJobsStatsAction.Request,
    GetJobsStatsAction.Response,
    QueryPage<JobStats>> {

    private static final Logger logger = LogManager.getLogger(TransportGetJobsStatsAction.class);

    private final ClusterService clusterService;
    private final AutodetectProcessManager processManager;
    private final JobResultsProvider jobResultsProvider;
    private final JobConfigProvider jobConfigProvider;
    private final ThreadPool threadPool;

    @Inject
    public TransportGetJobsStatsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        AutodetectProcessManager processManager,
        JobResultsProvider jobResultsProvider,
        JobConfigProvider jobConfigProvider,
        ThreadPool threadPool
    ) {
        super(
            GetJobsStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            GetJobsStatsAction.Request::new,
            GetJobsStatsAction.Response::new,
            in -> new QueryPage<>(in, JobStats::new),
            ThreadPool.Names.MANAGEMENT
        );
        this.clusterService = clusterService;
        this.processManager = processManager;
        this.jobResultsProvider = jobResultsProvider;
        this.jobConfigProvider = jobConfigProvider;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, GetJobsStatsAction.Request request, ActionListener<GetJobsStatsAction.Response> finalListener) {
        logger.debug("Get stats for job [{}]", request.getJobId());
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());

        ClusterState state = clusterService.state();
        PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        // If there are deleted configs, but the task is still around, we probably want to return the tasks in the stats call
        jobConfigProvider.expandJobsIds(
            request.getJobId(),
            request.allowNoMatch(),
            true,
            tasks,
            true,
            parentTaskId,
            ActionListener.wrap(expandedIds -> {
                request.setExpandedJobsIds(new ArrayList<>(expandedIds));
                ActionListener<GetJobsStatsAction.Response> jobStatsListener = ActionListener.wrap(
                    response -> gatherStatsForClosedJobs(request, response, parentTaskId, finalListener),
                    finalListener::onFailure
                );
                super.doExecute(task, request, jobStatsListener);
            }, finalListener::onFailure)
        );
    }

    @Override
    protected GetJobsStatsAction.Response newResponse(
        GetJobsStatsAction.Request request,
        List<QueryPage<JobStats>> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        List<JobStats> stats = new ArrayList<>();
        for (QueryPage<JobStats> task : tasks) {
            stats.addAll(task.results());
        }
        stats.sort(Comparator.comparing(JobStats::getJobId));
        return new GetJobsStatsAction.Response(
            taskOperationFailures,
            failedNodeExceptions,
            new QueryPage<>(stats, stats.size(), Job.RESULTS_FIELD)
        );
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        GetJobsStatsAction.Request request,
        JobTask task,
        ActionListener<QueryPage<JobStats>> listener
    ) {
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), actionTask.getId());
        String jobId = task.getJobId();
        ClusterState state = clusterService.state();
        PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        Optional<Tuple<DataCounts, Tuple<ModelSizeStats, TimingStats>>> stats = processManager.getStatistics(task);
        if (stats.isPresent()) {
            DataCounts dataCounts = stats.get().v1();
            ModelSizeStats modelSizeStats = stats.get().v2().v1();
            TimingStats timingStats = stats.get().v2().v2();
            PersistentTasksCustomMetadata.PersistentTask<?> pTask = MlTasks.getJobTask(jobId, tasks);
            DiscoveryNode node = state.nodes().get(pTask.getExecutorNode());
            JobState jobState = MlTasks.getJobState(jobId, tasks);
            String assignmentExplanation = pTask.getAssignment().getExplanation();
            TimeValue openTime = processManager.jobOpenTime(task).map(value -> TimeValue.timeValueSeconds(value.getSeconds())).orElse(null);
            jobResultsProvider.getForecastStats(jobId, parentTaskId, forecastStats -> {
                JobStats jobStats = new JobStats(
                    jobId,
                    dataCounts,
                    modelSizeStats,
                    forecastStats,
                    jobState,
                    node,
                    assignmentExplanation,
                    openTime,
                    timingStats
                );
                listener.onResponse(new QueryPage<>(Collections.singletonList(jobStats), 1, Job.RESULTS_FIELD));
            }, listener::onFailure);

        } else {
            listener.onResponse(new QueryPage<>(Collections.emptyList(), 0, Job.RESULTS_FIELD));
        }
    }

    // Up until now we gathered the stats for jobs that were open,
    // This method will fetch the stats for missing jobs, that was stored in the jobs index
    void gatherStatsForClosedJobs(
        GetJobsStatsAction.Request request,
        GetJobsStatsAction.Response response,
        TaskId parentTaskId,
        ActionListener<GetJobsStatsAction.Response> listener
    ) {
        List<String> closedJobIds = determineJobIdsWithoutLiveStats(request.getExpandedJobsIds(), response.getResponse().results());
        if (closedJobIds.isEmpty()) {
            listener.onResponse(response);
            return;
        }

        AtomicInteger counter = new AtomicInteger(closedJobIds.size());
        AtomicReference<Exception> searchException = new AtomicReference<>();
        AtomicArray<GetJobsStatsAction.Response.JobStats> jobStats = new AtomicArray<>(closedJobIds.size());

        Consumer<Exception> errorHandler = e -> {
            // take the first error
            searchException.compareAndSet(null, e);
            if (counter.decrementAndGet() == 0) {
                listener.onFailure(e);
            }
        };

        PersistentTasksCustomMetadata tasks = clusterService.state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(() -> {
            for (int i = 0; i < closedJobIds.size(); i++) {
                int slot = i;
                String jobId = closedJobIds.get(i);
                jobResultsProvider.getForecastStats(
                    jobId,
                    parentTaskId,
                    forecastStats -> threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
                        .execute(
                            () -> jobResultsProvider.getDataCountsModelSizeAndTimingStats(
                                jobId,
                                parentTaskId,
                                (dataCounts, modelSizeStats, timingStats) -> {
                                    JobState jobState = MlTasks.getJobState(jobId, tasks);
                                    PersistentTasksCustomMetadata.PersistentTask<?> pTask = MlTasks.getJobTask(jobId, tasks);
                                    String assignmentExplanation = null;
                                    if (pTask != null) {
                                        assignmentExplanation = pTask.getAssignment().getExplanation();
                                    }
                                    jobStats.set(
                                        slot,
                                        new JobStats(
                                            jobId,
                                            dataCounts,
                                            modelSizeStats,
                                            forecastStats,
                                            jobState,
                                            null,
                                            assignmentExplanation,
                                            null,
                                            timingStats
                                        )
                                    );
                                    if (counter.decrementAndGet() == 0) {
                                        if (searchException.get() != null) {
                                            // there was an error
                                            listener.onFailure(searchException.get());
                                            return;
                                        }
                                        List<JobStats> results = response.getResponse().results();
                                        results.addAll(jobStats.asList());
                                        results.sort(Comparator.comparing(JobStats::getJobId));
                                        listener.onResponse(
                                            new GetJobsStatsAction.Response(
                                                response.getTaskFailures(),
                                                response.getNodeFailures(),
                                                new QueryPage<>(results, results.size(), Job.RESULTS_FIELD)
                                            )
                                        );
                                    }
                                },
                                errorHandler
                            )
                        ),
                    errorHandler
                );
            }
        });
    }

    static List<String> determineJobIdsWithoutLiveStats(List<String> requestedJobIds, List<GetJobsStatsAction.Response.JobStats> stats) {
        Set<String> excludeJobIds = stats.stream().map(GetJobsStatsAction.Response.JobStats::getJobId).collect(Collectors.toSet());
        return requestedJobIds.stream().filter(jobId -> excludeJobIds.contains(jobId) == false).collect(Collectors.toList());
    }
}
