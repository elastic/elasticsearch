/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.task.JobTask;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.ml.utils.ExceptionCollectionHandling.exceptionArrayToStatusException;

public class TransportCloseJobAction extends TransportTasksAction<
    JobTask,
    CloseJobAction.Request,
    CloseJobAction.Response,
    CloseJobAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportCloseJobAction.class);

    private final ThreadPool threadPool;
    private final Client client;
    private final AnomalyDetectionAuditor auditor;
    private final PersistentTasksService persistentTasksService;
    private final JobConfigProvider jobConfigProvider;
    private final DatafeedConfigProvider datafeedConfigProvider;

    @Inject
    public TransportCloseJobAction(
        TransportService transportService,
        Client client,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ClusterService clusterService,
        AnomalyDetectionAuditor auditor,
        PersistentTasksService persistentTasksService,
        JobConfigProvider jobConfigProvider,
        DatafeedConfigProvider datafeedConfigProvider
    ) {
        // We fork in innerTaskOperation(...), so we can use ThreadPool.Names.SAME here:
        super(
            CloseJobAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            CloseJobAction.Request::new,
            CloseJobAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.threadPool = threadPool;
        this.client = client;
        this.auditor = auditor;
        this.persistentTasksService = persistentTasksService;
        this.jobConfigProvider = jobConfigProvider;
        this.datafeedConfigProvider = datafeedConfigProvider;
    }

    @Override
    protected void doExecute(Task task, CloseJobAction.Request request, ActionListener<CloseJobAction.Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        if (request.isLocal() == false && nodes.isLocalNodeElectedMaster() == false) {
            // Delegates close job to elected master node, so it becomes the coordinating node.
            // See comment in OpenJobAction.Transport class for more information.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException());
            } else {
                transportService.sendRequest(
                    nodes.getMasterNode(),
                    actionName,
                    request,
                    new ActionListenerResponseHandler<>(listener, CloseJobAction.Response::new, TransportResponseHandler.TRANSPORT_WORKER)
                );
            }
        } else {
            /*
             * Closing of multiple jobs:
             *
             * 1. Resolve and validate jobs first: if any job does not meet the
             * criteria (e.g. open datafeed), fail immediately, do not close any
             * job
             *
             * 2. If any of the jobs to be closed have running datafeeds, these
             * are stopped first, using the same level of force as the close request
             *
             * 3. Internally a task request is created for every open job, so there
             * are n inner tasks for 1 user request
             *
             * 4. No task is created for closing jobs but those will be waited on
             *
             * 5. Collect n inner task results or failures and send 1 outer
             * result/failure
             */
            final boolean isForce = request.isForce();
            final TimeValue timeout = request.getCloseTimeout();

            PersistentTasksCustomMetadata tasksMetadata = PersistentTasksCustomMetadata.get(state.metadata().getDefaultProject());
            jobConfigProvider.expandJobsIds(
                request.getJobId(),
                request.allowNoMatch(),
                true,
                tasksMetadata,
                isForce,
                null,
                listener.delegateFailureAndWrap(
                    (delegate, expandedJobIds) -> validate(
                        expandedJobIds,
                        isForce,
                        tasksMetadata,
                        delegate.delegateFailureAndWrap(
                            (delegate2, response) -> stopDatafeedsIfNecessary(
                                response,
                                isForce,
                                timeout,
                                tasksMetadata,
                                delegate2.delegateFailureAndWrap((delegate3, bool) -> {
                                    request.setOpenJobIds(response.openJobIds.toArray(new String[0]));
                                    if (response.openJobIds.isEmpty() && response.closingJobIds.isEmpty()) {
                                        delegate3.onResponse(new CloseJobAction.Response(true));
                                        return;
                                    }

                                    if (isForce) {
                                        List<String> jobIdsToForceClose = new ArrayList<>(response.openJobIds);
                                        jobIdsToForceClose.addAll(response.closingJobIds);
                                        forceCloseJob(state, request, jobIdsToForceClose, delegate3);
                                    } else {
                                        Set<String> executorNodes = new HashSet<>();
                                        for (String resolvedJobId : request.getOpenJobIds()) {
                                            PersistentTasksCustomMetadata.PersistentTask<?> jobTask = MlTasks.getJobTask(
                                                resolvedJobId,
                                                tasksMetadata
                                            );
                                            if (jobTask == null) {
                                                // This should not happen, because openJobIds was
                                                // derived from the same tasks metadata as jobTask
                                                String msg = "Requested job ["
                                                    + resolvedJobId
                                                    + "] be stopped, but job's task could not be found.";
                                                assert jobTask != null : msg;
                                                logger.error(msg);
                                            } else if (jobTask.isAssigned()) {
                                                executorNodes.add(jobTask.getExecutorNode());
                                            } else {
                                                // This is the easy case - the job is not currently assigned to a node, so can
                                                // be gracefully stopped simply by removing its persistent task. (Usually a
                                                // graceful stop cannot be achieved by simply removing the persistent task, but
                                                // if the job has no running code then graceful/forceful are basically the same.)
                                                // The listener here can be a no-op, as waitForJobClosed() already waits for
                                                // these persistent tasks to disappear.
                                                persistentTasksService.sendRemoveRequest(
                                                    jobTask.getId(),
                                                    MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
                                                    ActionListener.wrap(
                                                        r -> logger.trace(
                                                            () -> format("[%s] removed task to close unassigned job", resolvedJobId)
                                                        ),
                                                        e -> logger.error(
                                                            () -> format(
                                                                "[%s] failed to remove task to close unassigned job",
                                                                resolvedJobId
                                                            ),
                                                            e
                                                        )
                                                    )
                                                );
                                            }
                                        }
                                        request.setNodes(executorNodes.toArray(new String[0]));

                                        normalCloseJob(state, task, request, response.openJobIds, response.closingJobIds, delegate3);
                                    }
                                })
                            )
                        )
                    )
                )
            );
        }
    }

    static class OpenAndClosingIds {
        OpenAndClosingIds() {
            openJobIds = new ArrayList<>();
            closingJobIds = new ArrayList<>();
        }

        List<String> openJobIds;
        final List<String> closingJobIds;
    }

    /**
     * Separate the job Ids into open and closing job Ids and validate.
     * If a job is failed it is will not be closed unless the force parameter
     * in request is true.
     *
     * @param expandedJobIds The job ids
     * @param forceClose Force close the job(s)
     * @param tasksMetadata Persistent tasks
     * @param listener Resolved job Ids listener
     */
    void validate(
        Collection<String> expandedJobIds,
        boolean forceClose,
        PersistentTasksCustomMetadata tasksMetadata,
        ActionListener<OpenAndClosingIds> listener
    ) {

        OpenAndClosingIds ids = new OpenAndClosingIds();
        List<String> failedJobs = new ArrayList<>();

        for (String jobId : expandedJobIds) {
            addJobAccordingToState(jobId, tasksMetadata, ids.openJobIds, ids.closingJobIds, failedJobs);
        }

        if (forceClose == false && failedJobs.size() > 0) {
            if (expandedJobIds.size() == 1) {
                listener.onFailure(
                    ExceptionsHelper.conflictStatusException(
                        "cannot close job [{}] because it failed, use force close",
                        expandedJobIds.iterator().next()
                    )
                );
                return;
            }
            listener.onFailure(ExceptionsHelper.conflictStatusException("one or more jobs have state failed, use force close"));
            return;
        }

        // If there are failed jobs force close is true
        ids.openJobIds.addAll(failedJobs);
        listener.onResponse(ids);
    }

    void stopDatafeedsIfNecessary(
        OpenAndClosingIds jobIds,
        boolean isForce,
        TimeValue timeout,
        PersistentTasksCustomMetadata tasksMetadata,
        ActionListener<Boolean> listener
    ) {
        datafeedConfigProvider.findDatafeedIdsForJobIds(jobIds.openJobIds, listener.delegateFailureAndWrap((delegate, datafeedIds) -> {
            List<String> runningDatafeedIds = datafeedIds.stream()
                .filter(datafeedId -> MlTasks.getDatafeedState(datafeedId, tasksMetadata) != DatafeedState.STOPPED)
                .collect(Collectors.toList());
            if (runningDatafeedIds.isEmpty()) {
                delegate.onResponse(false);
            } else {
                if (isForce) {
                    // A datafeed with an end time will gracefully close its job when it stops even if it was force stopped.
                    // If we didn't do anything about this then it would turn requests to force close jobs into normal close
                    // requests for those datafeeds, which is undesirable - the caller specifically asked for the job to be
                    // closed forcefully, skipping the final state persistence to save time. Therefore, before stopping the
                    // datafeeds in this case we isolate them. An isolated datafeed will NOT close its associated job under
                    // any circumstances. The downside of doing this is that if the stop datafeeds call fails then this API
                    // will not have closed any jobs, but will have isolated one or more datafeeds, so the failure will have
                    // an unexpected side effect. Hopefully the project to combine jobs and datafeeds will be able to improve
                    // on this.
                    isolateDatafeeds(
                        jobIds.openJobIds,
                        runningDatafeedIds,
                        delegate.delegateFailureAndWrap((l, r) -> stopDatafeeds(runningDatafeedIds, true, timeout, l))
                    );
                } else {
                    stopDatafeeds(runningDatafeedIds, false, timeout, delegate);
                }
            }
        }));
    }

    private void stopDatafeeds(List<String> runningDatafeedIds, boolean isForce, TimeValue timeout, ActionListener<Boolean> listener) {
        StopDatafeedAction.Request request = new StopDatafeedAction.Request(String.join(",", runningDatafeedIds));
        request.setForce(isForce);
        request.setStopTimeout(timeout);
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.ML_ORIGIN,
            StopDatafeedAction.INSTANCE,
            request,
            ActionListener.wrap(
                r -> listener.onResponse(r.isStopped()),
                e -> listener.onFailure(
                    ExceptionsHelper.conflictStatusException(
                        "failed to close jobs as one or more had started datafeeds that could not be stopped: "
                            + "started datafeeds [{}], error stopping them [{}]",
                        e,
                        request.getDatafeedId(),
                        e.getMessage()
                    )
                )
            )
        );
    }

    void isolateDatafeeds(List<String> openJobs, List<String> runningDatafeedIds, ActionListener<Void> listener) {

        GroupedActionListener<IsolateDatafeedAction.Response> groupedListener = new GroupedActionListener<>(
            runningDatafeedIds.size(),
            ActionListener.wrap(c -> listener.onResponse(null), e -> {
                // This is deliberately NOT an error. The reasoning is as follows:
                // - Isolate datafeed just sets a flag on the datafeed, so cannot fail IF it reaches the running datafeed code
                // - If the request fails because it cannot get to a node running the datafeed then that will be because either:
                // 1. The datafeed isn't assigned to a node
                // 2. There's no master node
                // - But because close job runs on the master node, it cannot be option 2 - this code is running there
                // - In the case where a datafeed isn't assigned to a node, stop and force stop, with and without isolation, are
                // all the same
                // - So we might as well move onto the next step, which is to force stop these same datafeeds (we know this because
                // this is a specialist internal method of closing a job, not a generic isolation method)
                // - Force stopping the datafeeds operates purely on the master node (i.e. the current node where this code is
                // running), and is a simple cluster state update, so will not fail in the same way
                // Unfortunately there is still a race condition here, which is that the datafeed could get assigned to a node
                // after the isolation request fails but before we follow up with the force close. In this case force stopping
                // the datafeed will gracefully close the associated job if the datafeed has an end time, which is not what we
                // want. But this will be a rare edge case. Hopefully the loopholes can be closed during the job/datafeed
                // unification project. In the meantime we'll log at a level that is usually enabled, to make diagnosing the
                // race condition easier.
                logger.info("could not isolate all datafeeds while force closing jobs " + openJobs, e);
                listener.onResponse(null);
            })
        );

        for (String runningDatafeedId : runningDatafeedIds) {
            IsolateDatafeedAction.Request request = new IsolateDatafeedAction.Request(runningDatafeedId);
            ClientHelper.executeAsyncWithOrigin(client, ClientHelper.ML_ORIGIN, IsolateDatafeedAction.INSTANCE, request, groupedListener);
        }
    }

    static void addJobAccordingToState(
        String jobId,
        PersistentTasksCustomMetadata tasksMetadata,
        List<String> openJobs,
        List<String> closingJobs,
        List<String> failedJobs
    ) {

        JobState jobState = MlTasks.getJobState(jobId, tasksMetadata);
        switch (jobState) {
            case CLOSING -> closingJobs.add(jobId);
            case FAILED -> failedJobs.add(jobId);
            case OPENING, OPENED -> openJobs.add(jobId);
        }
    }

    static TransportCloseJobAction.WaitForCloseRequest buildWaitForCloseRequest(
        List<String> openJobIds,
        List<String> closingJobIds,
        PersistentTasksCustomMetadata tasks,
        AnomalyDetectionAuditor auditor
    ) {
        TransportCloseJobAction.WaitForCloseRequest waitForCloseRequest = new TransportCloseJobAction.WaitForCloseRequest();

        for (String jobId : openJobIds) {
            PersistentTasksCustomMetadata.PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
            if (jobTask != null) {
                auditor.info(jobId, Messages.JOB_AUDIT_CLOSING);
                waitForCloseRequest.persistentTasks.add(jobTask);
                waitForCloseRequest.jobsToFinalize.add(jobId);
            }
        }
        for (String jobId : closingJobIds) {
            PersistentTasksCustomMetadata.PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
            if (jobTask != null) {
                waitForCloseRequest.persistentTasks.add(jobTask);
            }
        }

        return waitForCloseRequest;
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        CloseJobAction.Request request,
        JobTask jobTask,
        ActionListener<CloseJobAction.Response> listener
    ) {
        JobTaskState taskState = new JobTaskState(JobState.CLOSING, jobTask.getAllocationId(), "close job (api)", Instant.now());
        jobTask.updatePersistentTaskState(taskState, ActionListener.wrap(task -> {
            // we need to fork because we are now on a network threadpool and closeJob method may take a while to complete:
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                        logger.trace(
                            () -> format(
                                "[%s] [%s] failed to close job due to resource not found exception",
                                jobTask.getJobId(),
                                jobTask.getId()
                            ),
                            e
                        );
                        jobTask.closeJob("close job (api)");
                        listener.onResponse(new CloseJobAction.Response(true));
                    } else {
                        listener.onFailure(e);
                    }
                }

                @Override
                protected void doRun() {
                    jobTask.closeJob("close job (api)");
                    listener.onResponse(new CloseJobAction.Response(true));
                }
            });
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                logger.trace(
                    () -> format(
                        "[%s] [%s] failed to update job to closing due to resource not found exception",
                        jobTask.getJobId(),
                        jobTask.getId()
                    ),
                    e
                );
                listener.onResponse(new CloseJobAction.Response(true));
            } else {
                listener.onFailure(e);
            }
        }));
    }

    @Override
    protected CloseJobAction.Response newResponse(
        CloseJobAction.Request request,
        List<CloseJobAction.Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {

        // number of resolved jobs should be equal to the number of tasks,
        // otherwise something went wrong
        if (request.getOpenJobIds().length != tasks.size()) {
            if (taskOperationFailures.isEmpty() == false) {
                throw ExceptionsHelper.taskOperationFailureToStatusException(taskOperationFailures.get(0));
            } else if (failedNodeExceptions.isEmpty() == false) {
                throw failedNodeExceptions.get(0);
            } else {
                // This can happen when the actual task in the node no longer exists,
                // which means the job(s) have already been closed.
                return new CloseJobAction.Response(true);
            }
        }

        return new CloseJobAction.Response(tasks.stream().allMatch(CloseJobAction.Response::isClosed));
    }

    private void forceCloseJob(
        ClusterState currentState,
        CloseJobAction.Request request,
        List<String> jobIdsToForceClose,
        ActionListener<CloseJobAction.Response> listener
    ) {
        PersistentTasksCustomMetadata tasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(currentState);

        final int numberOfJobs = jobIdsToForceClose.size();
        final AtomicInteger counter = new AtomicInteger();
        final AtomicArray<Exception> failures = new AtomicArray<>(numberOfJobs);

        for (String jobId : jobIdsToForceClose) {
            PersistentTasksCustomMetadata.PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
            if (jobTask != null) {
                auditor.info(jobId, Messages.JOB_AUDIT_FORCE_CLOSING);
                persistentTasksService.sendRemoveRequest(
                    jobTask.getId(),
                    MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(PersistentTasksCustomMetadata.PersistentTask<?> task) {
                            if (counter.incrementAndGet() == numberOfJobs) {
                                sendResponseOrFailure(request.getJobId(), listener, failures);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            final int slot = counter.incrementAndGet();
                            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException == false) {
                                failures.set(slot - 1, e);
                            }
                            if (slot == numberOfJobs) {
                                sendResponseOrFailure(request.getJobId(), listener, failures);
                            }
                        }

                        private static void sendResponseOrFailure(
                            String jobId,
                            ActionListener<CloseJobAction.Response> listener,
                            AtomicArray<Exception> failures
                        ) {
                            List<Exception> caughtExceptions = failures.asList();
                            if (caughtExceptions.isEmpty()) {
                                listener.onResponse(new CloseJobAction.Response(true));
                                return;
                            }

                            String msg = "Failed to force close job ["
                                + jobId
                                + "] with ["
                                + caughtExceptions.size()
                                + "] failures, rethrowing first. All Exceptions: ["
                                + caughtExceptions.stream().map(Exception::getMessage).collect(Collectors.joining(", "))
                                + "]";

                            ElasticsearchStatusException e = exceptionArrayToStatusException(failures, msg);
                            listener.onFailure(e);
                        }
                    }
                );
            }
        }
    }

    private void normalCloseJob(
        ClusterState currentState,
        Task task,
        CloseJobAction.Request request,
        List<String> openJobIds,
        List<String> closingJobIds,
        ActionListener<CloseJobAction.Response> listener
    ) {
        PersistentTasksCustomMetadata tasks = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(currentState);

        WaitForCloseRequest waitForCloseRequest = buildWaitForCloseRequest(openJobIds, closingJobIds, tasks, auditor);

        // If there are no open or closing jobs in the request return
        if (waitForCloseRequest.hasJobsToWaitFor() == false) {
            listener.onResponse(new CloseJobAction.Response(true));
            return;
        }

        final Set<String> movedJobs = ConcurrentCollections.newConcurrentSet();

        ActionListener<CloseJobAction.Response> intermediateListener = listener.delegateFailureAndWrap((delegate, response) -> {
            for (String jobId : movedJobs) {
                PersistentTasksCustomMetadata.PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
                persistentTasksService.sendRemoveRequest(
                    jobTask.getId(),
                    MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
                    ActionListener.wrap(r -> logger.trace("[{}] removed persistent task for relocated job", jobId), e -> {
                        if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                            logger.debug("[{}] relocated job task already removed", jobId);
                        } else {
                            logger.error("[" + jobId + "] failed to remove task to stop relocated job", e);
                        }
                    })
                );
            }
            delegate.onResponse(response);
        });

        boolean noOpenJobsToClose = openJobIds.isEmpty();
        if (noOpenJobsToClose) {
            // No jobs to close but we still want to wait on closing jobs in the request
            waitForJobClosed(request, waitForCloseRequest, new CloseJobAction.Response(true), intermediateListener, movedJobs);
            return;
        }

        ActionListener<CloseJobAction.Response> finalListener = intermediateListener.delegateFailureAndWrap(
            (l, r) -> waitForJobClosed(request, waitForCloseRequest, r, l, movedJobs)
        );
        super.doExecute(task, request, finalListener);
    }

    static class WaitForCloseRequest {
        final List<PersistentTasksCustomMetadata.PersistentTask<?>> persistentTasks = new ArrayList<>();
        final List<String> jobsToFinalize = new ArrayList<>();

        public boolean hasJobsToWaitFor() {
            return persistentTasks.isEmpty() == false;
        }
    }

    /**
     * Wait for job to be marked as closed in cluster state, which means the job persistent task has been removed
     * This api returns when job has been closed, but that doesn't mean the persistent task has been removed from cluster state,
     * so wait for that to happen here.
     *
     * Since the close job action consists of a chain of async callbacks, it's possible that jobs have moved nodes since we decided
     * what to do with them at the beginning of the chain.  We cannot simply wait for these, as the request to stop them will have
     * been sent to the wrong node and ignored there, so we'll just spin until the timeout expires.
     */
    void waitForJobClosed(
        CloseJobAction.Request request,
        WaitForCloseRequest waitForCloseRequest,
        CloseJobAction.Response response,
        ActionListener<CloseJobAction.Response> listener,
        Set<String> movedJobs
    ) {
        @FixForMultiProject
        final var projectId = Metadata.DEFAULT_PROJECT_ID;
        persistentTasksService.waitForPersistentTasksCondition(projectId, persistentTasksCustomMetadata -> {
            if (persistentTasksCustomMetadata == null) {
                return true;
            }
            for (PersistentTasksCustomMetadata.PersistentTask<?> originalPersistentTask : waitForCloseRequest.persistentTasks) {
                String originalPersistentTaskId = originalPersistentTask.getId();
                PersistentTasksCustomMetadata.PersistentTask<?> currentPersistentTask = persistentTasksCustomMetadata.getTask(
                    originalPersistentTaskId
                );
                if (currentPersistentTask != null) {
                    if (Objects.equals(originalPersistentTask.getExecutorNode(), currentPersistentTask.getExecutorNode())
                        && originalPersistentTask.getAllocationId() == currentPersistentTask.getAllocationId()) {
                        return false;
                    }
                    OpenJobAction.JobParams params = (OpenJobAction.JobParams) originalPersistentTask.getParams();
                    if (movedJobs.add(params.getJobId())) {
                        logger.info("Job [{}] changed assignment while waiting for it to be closed", params.getJobId());
                    }
                }
            }
            return true;
        }, request.getCloseTimeout(), listener.safeMap(r -> response));
    }
}
