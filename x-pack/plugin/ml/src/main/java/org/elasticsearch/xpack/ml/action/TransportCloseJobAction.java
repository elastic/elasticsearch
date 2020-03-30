/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TransportCloseJobAction extends TransportTasksAction<TransportOpenJobAction.JobTask, CloseJobAction.Request,
        CloseJobAction.Response, CloseJobAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportCloseJobAction.class);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final AnomalyDetectionAuditor auditor;
    private final PersistentTasksService persistentTasksService;
    private final JobConfigProvider jobConfigProvider;
    private final DatafeedConfigProvider datafeedConfigProvider;

    @Inject
    public TransportCloseJobAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                                   ClusterService clusterService, AnomalyDetectionAuditor auditor,
                                   PersistentTasksService persistentTasksService, JobConfigProvider jobConfigProvider,
                                   DatafeedConfigProvider datafeedConfigProvider) {
        // We fork in innerTaskOperation(...), so we can use ThreadPool.Names.SAME here:
        super(CloseJobAction.NAME, clusterService, transportService, actionFilters,
            CloseJobAction.Request::new, CloseJobAction.Response::new, CloseJobAction.Response::new, ThreadPool.Names.SAME);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
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
                transportService.sendRequest(nodes.getMasterNode(), actionName, request,
                        new ActionListenerResponseHandler<>(listener, CloseJobAction.Response::new));
            }
        } else {
            /*
             * Closing of multiple jobs:
             *
             * 1. Resolve and validate jobs first: if any job does not meet the
             * criteria (e.g. open datafeed), fail immediately, do not close any
             * job
             *
             * 2. Internally a task request is created for every open job, so there
             * are n inner tasks for 1 user request
             *
             * 3. No task is created for closing jobs but those will be waited on
             *
             * 4. Collect n inner task results or failures and send 1 outer
             * result/failure
             */

            PersistentTasksCustomMetaData tasksMetaData = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            jobConfigProvider.expandJobsIds(request.getJobId(),
                request.allowNoJobs(),
                true,
                tasksMetaData,
                request.isForce(),
                ActionListener.wrap(
                    expandedJobIds -> {
                        validate(expandedJobIds, request.isForce(), tasksMetaData, ActionListener.wrap(
                            response -> {
                                request.setOpenJobIds(response.openJobIds.toArray(new String[0]));
                                if (response.openJobIds.isEmpty() && response.closingJobIds.isEmpty()) {
                                    listener.onResponse(new CloseJobAction.Response(true));
                                    return;
                                }

                                if (request.isForce()) {
                                    List<String> jobIdsToForceClose = new ArrayList<>(response.openJobIds);
                                    jobIdsToForceClose.addAll(response.closingJobIds);
                                    forceCloseJob(state, request, jobIdsToForceClose, listener);
                                } else {
                                    Set<String> executorNodes = new HashSet<>();
                                    PersistentTasksCustomMetaData tasks = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);
                                    for (String resolvedJobId : request.getOpenJobIds()) {
                                        PersistentTasksCustomMetaData.PersistentTask<?> jobTask = MlTasks.getJobTask(resolvedJobId, tasks);
                                        if (jobTask == null) {
                                            // This should not happen, because openJobIds was
                                            // derived from the same tasks metadata as jobTask
                                            String msg = "Requested job [" + resolvedJobId
                                                + "] be stopped, but job's task could not be found.";
                                            assert jobTask != null : msg;
                                            logger.error(msg);
                                        } else if (jobTask.isAssigned()) {
                                            executorNodes.add(jobTask.getExecutorNode());
                                        } else {
                                            // This is the easy case - the job is not currently assigned to a node, so can
                                            // be gracefully stopped simply by removing its persistent task.  (Usually a
                                            // graceful stop cannot be achieved by simply removing the persistent task, but
                                            // if the job has no running code then graceful/forceful are basically the same.)
                                            // The listener here can be a no-op, as waitForJobClosed() already waits for
                                            // these persistent tasks to disappear.
                                            persistentTasksService.sendRemoveRequest(jobTask.getId(),
                                                ActionListener.wrap(
                                                    r -> logger.trace("[{}] removed task to close unassigned job", resolvedJobId),
                                                    e -> logger.error("[" + resolvedJobId
                                                        + "] failed to remove task to close unassigned job", e)
                                                ));
                                        }
                                    }
                                    request.setNodes(executorNodes.toArray(new String[0]));

                                    normalCloseJob(state, task, request, response.openJobIds, response.closingJobIds, listener);
                                }
                                },
                            listener::onFailure
                    ));
                    },
                listener::onFailure
            ));
        }
    }

    class OpenAndClosingIds {
        OpenAndClosingIds() {
            openJobIds = new ArrayList<>();
            closingJobIds = new ArrayList<>();
        }
        List<String> openJobIds;
        List<String> closingJobIds;
    }

    /**
     * Separate the job Ids into open and closing job Ids and validate.
     * If a job is failed it is will not be closed unless the force parameter
     * in request is true.
     * It is an error if the datafeed the job uses is not stopped
     *
     * @param expandedJobIds The job ids
     * @param forceClose Force close the job(s)
     * @param tasksMetaData Persistent tasks
     * @param listener Resolved job Ids listener
     */
    void validate(Collection<String> expandedJobIds, boolean forceClose, PersistentTasksCustomMetaData tasksMetaData,
                  ActionListener<OpenAndClosingIds> listener) {

        checkDatafeedsHaveStopped(expandedJobIds, tasksMetaData, ActionListener.wrap(
                response -> {
                    OpenAndClosingIds ids = new OpenAndClosingIds();
                    List<String> failedJobs = new ArrayList<>();

                    for (String jobId : expandedJobIds) {
                        addJobAccordingToState(jobId, tasksMetaData, ids.openJobIds, ids.closingJobIds, failedJobs);
                    }

                    if (forceClose == false && failedJobs.size() > 0) {
                        if (expandedJobIds.size() == 1) {
                            listener.onFailure(
                                    ExceptionsHelper.conflictStatusException("cannot close job [{}] because it failed, use force close",
                                            expandedJobIds.iterator().next()));
                            return;
                        }
                        listener.onFailure(
                                ExceptionsHelper.conflictStatusException("one or more jobs have state failed, use force close"));
                        return;
                    }

                    // If there are failed jobs force close is true
                    ids.openJobIds.addAll(failedJobs);
                    listener.onResponse(ids);
                },
                listener::onFailure
        ));
    }

    void checkDatafeedsHaveStopped(Collection<String> jobIds, PersistentTasksCustomMetaData tasksMetaData,
                                   ActionListener<Boolean> listener) {
        datafeedConfigProvider.findDatafeedsForJobIds(jobIds, ActionListener.wrap(
                datafeedIds -> {
                    for (String datafeedId : datafeedIds) {
                        DatafeedState datafeedState = MlTasks.getDatafeedState(datafeedId, tasksMetaData);
                        if (datafeedState != DatafeedState.STOPPED) {
                            listener.onFailure(ExceptionsHelper.conflictStatusException(
                                "cannot close job datafeed [{}] hasn't been stopped", datafeedId));
                            return;
                        }
                    }
                    listener.onResponse(Boolean.TRUE);
                },
                listener::onFailure
        ));
    }

    static void addJobAccordingToState(String jobId, PersistentTasksCustomMetaData tasksMetaData,
                                       List<String> openJobs, List<String> closingJobs, List<String> failedJobs) {

        JobState jobState = MlTasks.getJobState(jobId, tasksMetaData);
        switch (jobState) {
            case CLOSING:
                closingJobs.add(jobId);
                break;
            case FAILED:
                failedJobs.add(jobId);
                break;
            case OPENING:
            case OPENED:
                openJobs.add(jobId);
                break;
            default:
                break;
        }
    }

    static TransportCloseJobAction.WaitForCloseRequest buildWaitForCloseRequest(List<String> openJobIds,
                                                                                List<String> closingJobIds,
                                                                                PersistentTasksCustomMetaData tasks,
                                                                                AnomalyDetectionAuditor auditor) {
        TransportCloseJobAction.WaitForCloseRequest waitForCloseRequest = new TransportCloseJobAction.WaitForCloseRequest();

        for (String jobId : openJobIds) {
            PersistentTasksCustomMetaData.PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
            if (jobTask != null) {
                auditor.info(jobId, Messages.JOB_AUDIT_CLOSING);
                waitForCloseRequest.persistentTaskIds.add(jobTask.getId());
                waitForCloseRequest.jobsToFinalize.add(jobId);
            }
        }
        for (String jobId : closingJobIds) {
            PersistentTasksCustomMetaData.PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
            if (jobTask != null) {
                waitForCloseRequest.persistentTaskIds.add(jobTask.getId());
            }
        }

        return waitForCloseRequest;
    }


    @Override
    protected void taskOperation(CloseJobAction.Request request, TransportOpenJobAction.JobTask jobTask,
                                 ActionListener<CloseJobAction.Response> listener) {
        JobTaskState taskState = new JobTaskState(JobState.CLOSING, jobTask.getAllocationId(), "close job (api)");
        jobTask.updatePersistentTaskState(taskState, ActionListener.wrap(task -> {
            // we need to fork because we are now on a network threadpool and closeJob method may take a while to complete:
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ResourceNotFoundException && Strings.isAllOrWildcard(new String[]{request.getJobId()})) {
                        jobTask.closeJob("close job (api)");
                        listener.onResponse(new CloseJobAction.Response(true));
                    } else {
                        listener.onFailure(e);
                    }
                }

                @Override
                protected void doRun() throws Exception {
                    jobTask.closeJob("close job (api)");
                    listener.onResponse(new CloseJobAction.Response(true));
                }
            });
        }, listener::onFailure));
    }

    @Override
    protected CloseJobAction.Response newResponse(CloseJobAction.Request request, List<CloseJobAction.Response> tasks,
                                                  List<TaskOperationFailure> taskOperationFailures,
                                                  List<FailedNodeException> failedNodeExceptions) {

        // number of resolved jobs should be equal to the number of tasks,
        // otherwise something went wrong
        if (request.getOpenJobIds().length != tasks.size()) {
            if (taskOperationFailures.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper
                        .convertToElastic(taskOperationFailures.get(0).getCause());
            } else if (failedNodeExceptions.isEmpty() == false) {
                throw org.elasticsearch.ExceptionsHelper
                        .convertToElastic(failedNodeExceptions.get(0));
            } else {
                // This can happen we the actual task in the node no longer exists,
                // which means the job(s) have already been closed.
                return new CloseJobAction.Response(true);
            }
        }

        return new CloseJobAction.Response(tasks.stream().allMatch(CloseJobAction.Response::isClosed));
    }

    private void forceCloseJob(ClusterState currentState, CloseJobAction.Request request, List<String> jobIdsToForceClose,
                               ActionListener<CloseJobAction.Response> listener) {
        PersistentTasksCustomMetaData tasks = currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        final int numberOfJobs = jobIdsToForceClose.size();
        final AtomicInteger counter = new AtomicInteger();
        final AtomicArray<Exception> failures = new AtomicArray<>(numberOfJobs);

        for (String jobId : jobIdsToForceClose) {
            PersistentTasksCustomMetaData.PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
            if (jobTask != null) {
                auditor.info(jobId, Messages.JOB_AUDIT_FORCE_CLOSING);
                persistentTasksService.sendRemoveRequest(jobTask.getId(),
                        new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                            @Override
                            public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> task) {
                                if (counter.incrementAndGet() == numberOfJobs) {
                                    sendResponseOrFailure(request.getJobId(), listener, failures);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                final int slot = counter.incrementAndGet();
                                if ((ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException &&
                                    Strings.isAllOrWildcard(new String[]{request.getJobId()})) == false) {
                                    failures.set(slot - 1, e);
                                }
                                if (slot == numberOfJobs) {
                                    sendResponseOrFailure(request.getJobId(), listener, failures);
                                }
                            }

                            private void sendResponseOrFailure(String jobId,
                                                               ActionListener<CloseJobAction.Response> listener,
                                                               AtomicArray<Exception> failures) {
                                List<Exception> caughtExceptions = failures.asList();
                                if (caughtExceptions.size() == 0) {
                                    listener.onResponse(new CloseJobAction.Response(true));
                                    return;
                                }

                                String msg = "Failed to force close job [" + jobId + "] with ["
                                        + caughtExceptions.size()
                                        + "] failures, rethrowing last, all Exceptions: ["
                                        + caughtExceptions.stream().map(Exception::getMessage)
                                        .collect(Collectors.joining(", "))
                                        + "]";

                                ElasticsearchException e = new ElasticsearchException(msg, caughtExceptions.get(0));
                                listener.onFailure(e);
                            }
                        });
            }
        }
    }

    private void normalCloseJob(ClusterState currentState, Task task, CloseJobAction.Request request,
                                List<String> openJobIds, List<String> closingJobIds,
                                ActionListener<CloseJobAction.Response> listener) {
        PersistentTasksCustomMetaData tasks = currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        WaitForCloseRequest waitForCloseRequest = buildWaitForCloseRequest(openJobIds, closingJobIds, tasks, auditor);

        // If there are no open or closing jobs in the request return
        if (waitForCloseRequest.hasJobsToWaitFor() == false) {
            listener.onResponse(new CloseJobAction.Response(true));
            return;
        }

        boolean noOpenJobsToClose = openJobIds.isEmpty();
        if (noOpenJobsToClose) {
            // No jobs to close but we still want to wait on closing jobs in the request
            waitForJobClosed(request, waitForCloseRequest, new CloseJobAction.Response(true), listener);
            return;
        }

        ActionListener<CloseJobAction.Response> finalListener =
                ActionListener.wrap(
                        r -> waitForJobClosed(request, waitForCloseRequest,
                        r, listener),
                        listener::onFailure);
        super.doExecute(task, request, finalListener);
    }

    static class WaitForCloseRequest {
        List<String> persistentTaskIds = new ArrayList<>();
        List<String> jobsToFinalize = new ArrayList<>();

        public boolean hasJobsToWaitFor() {
            return persistentTaskIds.isEmpty() == false;
        }
    }

    // Wait for job to be marked as closed in cluster state, which means the job persistent task has been removed
    // This api returns when job has been closed, but that doesn't mean the persistent task has been removed from cluster state,
    // so wait for that to happen here.
    void waitForJobClosed(CloseJobAction.Request request, WaitForCloseRequest waitForCloseRequest, CloseJobAction.Response response,
                          ActionListener<CloseJobAction.Response> listener) {
        persistentTasksService.waitForPersistentTasksCondition(persistentTasksCustomMetaData -> {
            for (String persistentTaskId : waitForCloseRequest.persistentTaskIds) {
                if (persistentTasksCustomMetaData.getTask(persistentTaskId) != null) {
                    return false;
                }
            }
            return true;
        }, request.getCloseTimeout(), new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean result) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
