/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.ResetJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Blocked;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportResetJobAction extends AcknowledgedTransportMasterNodeAction<ResetJobAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportResetJobAction.class);

    private final Client client;
    private final JobConfigProvider jobConfigProvider;
    private final JobResultsProvider jobResultsProvider;
    private final AnomalyDetectionAuditor auditor;

    @Inject
    public TransportResetJobAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        JobConfigProvider jobConfigProvider,
        JobResultsProvider jobResultsProvider,
        AnomalyDetectionAuditor auditor
    ) {
        super(
            ResetJobAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ResetJobAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.client = Objects.requireNonNull(client);
        this.jobConfigProvider = Objects.requireNonNull(jobConfigProvider);
        this.jobResultsProvider = Objects.requireNonNull(jobResultsProvider);
        this.auditor = Objects.requireNonNull(auditor);
    }

    @Override
    protected void masterOperation(
        Task task,
        ResetJobAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        if (MlMetadata.getMlMetadata(state).isUpgradeMode()) {
            listener.onFailure(ExceptionsHelper.conflictStatusException("cannot reset job while indices are being upgraded"));
            return;
        }

        final TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());

        ActionListener<Job.Builder> jobListener = ActionListener.wrap(jobBuilder -> {
            Job job = jobBuilder.build();
            PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
            JobState jobState = MlTasks.getJobState(job.getId(), tasks);
            if (request.isSkipJobStateValidation() == false && jobState != JobState.CLOSED) {
                listener.onFailure(ExceptionsHelper.conflictStatusException(Messages.getMessage(Messages.REST_JOB_NOT_CLOSED_RESET)));
                return;
            }
            if (job.getBlocked().getReason() != Blocked.Reason.NONE && job.getBlocked().getReason() != Blocked.Reason.RESET) {
                listener.onFailure(
                    ExceptionsHelper.conflictStatusException(
                        "cannot reset job while it is blocked with [" + job.getBlocked().getReason() + "]"
                    )
                );
                return;
            }

            if (job.getBlocked().getReason() == Blocked.Reason.RESET) {
                waitExistingResetTaskToComplete(
                    job.getBlocked().getTaskId(),
                    request,
                    ActionListener.wrap(r -> resetIfJobIsStillBlockedOnReset(task, request, listener), listener::onFailure)
                );
            } else {
                ParentTaskAssigningClient taskClient = new ParentTaskAssigningClient(client, taskId);
                jobConfigProvider.updateJobBlockReason(
                    job.getId(),
                    new Blocked(Blocked.Reason.RESET, taskId),
                    ActionListener.wrap(r -> resetJob(taskClient, (CancellableTask) task, request, listener), listener::onFailure)
                );
            }
        }, listener::onFailure);

        jobConfigProvider.getJob(request.getJobId(), jobListener);
    }

    private void waitExistingResetTaskToComplete(
        TaskId existingTaskId,
        ResetJobAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        logger.debug(() -> new ParameterizedMessage("[{}] Waiting on existing reset task: {}", request.getJobId(), existingTaskId));
        GetTaskRequest getTaskRequest = new GetTaskRequest();
        getTaskRequest.setTaskId(existingTaskId);
        getTaskRequest.setWaitForCompletion(true);
        getTaskRequest.setTimeout(request.timeout());
        executeAsyncWithOrigin(client, ML_ORIGIN, GetTaskAction.INSTANCE, getTaskRequest, ActionListener.wrap(getTaskResponse -> {
            TaskResult taskResult = getTaskResponse.getTask();
            if (taskResult.isCompleted()) {
                listener.onResponse(AcknowledgedResponse.of(true));
            } else {
                BytesReference taskError = taskResult.getError();
                if (taskError != null) {
                    listener.onFailure(ExceptionsHelper.serverError("reset failed to complete; error [{}]", taskError.utf8ToString()));
                } else {
                    listener.onFailure(ExceptionsHelper.serverError("reset failed to complete"));
                }
            }
        }, listener::onFailure));
    }

    private void resetIfJobIsStillBlockedOnReset(Task task, ResetJobAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        ActionListener<Job.Builder> jobListener = ActionListener.wrap(jobResponse -> {
            Job job = jobResponse.build();
            if (job.getBlocked().getReason() == Blocked.Reason.NONE) {
                // This means the previous reset task finished successfully as it managed to unset the blocked reason.
                logger.debug(() -> new ParameterizedMessage("[{}] Existing reset task finished successfully", request.getJobId()));
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else if (job.getBlocked().getReason() == Blocked.Reason.RESET) {
                // Seems like the task was removed abruptly as it hasn't unset the block on reset.
                // Let us try reset again.
                logger.debug(
                    () -> new ParameterizedMessage("[{}] Existing reset task was interrupted; retrying reset", request.getJobId())
                );
                ParentTaskAssigningClient taskClient = new ParentTaskAssigningClient(
                    client,
                    new TaskId(clusterService.localNode().getId(), task.getId())
                );
                resetJob(taskClient, (CancellableTask) task, request, listener);
            } else {
                // Blocked reason is now different. Let us just communicate the conflict.
                listener.onFailure(
                    ExceptionsHelper.conflictStatusException(
                        "cannot reset job while it is blocked with [" + job.getBlocked().getReason() + "]"
                    )
                );
            }
        }, listener::onFailure);

        // Get job again to check if it is still blocked
        jobConfigProvider.getJob(request.getJobId(), jobListener);
    }

    private void resetJob(
        ParentTaskAssigningClient taskClient,
        CancellableTask task,
        ResetJobAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        String jobId = request.getJobId();

        // Now that we have updated the job's block reason, we should check again
        // if the job has been opened.
        PersistentTasksCustomMetadata tasks = clusterService.state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        JobState jobState = MlTasks.getJobState(jobId, tasks);
        if (request.isSkipJobStateValidation() == false && jobState != JobState.CLOSED) {
            jobConfigProvider.updateJobBlockReason(
                jobId,
                null,
                ActionListener.wrap(
                    clearResetResponse -> listener.onFailure(
                        ExceptionsHelper.conflictStatusException(Messages.getMessage(Messages.REST_JOB_NOT_CLOSED_RESET))
                    ),
                    e -> listener.onFailure(
                        ExceptionsHelper.conflictStatusException(Messages.getMessage(Messages.REST_JOB_NOT_CLOSED_RESET))
                    )
                )
            );
            return;
        }

        logger.info("[{}] Resetting job", jobId);

        ActionListener<Boolean> resultsIndexCreatedListener = ActionListener.wrap(resultsIndexCreatedResponse -> {
            if (task.isCancelled()) {
                listener.onResponse(AcknowledgedResponse.of(false));
                return;
            }
            finishSuccessfulReset(jobId, listener);
        }, listener::onFailure);

        CheckedConsumer<Boolean, Exception> jobDocsDeletionListener = response -> {
            if (task.isCancelled()) {
                listener.onResponse(AcknowledgedResponse.of(false));
                return;
            }
            jobConfigProvider.getJob(jobId, ActionListener.wrap(jobBuilder -> {
                if (task.isCancelled()) {
                    listener.onResponse(AcknowledgedResponse.of(false));
                    return;
                }
                jobResultsProvider.createJobResultIndex(jobBuilder.build(), clusterService.state(), resultsIndexCreatedListener);
            }, listener::onFailure));
        };

        JobDataDeleter jobDataDeleter = new JobDataDeleter(taskClient, jobId);
        jobDataDeleter.deleteJobDocuments(
            jobConfigProvider,
            indexNameExpressionResolver,
            clusterService.state(),
            jobDocsDeletionListener,
            listener::onFailure
        );
    }

    private void finishSuccessfulReset(String jobId, ActionListener<AcknowledgedResponse> listener) {
        jobConfigProvider.updateJobAfterReset(jobId, ActionListener.wrap(blockReasonUpdatedResponse -> {
            logger.info("[{}] Reset has successfully completed", jobId);
            auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_RESET));
            listener.onResponse(AcknowledgedResponse.of(true));
        }, listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(ResetJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
