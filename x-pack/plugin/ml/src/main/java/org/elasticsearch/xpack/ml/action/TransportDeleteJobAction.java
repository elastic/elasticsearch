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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MlConfigMigrationEligibilityCheck;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteJobAction extends AcknowledgedTransportMasterNodeAction<DeleteJobAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteJobAction.class);

    private final Client client;
    private final PersistentTasksService persistentTasksService;
    private final AnomalyDetectionAuditor auditor;
    private final JobResultsProvider jobResultsProvider;
    private final JobConfigProvider jobConfigProvider;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final MlMemoryTracker memoryTracker;
    private final MlConfigMigrationEligibilityCheck migrationEligibilityCheck;

    /**
     * A map of task listeners by job_id.
     * Subsequent delete requests store their listeners in the corresponding list in this map
     * and wait to be notified when the first deletion task completes.
     * This is guarded by synchronizing on its lock.
     */
    private final Map<String, List<ActionListener<AcknowledgedResponse>>> listenersByJobId;

    @Inject
    public TransportDeleteJobAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    ThreadPool threadPool, ActionFilters actionFilters,
                                    IndexNameExpressionResolver indexNameExpressionResolver, PersistentTasksService persistentTasksService,
                                    Client client, AnomalyDetectionAuditor auditor, JobResultsProvider jobResultsProvider,
                                    JobConfigProvider jobConfigProvider, DatafeedConfigProvider datafeedConfigProvider,
                                    MlMemoryTracker memoryTracker) {
        super(DeleteJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
                DeleteJobAction.Request::new, indexNameExpressionResolver, ThreadPool.Names.SAME);
        this.client = client;
        this.persistentTasksService = persistentTasksService;
        this.auditor = auditor;
        this.jobResultsProvider = jobResultsProvider;
        this.jobConfigProvider = jobConfigProvider;
        this.datafeedConfigProvider = datafeedConfigProvider;
        this.memoryTracker = memoryTracker;
        this.migrationEligibilityCheck = new MlConfigMigrationEligibilityCheck(settings, clusterService);
        this.listenersByJobId = new HashMap<>();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, DeleteJobAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {

        if (migrationEligibilityCheck.jobIsEligibleForMigration(request.getJobId(), state)) {
            listener.onFailure(ExceptionsHelper.configHasNotBeenMigrated("delete job", request.getJobId()));
            return;
        }

        logger.debug("Deleting job '{}'", request.getJobId());

        if (request.isForce() == false) {
            checkJobIsNotOpen(request.getJobId(), state);
        }

        TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
        ParentTaskAssigningClient parentTaskClient = new ParentTaskAssigningClient(client, taskId);

        // Check if there is a deletion task for this job already and if yes wait for it to complete
        synchronized (listenersByJobId) {
            if (listenersByJobId.containsKey(request.getJobId())) {
                logger.debug("[{}] Deletion task [{}] will wait for existing deletion task to complete",
                        request.getJobId(), task.getId());
                listenersByJobId.get(request.getJobId()).add(listener);
                return;
            } else {
                List<ActionListener<AcknowledgedResponse>> listeners = new ArrayList<>();
                listeners.add(listener);
                listenersByJobId.put(request.getJobId(), listeners);
            }
        }

        // The listener that will be executed at the end of the chain will notify all listeners
        ActionListener<AcknowledgedResponse> finalListener = ActionListener.wrap(
                ack -> notifyListeners(request.getJobId(), ack, null),
                e -> {
                    notifyListeners(request.getJobId(), null, e);
                    if ((ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) == false) {
                        auditor.error(request.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_DELETING_FAILED, e.getMessage()));
                    }
                }
        );

        ActionListener<Boolean> markAsDeletingListener = ActionListener.wrap(
                response -> {
                    if (request.isForce()) {
                        forceDeleteJob(parentTaskClient, request, finalListener);
                    } else {
                        normalDeleteJob(parentTaskClient, request, finalListener);
                    }
                },
                finalListener::onFailure);

        ActionListener<Boolean> jobExistsListener = ActionListener.wrap(
            response -> {
                auditor.info(request.getJobId(), Messages.getMessage(Messages.JOB_AUDIT_DELETING, taskId));
                markJobAsDeletingIfNotUsed(request.getJobId(), markAsDeletingListener);
            },
            e -> {
                if (request.isForce()
                    && MlTasks.getJobTask(request.getJobId(), state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE)) != null) {
                    logger.info(
                        "[{}] config is missing but task exists. Attempting to delete tasks and stop process",
                        request.getJobId());
                    forceDeleteJob(parentTaskClient, request, finalListener);
                } else {
                    finalListener.onFailure(e);
                }
            });

        // First check that the job exists, because we don't want to audit
        // the beginning of its deletion if it didn't exist in the first place
        jobConfigProvider.jobExists(request.getJobId(), true, jobExistsListener);
    }

    private void notifyListeners(String jobId, @Nullable AcknowledgedResponse ack, @Nullable Exception error) {
        synchronized (listenersByJobId) {
            List<ActionListener<AcknowledgedResponse>> listeners = listenersByJobId.remove(jobId);
            if (listeners == null) {
                logger.error("[{}] No deletion job listeners could be found", jobId);
                return;
            }
            for (ActionListener<AcknowledgedResponse> listener : listeners) {
                if (error != null) {
                    listener.onFailure(error);
                } else {
                    listener.onResponse(ack);
                }
            }
        }
    }

    private void normalDeleteJob(ParentTaskAssigningClient parentTaskClient, DeleteJobAction.Request request,
                                 ActionListener<AcknowledgedResponse> listener) {
        String jobId = request.getJobId();

        // We clean up the memory tracker on delete rather than close as close is not a master node action
        memoryTracker.removeAnomalyDetectorJob(jobId);

        // Step 4. When the job has been removed from the cluster state, return a response
        // -------
        CheckedConsumer<Boolean, Exception> apiResponseHandler = jobDeleted -> {
            if (jobDeleted) {
                logger.info("Job [" + jobId + "] deleted");
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DELETED));
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                listener.onResponse(AcknowledgedResponse.FALSE);
            }
        };

        // Step 3. When the physical storage has been deleted, delete the job config document
        // -------
        // Don't report an error if the document has already been deleted
        CheckedConsumer<Boolean, Exception> deleteJobStateHandler = response -> jobConfigProvider.deleteJob(jobId, false,
                ActionListener.wrap(
                        deleteResponse -> apiResponseHandler.accept(Boolean.TRUE),
                        listener::onFailure
                )
        );

        // Step 2. Remove the job from any calendars
        CheckedConsumer<Boolean, Exception> removeFromCalendarsHandler = response -> jobResultsProvider.removeJobFromCalendars(jobId,
                ActionListener.wrap(deleteJobStateHandler::accept, listener::onFailure));


        // Step 1. Delete the physical storage
        new JobDataDeleter(parentTaskClient, jobId).deleteJobDocuments(
            jobId, jobConfigProvider, indexNameExpressionResolver, clusterService.state(), removeFromCalendarsHandler, listener::onFailure);
    }

    private void forceDeleteJob(ParentTaskAssigningClient parentTaskClient, DeleteJobAction.Request request,
                                ActionListener<AcknowledgedResponse> listener) {

        logger.debug("Force deleting job [{}]", request.getJobId());

        final ClusterState state = clusterService.state();
        final String jobId = request.getJobId();

        // 3. Delete the job
        ActionListener<Boolean> removeTaskListener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean response) {
                normalDeleteJob(parentTaskClient, request, listener);
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                    normalDeleteJob(parentTaskClient, request, listener);
                } else {
                    listener.onFailure(e);
                }
            }
        };

        // 2. Cancel the persistent task. This closes the process gracefully so
        // the process should be killed first.
        ActionListener<KillProcessAction.Response> killJobListener = ActionListener.wrap(
                response -> removePersistentTask(request.getJobId(), state, removeTaskListener),
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ElasticsearchStatusException) {
                        // Killing the process marks the task as completed so it
                        // may have disappeared when we get here
                        removePersistentTask(request.getJobId(), state, removeTaskListener);
                    } else {
                        listener.onFailure(e);
                    }
                }
        );

        // 1. Kill the job's process
        killProcess(parentTaskClient, jobId, killJobListener);
    }

    private void killProcess(ParentTaskAssigningClient parentTaskClient, String jobId,
                             ActionListener<KillProcessAction.Response> listener) {
        KillProcessAction.Request killRequest = new KillProcessAction.Request(jobId);
        executeAsyncWithOrigin(parentTaskClient, ML_ORIGIN, KillProcessAction.INSTANCE, killRequest, listener);
    }

    private void removePersistentTask(String jobId, ClusterState currentState,
                                      ActionListener<Boolean> listener) {
        PersistentTasksCustomMetadata tasks = currentState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);

        PersistentTasksCustomMetadata.PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
        if (jobTask == null) {
            listener.onResponse(null);
        } else {
            persistentTasksService.sendRemoveRequest(jobTask.getId(), listener.delegateFailure((l, task) -> l.onResponse(Boolean.TRUE)));
        }
    }

    private void checkJobIsNotOpen(String jobId, ClusterState state) {
        PersistentTasksCustomMetadata tasks = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata.PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
        if (jobTask != null) {
            JobTaskState jobTaskState = (JobTaskState) jobTask.getState();
            throw ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] because the job is "
                    + ((jobTaskState == null) ? JobState.OPENING : jobTaskState.getState()));
        }
    }

    private void markJobAsDeletingIfNotUsed(String jobId, ActionListener<Boolean> listener) {

        datafeedConfigProvider.findDatafeedsForJobIds(Collections.singletonList(jobId), ActionListener.wrap(
                datafeedIds -> {
                    if (datafeedIds.isEmpty() == false) {
                        listener.onFailure(ExceptionsHelper.conflictStatusException("Cannot delete job [" + jobId + "] because datafeed ["
                                + datafeedIds.iterator().next() + "] refers to it"));
                        return;
                    }
                    jobConfigProvider.markJobAsDeleting(jobId, listener);
                },
                listener::onFailure
        ));
    }
}
