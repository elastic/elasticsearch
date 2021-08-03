/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.config.Blocked;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MlConfigMigrationEligibilityCheck;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.Date;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportRevertModelSnapshotAction extends TransportMasterNodeAction<RevertModelSnapshotAction.Request,
        RevertModelSnapshotAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportRevertModelSnapshotAction.class);

    private final Client client;
    private final JobManager jobManager;
    private final JobResultsProvider jobResultsProvider;
    private final JobDataCountsPersister jobDataCountsPersister;
    private final MlConfigMigrationEligibilityCheck migrationEligibilityCheck;

    @Inject
    public TransportRevertModelSnapshotAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                              ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                              JobManager jobManager, JobResultsProvider jobResultsProvider,
                                              ClusterService clusterService, Client client, JobDataCountsPersister jobDataCountsPersister) {
        super(RevertModelSnapshotAction.NAME, transportService, clusterService, threadPool, actionFilters,
                RevertModelSnapshotAction.Request::new, indexNameExpressionResolver, RevertModelSnapshotAction.Response::new,
                ThreadPool.Names.SAME);
        this.client = client;
        this.jobManager = jobManager;
        this.jobResultsProvider = jobResultsProvider;
        this.jobDataCountsPersister = jobDataCountsPersister;
        this.migrationEligibilityCheck = new MlConfigMigrationEligibilityCheck(settings, clusterService);
    }

    @Override
    protected void masterOperation(Task task, RevertModelSnapshotAction.Request request, ClusterState state,
                                   ActionListener<RevertModelSnapshotAction.Response> listener) {
        final String jobId = request.getJobId();
        final TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());

        if (migrationEligibilityCheck.jobIsEligibleForMigration(jobId, state)) {
            listener.onFailure(ExceptionsHelper.configHasNotBeenMigrated("revert model snapshot", jobId));
            return;
        }

        logger.debug("Received request to revert to snapshot id '{}' for job '{}', deleting intervening results: {}",
                request.getSnapshotId(), jobId, request.getDeleteInterveningResults());

        // 5. Revert the state
        ActionListener<Boolean> annotationsIndexUpdateListener = ActionListener.wrap(
            r -> {
                ActionListener<Job> jobListener = ActionListener.wrap(
                    job -> {
                        PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
                        JobState jobState = MlTasks.getJobState(job.getId(), tasks);
                        if (request.isForce() == false && jobState.equals(JobState.CLOSED) == false) {
                            listener.onFailure(ExceptionsHelper.conflictStatusException(
                                Messages.getMessage(Messages.REST_JOB_NOT_CLOSED_REVERT)));
                            return;
                        }
                        if (MlTasks.getSnapshotUpgraderTask(jobId, request.getSnapshotId(), tasks) != null) {
                            listener.onFailure(ExceptionsHelper.conflictStatusException(
                                "Cannot revert job [{}] to snapshot [{}] as it is being upgraded",
                                jobId,
                                request.getSnapshotId()
                            ));
                            return;
                        }
                        isBlocked(job, ActionListener.wrap(
                            isBlocked -> {
                                if (isBlocked) {
                                    listener.onFailure(ExceptionsHelper.conflictStatusException(
                                        "cannot revert job [{}] to snapshot [{}] while it is blocked with [{}]",
                                        jobId, request.getSnapshotId(), job.getBlocked().getReason())
                                    );
                                } else {
                                    jobManager.updateJobBlockReason(jobId, new Blocked(Blocked.Reason.REVERT, taskId), ActionListener.wrap(
                                        aBoolean -> revertSnapshot(jobId, request, listener),
                                        listener::onFailure
                                    ));
                                }
                            },
                            listener::onFailure
                        ));
                    },
                    listener::onFailure
                );

                jobManager.getJob(jobId, jobListener);
            },
            listener::onFailure
        );

        // 4. Ensure the annotations index mappings are up to date
        ActionListener<Boolean> configMappingUpdateListener = ActionListener.wrap(
            r -> AnnotationIndex.createAnnotationsIndexIfNecessaryAndWaitForYellow(client, state, request.masterNodeTimeout(),
                annotationsIndexUpdateListener),
            listener::onFailure
        );

        // 3. Ensure the config index mappings are up to date
        ActionListener<Boolean> jobExistsListener = ActionListener.wrap(
            r -> ElasticsearchMappings.addDocMappingIfMissing(MlConfigIndex.indexName(), MlConfigIndex::mapping,
                client, state, request.masterNodeTimeout(), configMappingUpdateListener),
            listener::onFailure
        );

        // 2. Verify the job exists
        ActionListener<Boolean> createStateIndexListener = ActionListener.wrap(
            r -> jobManager.jobExists(jobId, jobExistsListener),
            listener::onFailure
        );

        // 1. Verify/Create the state index and its alias exists
        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(client, state, indexNameExpressionResolver, request.masterNodeTimeout(),
            createStateIndexListener);
    }

    private void isBlocked(Job job, ActionListener<Boolean> listener) {
        if (job.getBlocked().getReason() == Blocked.Reason.NONE) {
            listener.onResponse(false);
            return;
        }
        if (job.getBlocked().getReason() == Blocked.Reason.REVERT) {
            // If another revert is called but there is a revert task running
            // we do not allow it to run. However, if the job got stuck with
            // a block on revert, it means the node that was running the previous
            // revert failed. So, we allow a revert to run if the task has completed
            // in order to complete and eventually unblock the job.
            GetTaskRequest getTaskRequest = new GetTaskRequest();
            getTaskRequest.setTaskId(job.getBlocked().getTaskId());
            executeAsyncWithOrigin(client, ML_ORIGIN, GetTaskAction.INSTANCE, getTaskRequest, ActionListener.wrap(
                r -> listener.onResponse(r.getTask().isCompleted() == false),
                e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                        listener.onResponse(false);
                    } else {
                        listener.onFailure(e);
                    }
                }
            ));
        } else {
            listener.onResponse(true);
        }
    }

    private void revertSnapshot(String jobId, RevertModelSnapshotAction.Request request,
                                ActionListener<RevertModelSnapshotAction.Response> listener) {
        ActionListener<RevertModelSnapshotAction.Response> finalListener = ActionListener.wrap(
            r -> jobManager.updateJobBlockReason(jobId, Blocked.none(), ActionListener.wrap(
                    aBoolean -> listener.onResponse(r),
                    listener::onFailure
                ))
            , e -> jobManager.updateJobBlockReason(jobId, Blocked.none(), ActionListener.wrap(
                aBoolean -> listener.onFailure(e),
                listener::onFailure
            ))
        );

        getModelSnapshot(request, jobResultsProvider, modelSnapshot -> {
            ActionListener<RevertModelSnapshotAction.Response> wrappedListener = finalListener;
            if (request.getDeleteInterveningResults()) {
                wrappedListener = wrapDeleteOldAnnotationsListener(wrappedListener, modelSnapshot, jobId);
                wrappedListener = wrapDeleteOldDataListener(wrappedListener, modelSnapshot, jobId);
                wrappedListener = wrapRevertDataCountsListener(wrappedListener, modelSnapshot, jobId);
            }
            jobManager.revertSnapshot(request, wrappedListener, modelSnapshot);
        }, listener::onFailure);
    }

    private void getModelSnapshot(RevertModelSnapshotAction.Request request, JobResultsProvider provider, Consumer<ModelSnapshot> handler,
                                  Consumer<Exception> errorHandler) {
        logger.info("Reverting to snapshot '" + request.getSnapshotId() + "'");

        if (ModelSnapshot.isTheEmptySnapshot(request.getSnapshotId())) {
            handler.accept(ModelSnapshot.emptySnapshot(request.getJobId()));
            return;
        }

        provider.getModelSnapshot(request.getJobId(), request.getSnapshotId(), modelSnapshot -> {
            if (modelSnapshot == null) {
                throw missingSnapshotException(request);
            }
            handler.accept(modelSnapshot.result);
        }, errorHandler);
    }

    private static ResourceNotFoundException missingSnapshotException(RevertModelSnapshotAction.Request request) {
        return new ResourceNotFoundException(Messages.getMessage(Messages.REST_NO_SUCH_MODEL_SNAPSHOT, request.getSnapshotId(),
            request.getJobId()));
    }

    private ActionListener<RevertModelSnapshotAction.Response> wrapDeleteOldAnnotationsListener(
            ActionListener<RevertModelSnapshotAction.Response> listener,
            ModelSnapshot modelSnapshot,
            String jobId) {

        return ActionListener.wrap(response -> {
            Date deleteAfter = modelSnapshot.getLatestResultTimeStamp() == null ? new Date(0) : modelSnapshot.getLatestResultTimeStamp();
            logger.info("[{}] Removing intervening annotations after reverting model: deleting annotations after [{}]", jobId, deleteAfter);

            JobDataDeleter dataDeleter = new JobDataDeleter(client, jobId);
            Set<String> eventsToDelete =
                Set.of(
                    // Because the results based on the delayed data are being deleted, the fact that the data was originally delayed is
                    // not relevant
                    Annotation.Event.DELAYED_DATA.toString(),
                    // Because the model that changed is no longer in use as it has been rolled back to a time before those changes occurred
                    Annotation.Event.MODEL_CHANGE.toString());
            dataDeleter.deleteAnnotations(deleteAfter.getTime() + 1, null, eventsToDelete,
                    listener.delegateFailure((l, r) -> l.onResponse(response)));
        }, listener::onFailure);
    }

    private ActionListener<RevertModelSnapshotAction.Response> wrapDeleteOldDataListener(
            ActionListener<RevertModelSnapshotAction.Response> listener,
            ModelSnapshot modelSnapshot,
            String jobId) {

        // If we need to delete buckets that occurred after the snapshot, we
        // wrap the listener with one that invokes the OldDataRemover on
        // acknowledged responses
        return ActionListener.wrap(response -> {
            Date deleteAfter = modelSnapshot.getLatestResultTimeStamp() == null ? new Date(0) : modelSnapshot.getLatestResultTimeStamp();
            logger.info("[{}] Removing intervening records after reverting model: deleting results after [{}]", jobId, deleteAfter);

            JobDataDeleter dataDeleter = new JobDataDeleter(client, jobId);
            dataDeleter.deleteResultsFromTime(deleteAfter.getTime() + 1, listener.delegateFailure((l, r) -> l.onResponse(response)));
        }, listener::onFailure);
    }

    private ActionListener<RevertModelSnapshotAction.Response> wrapRevertDataCountsListener(
            ActionListener<RevertModelSnapshotAction.Response> listener,
            ModelSnapshot modelSnapshot,
            String jobId) {

        return ActionListener.wrap(response -> jobResultsProvider.dataCounts(jobId, counts -> {
            counts.setLatestRecordTimeStamp(modelSnapshot.getLatestRecordTimeStamp());
            jobDataCountsPersister.persistDataCountsAsync(jobId, counts, listener.delegateFailure((l, r) -> l.onResponse(response)));
        }, listener::onFailure), listener::onFailure);
    }

    @Override
    protected ClusterBlockException checkBlock(RevertModelSnapshotAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
