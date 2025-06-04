/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
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
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;

import java.util.Date;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportRevertModelSnapshotAction extends TransportMasterNodeAction<
    RevertModelSnapshotAction.Request,
    RevertModelSnapshotAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportRevertModelSnapshotAction.class);

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Client client;
    private final JobManager jobManager;
    private final JobResultsProvider jobResultsProvider;
    private final JobDataCountsPersister jobDataCountsPersister;

    @Inject
    public TransportRevertModelSnapshotAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        JobManager jobManager,
        JobResultsProvider jobResultsProvider,
        ClusterService clusterService,
        Client client,
        JobDataCountsPersister jobDataCountsPersister
    ) {
        super(
            RevertModelSnapshotAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RevertModelSnapshotAction.Request::new,
            RevertModelSnapshotAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
        this.jobManager = jobManager;
        this.jobResultsProvider = jobResultsProvider;
        this.jobDataCountsPersister = jobDataCountsPersister;
    }

    @Override
    protected void masterOperation(
        Task task,
        RevertModelSnapshotAction.Request request,
        ClusterState state,
        ActionListener<RevertModelSnapshotAction.Response> listener
    ) {
        final String jobId = request.getJobId();
        final TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());

        logger.debug(
            "Received request to revert to snapshot id '{}' for job '{}', deleting intervening results: {}",
            request.getSnapshotId(),
            jobId,
            request.getDeleteInterveningResults()
        );

        // 5. Revert the state
        ActionListener<Boolean> annotationsIndexUpdateListener = ActionListener.wrap(r -> {
            ActionListener<Job> jobListener = ActionListener.wrap(job -> {
                PersistentTasksCustomMetadata tasks = state.getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
                JobState jobState = MlTasks.getJobState(job.getId(), tasks);
                if (request.isForce() == false && jobState.equals(JobState.CLOSED) == false) {
                    listener.onFailure(ExceptionsHelper.conflictStatusException(Messages.getMessage(Messages.REST_JOB_NOT_CLOSED_REVERT)));
                    return;
                }
                if (MlTasks.getSnapshotUpgraderTask(jobId, request.getSnapshotId(), tasks) != null) {
                    listener.onFailure(
                        ExceptionsHelper.conflictStatusException(
                            "Cannot revert job [{}] to snapshot [{}] as it is being upgraded",
                            jobId,
                            request.getSnapshotId()
                        )
                    );
                    return;
                }
                isBlocked(job, request, ActionListener.wrap(isBlocked -> {
                    if (isBlocked) {
                        listener.onFailure(
                            ExceptionsHelper.conflictStatusException(
                                "cannot revert job [{}] to snapshot [{}] while it is blocked with [{}]",
                                jobId,
                                request.getSnapshotId(),
                                job.getBlocked().getReason()
                            )
                        );
                    } else {
                        jobManager.updateJobBlockReason(
                            jobId,
                            new Blocked(Blocked.Reason.REVERT, taskId),
                            ActionListener.wrap(aBoolean -> revertSnapshot(jobId, request, listener), listener::onFailure)
                        );
                    }
                }, listener::onFailure));
            }, listener::onFailure);

            jobManager.getJob(jobId, jobListener);
        }, listener::onFailure);

        // 4. Ensure the annotations index mappings are up to date
        ActionListener<Boolean> configMappingUpdateListener = ActionListener.wrap(
            r -> AnnotationIndex.createAnnotationsIndexIfNecessaryAndWaitForYellow(
                client,
                state,
                request.masterNodeTimeout(),
                annotationsIndexUpdateListener
            ),
            listener::onFailure
        );

        // 3. Ensure the config index mappings are up to date
        ActionListener<Boolean> jobExistsListener = ActionListener.wrap(
            r -> ElasticsearchMappings.addDocMappingIfMissing(
                MlConfigIndex.indexName(),
                MlConfigIndex::mapping,
                client,
                state,
                request.masterNodeTimeout(),
                configMappingUpdateListener,
                MlConfigIndex.CONFIG_INDEX_MAPPINGS_VERSION
            ),
            listener::onFailure
        );

        // 2. Verify the job exists
        ActionListener<Boolean> createStateIndexListener = ActionListener.wrap(
            r -> jobManager.jobExists(jobId, null, jobExistsListener),
            listener::onFailure
        );

        // 1. Verify/Create the state index and its alias exists
        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(
            client,
            state,
            indexNameExpressionResolver,
            request.masterNodeTimeout(),
            createStateIndexListener
        );
    }

    private void isBlocked(Job job, RevertModelSnapshotAction.Request request, ActionListener<Boolean> listener) {
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

            // If the request is forced, we will also wait for the existing task to finish
            // to give a chance to this request to be executed without returning an error.
            // This is particularly useful when a relocating job is calling revert.
            getTaskRequest.setWaitForCompletion(request.isForce());
            getTaskRequest.setTimeout(request.ackTimeout());

            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                TransportGetTaskAction.TYPE,
                getTaskRequest,
                ActionListener.wrap(r -> listener.onResponse(r.getTask().isCompleted() == false), e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                        listener.onResponse(false);
                    } else {
                        listener.onFailure(e);
                    }
                })
            );
        } else {
            listener.onResponse(true);
        }
    }

    private void revertSnapshot(
        String jobId,
        RevertModelSnapshotAction.Request request,
        ActionListener<RevertModelSnapshotAction.Response> listener
    ) {
        ActionListener<RevertModelSnapshotAction.Response> finalListener = ActionListener.wrap(
            r -> jobManager.updateJobBlockReason(
                jobId,
                Blocked.none(),
                ActionListener.wrap(aBoolean -> listener.onResponse(r), listener::onFailure)
            ),
            e -> jobManager.updateJobBlockReason(
                jobId,
                Blocked.none(),
                ActionListener.wrap(aBoolean -> listener.onFailure(e), listener::onFailure)
            )
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

    private static void getModelSnapshot(
        RevertModelSnapshotAction.Request request,
        JobResultsProvider provider,
        Consumer<ModelSnapshot> handler,
        Consumer<Exception> errorHandler
    ) {
        logger.info("[{}] Reverting to snapshot {}", request.getJobId(), request.getSnapshotId());

        if (ModelSnapshot.isTheEmptySnapshot(request.getSnapshotId())) {
            handler.accept(ModelSnapshot.emptySnapshot(request.getJobId()));
            return;
        }

        provider.getModelSnapshot(request.getJobId(), request.getSnapshotId(), true, modelSnapshot -> {
            if (modelSnapshot == null) {
                throw missingSnapshotException(request);
            }
            handler.accept(modelSnapshot.result);
        }, errorHandler);
    }

    private static ResourceNotFoundException missingSnapshotException(RevertModelSnapshotAction.Request request) {
        return new ResourceNotFoundException(
            Messages.getMessage(Messages.REST_NO_SUCH_MODEL_SNAPSHOT, request.getSnapshotId(), request.getJobId())
        );
    }

    private ActionListener<RevertModelSnapshotAction.Response> wrapDeleteOldAnnotationsListener(
        ActionListener<RevertModelSnapshotAction.Response> listener,
        ModelSnapshot modelSnapshot,
        String jobId
    ) {

        return ActionListener.wrap(response -> {
            Date deleteAfter = modelSnapshot.getLatestResultTimeStamp() == null ? new Date(0) : modelSnapshot.getLatestResultTimeStamp();
            logger.info("[{}] Removing intervening annotations after reverting model: deleting annotations after [{}]", jobId, deleteAfter);

            JobDataDeleter dataDeleter = new JobDataDeleter(client, jobId);
            Set<String> eventsToDelete = Set.of(
                // Because the results based on the delayed data are being deleted, the fact that the data was originally delayed is
                // not relevant
                Annotation.Event.DELAYED_DATA.toString(),
                // Because the model that changed is no longer in use as it has been rolled back to a time before those changes occurred
                Annotation.Event.MODEL_CHANGE.toString()
            );
            dataDeleter.deleteAnnotations(deleteAfter.getTime() + 1, null, eventsToDelete, listener.safeMap(r -> response));
        }, listener::onFailure);
    }

    private ActionListener<RevertModelSnapshotAction.Response> wrapDeleteOldDataListener(
        ActionListener<RevertModelSnapshotAction.Response> listener,
        ModelSnapshot modelSnapshot,
        String jobId
    ) {

        // If we need to delete buckets that occurred after the snapshot, we
        // wrap the listener with one that invokes the OldDataRemover on
        // acknowledged responses
        return ActionListener.wrap(response -> {
            Date deleteAfter = modelSnapshot.getLatestResultTimeStamp() == null ? new Date(0) : modelSnapshot.getLatestResultTimeStamp();
            logger.info("[{}] Removing intervening records after reverting model: deleting results after [{}]", jobId, deleteAfter);

            JobDataDeleter dataDeleter = new JobDataDeleter(client, jobId);
            dataDeleter.deleteResultsFromTime(deleteAfter.getTime() + 1, listener.safeMap(r -> response));
        }, listener::onFailure);
    }

    private ActionListener<RevertModelSnapshotAction.Response> wrapRevertDataCountsListener(
        ActionListener<RevertModelSnapshotAction.Response> listener,
        ModelSnapshot modelSnapshot,
        String jobId
    ) {
        return ActionListener.wrap(response -> jobResultsProvider.dataCounts(jobId, counts -> {
            counts.setLatestRecordTimeStamp(modelSnapshot.getLatestRecordTimeStamp());
            jobDataCountsPersister.persistDataCountsAsync(jobId, counts, listener.safeMap(r -> response));
        }, listener::onFailure), listener::onFailure);
    }

    @Override
    protected ClusterBlockException checkBlock(RevertModelSnapshotAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
