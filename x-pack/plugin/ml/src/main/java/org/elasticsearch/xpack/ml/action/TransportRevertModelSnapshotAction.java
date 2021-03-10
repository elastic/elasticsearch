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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
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

        if (migrationEligibilityCheck.jobIsEligibleForMigration(jobId, state)) {
            listener.onFailure(ExceptionsHelper.configHasNotBeenMigrated("revert model snapshot", jobId));
            return;
        }

        logger.debug("Received request to revert to snapshot id '{}' for job '{}', deleting intervening results: {}",
                request.getSnapshotId(), jobId, request.getDeleteInterveningResults());

        // 4. Revert the state
        ActionListener<Boolean> configMappingUpdateListener = ActionListener.wrap(
            r -> {
                PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
                JobState jobState = MlTasks.getJobState(jobId, tasks);

                if (request.isForce() == false && jobState.equals(JobState.CLOSED) == false) {
                    listener.onFailure(ExceptionsHelper.conflictStatusException(Messages.getMessage(Messages.REST_JOB_NOT_CLOSED_REVERT)));
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

                getModelSnapshot(request, jobResultsProvider, modelSnapshot -> {
                    ActionListener<RevertModelSnapshotAction.Response> wrappedListener = listener;
                    if (request.getDeleteInterveningResults()) {
                        wrappedListener = wrapDeleteOldAnnotationsListener(wrappedListener, modelSnapshot, jobId);
                        wrappedListener = wrapDeleteOldDataListener(wrappedListener, modelSnapshot, jobId);
                        wrappedListener = wrapRevertDataCountsListener(wrappedListener, modelSnapshot, jobId);
                    }
                    jobManager.revertSnapshot(request, wrappedListener, modelSnapshot);
                }, listener::onFailure);
            },
            listener::onFailure
        );

        // 3. Ensure the config index mappings are up to date
        ActionListener<Boolean> jobExistsListener = ActionListener.wrap(
            r -> ElasticsearchMappings.addDocMappingIfMissing(MlConfigIndex.indexName(), MlConfigIndex::mapping,
                client, state, configMappingUpdateListener),
            listener::onFailure
        );

        // 2. Verify the job exists
        ActionListener<Boolean> createStateIndexListener = ActionListener.wrap(
            r -> jobManager.jobExists(jobId, jobExistsListener),
            listener::onFailure
        );

        // 1. Verify/Create the state index and its alias exists
        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(client, state, indexNameExpressionResolver, createStateIndexListener);
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
            dataDeleter.deleteAnnotationsFromTime(deleteAfter.getTime() + 1, eventsToDelete, new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean success) {
                    listener.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
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
            dataDeleter.deleteResultsFromTime(deleteAfter.getTime() + 1, new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean success) {
                    listener.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }, listener::onFailure);
    }

    private ActionListener<RevertModelSnapshotAction.Response> wrapRevertDataCountsListener(
            ActionListener<RevertModelSnapshotAction.Response> listener,
            ModelSnapshot modelSnapshot,
            String jobId) {

        return ActionListener.wrap(response -> {
            jobResultsProvider.dataCounts(jobId, counts -> {
                counts.setLatestRecordTimeStamp(modelSnapshot.getLatestRecordTimeStamp());
                jobDataCountsPersister.persistDataCountsAsync(jobId, counts, new ActionListener<Boolean>() {
                    @Override
                    public void onResponse(Boolean aBoolean) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            }, listener::onFailure);
        }, listener::onFailure);
    }

    @Override
    protected ClusterBlockException checkBlock(RevertModelSnapshotAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
