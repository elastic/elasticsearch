/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.util.Date;
import java.util.function.Consumer;

public class TransportRevertModelSnapshotAction extends TransportMasterNodeAction<RevertModelSnapshotAction.Request,
        RevertModelSnapshotAction.Response> {

    private final Client client;
    private final JobManager jobManager;
    private final JobProvider jobProvider;
    private final JobDataCountsPersister jobDataCountsPersister;

    @Inject
    public TransportRevertModelSnapshotAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                              ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                              JobManager jobManager, JobProvider jobProvider,
                                              ClusterService clusterService, Client client, JobDataCountsPersister jobDataCountsPersister) {
        super(settings, RevertModelSnapshotAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, RevertModelSnapshotAction.Request::new);
        this.client = client;
        this.jobManager = jobManager;
        this.jobProvider = jobProvider;
        this.jobDataCountsPersister = jobDataCountsPersister;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected RevertModelSnapshotAction.Response newResponse() {
        return new RevertModelSnapshotAction.Response();
    }

    @Override
    protected void masterOperation(RevertModelSnapshotAction.Request request, ClusterState state,
                                   ActionListener<RevertModelSnapshotAction.Response> listener) throws Exception {
        logger.debug("Received request to revert to snapshot id '{}' for job '{}', deleting intervening results: {}",
                request.getSnapshotId(), request.getJobId(), request.getDeleteInterveningResults());

        Job job = JobManager.getJobOrThrowIfUnknown(request.getJobId(), clusterService.state());
        JobState jobState = jobManager.getJobState(job.getId());
        if (jobState.equals(JobState.CLOSED) == false) {
            throw ExceptionsHelper.conflictStatusException(Messages.getMessage(Messages.REST_JOB_NOT_CLOSED_REVERT));
        }

        getModelSnapshot(request, jobProvider, modelSnapshot -> {
            ActionListener<RevertModelSnapshotAction.Response> wrappedListener = listener;
            if (request.getDeleteInterveningResults()) {
                wrappedListener = wrapDeleteOldDataListener(wrappedListener, modelSnapshot, request.getJobId());
                wrappedListener = wrapRevertDataCountsListener(wrappedListener, modelSnapshot, request.getJobId());
            }
            jobManager.revertSnapshot(request, wrappedListener, modelSnapshot);
        }, listener::onFailure);
    }

    private void getModelSnapshot(RevertModelSnapshotAction.Request request, JobProvider provider, Consumer<ModelSnapshot> handler,
                                  Consumer<Exception> errorHandler) {
        logger.info("Reverting to snapshot '" + request.getSnapshotId() + "'");

        provider.getModelSnapshot(request.getJobId(), request.getSnapshotId(), modelSnapshot -> {
            if (modelSnapshot == null) {
                throw new ResourceNotFoundException(Messages.getMessage(Messages.REST_NO_SUCH_MODEL_SNAPSHOT, request.getSnapshotId(),
                        request.getJobId()));
            }
            handler.accept(modelSnapshot.result);
        }, errorHandler);
    }

    private ActionListener<RevertModelSnapshotAction.Response> wrapDeleteOldDataListener(
            ActionListener<RevertModelSnapshotAction.Response> listener,
            ModelSnapshot modelSnapshot, String jobId) {

        // If we need to delete buckets that occurred after the snapshot, we
        // wrap the listener with one that invokes the OldDataRemover on
        // acknowledged responses
        return ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                Date deleteAfter = modelSnapshot.getLatestResultTimeStamp();
                logger.debug("Removing intervening records: last record: " + deleteAfter + ", last result: "
                        + modelSnapshot.getLatestResultTimeStamp());

                logger.info("Deleting results after '" + deleteAfter + "'");

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
            }
        }, listener::onFailure);
    }

    private ActionListener<RevertModelSnapshotAction.Response> wrapRevertDataCountsListener(
            ActionListener<RevertModelSnapshotAction.Response> listener,
            ModelSnapshot modelSnapshot, String jobId) {


        return ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                jobProvider.dataCounts(jobId, counts -> {
                    counts.setLatestRecordTimeStamp(modelSnapshot.getLatestRecordTimeStamp());
                    jobDataCountsPersister.persistDataCounts(jobId, counts, new ActionListener<Boolean>() {
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
            }
        }, listener::onFailure);
    }

    @Override
    protected ClusterBlockException checkBlock(RevertModelSnapshotAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
