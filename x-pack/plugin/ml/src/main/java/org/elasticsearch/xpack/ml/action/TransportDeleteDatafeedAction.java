/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MlConfigMigrationEligibilityCheck;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobDataDeleter;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteDatafeedAction extends TransportMasterNodeAction<DeleteDatafeedAction.Request, AcknowledgedResponse> {

    private final Client client;
    private final DatafeedConfigProvider datafeedConfigProvider;
    private final ClusterService clusterService;
    private final PersistentTasksService persistentTasksService;
    private final MlConfigMigrationEligibilityCheck migrationEligibilityCheck;

    @Inject
    public TransportDeleteDatafeedAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver,
                                         Client client, PersistentTasksService persistentTasksService,
                                         NamedXContentRegistry xContentRegistry) {
        super(DeleteDatafeedAction.NAME, transportService, clusterService, threadPool, actionFilters,
                DeleteDatafeedAction.Request::new, indexNameExpressionResolver);
        this.client = client;
        this.datafeedConfigProvider = new DatafeedConfigProvider(client, xContentRegistry);
        this.persistentTasksService = persistentTasksService;
        this.clusterService = clusterService;
        this.migrationEligibilityCheck = new MlConfigMigrationEligibilityCheck(settings, clusterService);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(Task task, DeleteDatafeedAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {

        if (migrationEligibilityCheck.datafeedIsEligibleForMigration(request.getDatafeedId(), state)) {
            listener.onFailure(ExceptionsHelper.configHasNotBeenMigrated("delete datafeed", request.getDatafeedId()));
            return;
        }

        if (request.isForce()) {
            forceDeleteDatafeed(request, state, listener);
        } else {
            deleteDatafeedConfig(request, listener);
        }
    }

    private void forceDeleteDatafeed(DeleteDatafeedAction.Request request, ClusterState state,
                                     ActionListener<AcknowledgedResponse> listener) {
        ActionListener<Boolean> finalListener = ActionListener.wrap(
                response -> deleteDatafeedConfig(request, listener),
                listener::onFailure
        );

        ActionListener<IsolateDatafeedAction.Response> isolateDatafeedHandler = ActionListener.wrap(
                response -> removeDatafeedTask(request, state, finalListener),
                listener::onFailure
        );

        IsolateDatafeedAction.Request isolateDatafeedRequest = new IsolateDatafeedAction.Request(request.getDatafeedId());
        executeAsyncWithOrigin(client, ML_ORIGIN, IsolateDatafeedAction.INSTANCE, isolateDatafeedRequest, isolateDatafeedHandler);
    }

    private void removeDatafeedTask(DeleteDatafeedAction.Request request, ClusterState state, ActionListener<Boolean> listener) {
        PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(request.getDatafeedId(), tasks);
        if (datafeedTask == null) {
            listener.onResponse(true);
        } else {
            persistentTasksService.sendRemoveRequest(datafeedTask.getId(),
                    new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                        @Override
                        public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
                            listener.onResponse(Boolean.TRUE);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                                // the task has been removed in between
                                listener.onResponse(true);
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    });
        }
    }

    private void deleteDatafeedConfig(DeleteDatafeedAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        // Check datafeed is stopped
        PersistentTasksCustomMetaData tasks = clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (MlTasks.getDatafeedTask(request.getDatafeedId(), tasks) != null) {
            listener.onFailure(ExceptionsHelper.conflictStatusException(
                    Messages.getMessage(Messages.DATAFEED_CANNOT_DELETE_IN_CURRENT_STATE, request.getDatafeedId(), DatafeedState.STARTED)));
            return;
        }

        String datafeedId = request.getDatafeedId();

        datafeedConfigProvider.getDatafeedConfig(
            datafeedId,
            ActionListener.wrap(
                datafeedConfigBuilder -> {
                    String jobId = datafeedConfigBuilder.build().getJobId();
                    JobDataDeleter jobDataDeleter = new JobDataDeleter(client, jobId);
                    jobDataDeleter.deleteDatafeedTimingStats(
                        ActionListener.wrap(
                            unused1 -> {
                                datafeedConfigProvider.deleteDatafeedConfig(
                                    datafeedId,
                                    ActionListener.wrap(
                                        unused2 -> listener.onResponse(new AcknowledgedResponse(true)),
                                        listener::onFailure));
                            },
                            listener::onFailure));
                },
                listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDatafeedAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
