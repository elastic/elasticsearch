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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsDeleter;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * The action is a master node action to ensure it reads an up-to-date cluster
 * state in order to determine whether there is a persistent task for the analytics
 * to delete.
 */
public class TransportDeleteDataFrameAnalyticsAction extends AcknowledgedTransportMasterNodeAction<DeleteDataFrameAnalyticsAction.Request> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteDataFrameAnalyticsAction.class);

    private final Client client;
    private final MlMemoryTracker memoryTracker;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final DataFrameAnalyticsAuditor auditor;

    @Inject
    public TransportDeleteDataFrameAnalyticsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        MlMemoryTracker memoryTracker,
        DataFrameAnalyticsConfigProvider configProvider,
        DataFrameAnalyticsAuditor auditor
    ) {
        super(
            DeleteDataFrameAnalyticsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDataFrameAnalyticsAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
        );
        this.client = client;
        this.memoryTracker = memoryTracker;
        this.configProvider = configProvider;
        this.auditor = Objects.requireNonNull(auditor);
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDataFrameAnalyticsAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
        ParentTaskAssigningClient parentTaskClient = new ParentTaskAssigningClient(client, taskId);

        if (request.isForce()) {
            forceDelete(parentTaskClient, request, listener);
        } else {
            normalDelete(parentTaskClient, state, request, listener);
        }
    }

    private void forceDelete(
        ParentTaskAssigningClient parentTaskClient,
        DeleteDataFrameAnalyticsAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        logger.debug("[{}] Force deleting data frame analytics job", request.getId());

        ActionListener<StopDataFrameAnalyticsAction.Response> stopListener = ActionListener.wrap(
            stopResponse -> normalDelete(parentTaskClient, clusterService.state(), request, listener),
            listener::onFailure
        );

        stopJob(parentTaskClient, request, stopListener);
    }

    private void stopJob(
        ParentTaskAssigningClient parentTaskClient,
        DeleteDataFrameAnalyticsAction.Request request,
        ActionListener<StopDataFrameAnalyticsAction.Response> listener
    ) {
        // We first try to stop the job normally. Normal stop returns after the job was stopped.
        // If that fails then we proceed to force stopping which returns as soon as the persistent task is removed.
        // If we just did force stopping, then there is a chance we proceed to delete the config while it's
        // still used from the running task which results in logging errors.

        StopDataFrameAnalyticsAction.Request stopRequest = new StopDataFrameAnalyticsAction.Request(request.getId());
        stopRequest.setTimeout(request.timeout());

        ActionListener<StopDataFrameAnalyticsAction.Response> normalStopListener = ActionListener.wrap(
            listener::onResponse,
            normalStopFailure -> {
                stopRequest.setForce(true);
                executeAsyncWithOrigin(
                    parentTaskClient,
                    ML_ORIGIN,
                    StopDataFrameAnalyticsAction.INSTANCE,
                    stopRequest,
                    ActionListener.wrap(listener::onResponse, forceStopFailure -> {
                        logger.error(new ParameterizedMessage("[{}] Failed to stop normally", request.getId()), normalStopFailure);
                        logger.error(new ParameterizedMessage("[{}] Failed to stop forcefully", request.getId()), forceStopFailure);
                        listener.onFailure(forceStopFailure);
                    })
                );
            }
        );

        executeAsyncWithOrigin(parentTaskClient, ML_ORIGIN, StopDataFrameAnalyticsAction.INSTANCE, stopRequest, normalStopListener);
    }

    private void normalDelete(
        ParentTaskAssigningClient parentTaskClient,
        ClusterState state,
        DeleteDataFrameAnalyticsAction.Request request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        String id = request.getId();
        PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        DataFrameAnalyticsState taskState = MlTasks.getDataFrameAnalyticsState(id, tasks);
        if (taskState != DataFrameAnalyticsState.STOPPED) {
            listener.onFailure(
                ExceptionsHelper.conflictStatusException("Cannot delete data frame analytics [{}] while its status is [{}]", id, taskState)
            );
            return;
        }

        // We clean up the memory tracker on delete because there is no stop; the task stops by itself
        memoryTracker.removeDataFrameAnalyticsJob(id);

        configProvider.get(id, ActionListener.wrap(config -> {
            DataFrameAnalyticsDeleter deleter = new DataFrameAnalyticsDeleter(parentTaskClient, auditor);
            deleter.deleteAllDocuments(config, request.timeout(), listener);
        }, listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDataFrameAnalyticsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
