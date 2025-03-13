/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteDatafeedAction extends AcknowledgedTransportMasterNodeAction<DeleteDatafeedAction.Request> {

    private final Client client;
    private final DatafeedManager datafeedManager;
    private final PersistentTasksService persistentTasksService;

    @Inject
    public TransportDeleteDatafeedAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        PersistentTasksService persistentTasksService,
        DatafeedManager datafeedManager
    ) {
        super(
            DeleteDatafeedAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDatafeedAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
        this.persistentTasksService = persistentTasksService;
        this.datafeedManager = datafeedManager;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDatafeedAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        if (request.isForce()) {
            forceDeleteDatafeed(request, state, listener);
        } else {
            datafeedManager.deleteDatafeed(request, state, listener);
        }
    }

    private void forceDeleteDatafeed(
        DeleteDatafeedAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        IsolateDatafeedAction.Request isolateDatafeedRequest = new IsolateDatafeedAction.Request(request.getDatafeedId());
        executeAsyncWithOrigin(
            client,
            ML_ORIGIN,
            IsolateDatafeedAction.INSTANCE,
            isolateDatafeedRequest,
            listener.<Boolean>delegateFailureAndWrap(
                // use clusterService.state() here so that the updated state without the task is available
                (l, response) -> datafeedManager.deleteDatafeed(request, clusterService.state(), l)
            ).delegateFailureAndWrap((l, response) -> removeDatafeedTask(request, state, l))
        );
    }

    private void removeDatafeedTask(DeleteDatafeedAction.Request request, ClusterState state, ActionListener<Boolean> listener) {
        PersistentTasksCustomMetadata tasks = state.getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(request.getDatafeedId(), tasks);
        if (datafeedTask == null) {
            listener.onResponse(true);
        } else {
            persistentTasksService.sendRemoveRequest(
                datafeedTask.getId(),
                MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
                new ActionListener<>() {
                    @Override
                    public void onResponse(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
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
                }
            );
        }
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDatafeedAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
