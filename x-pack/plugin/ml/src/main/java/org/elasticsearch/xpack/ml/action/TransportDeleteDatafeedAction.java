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
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteDatafeedAction extends TransportMasterNodeAction<DeleteDatafeedAction.Request, AcknowledgedResponse> {

    private Client client;
    private PersistentTasksService persistentTasksService;

    @Inject
    public TransportDeleteDatafeedAction(TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver,
                                         Client client, PersistentTasksService persistentTasksService) {
        super(DeleteDatafeedAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, DeleteDatafeedAction.Request::new);
        this.client = client;
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(DeleteDatafeedAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        if (request.isForce()) {
            forceDeleteDatafeed(request, state, listener);
        } else {
            deleteDatafeedFromMetadata(request, listener);
        }
    }

    private void forceDeleteDatafeed(DeleteDatafeedAction.Request request, ClusterState state,
                                     ActionListener<AcknowledgedResponse> listener) {
        ActionListener<Boolean> finalListener = ActionListener.wrap(
                response -> deleteDatafeedFromMetadata(request, listener),
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
                            if (e instanceof ResourceNotFoundException) {
                                // the task has been removed in between
                                listener.onResponse(true);
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    });
        }
    }

    private void deleteDatafeedFromMetadata(DeleteDatafeedAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("delete-datafeed-" + request.getDatafeedId(),
                new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

                    @Override
                    protected AcknowledgedResponse newResponse(boolean acknowledged) {
                        return new AcknowledgedResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
                        MlMetadata currentMetadata = MlMetadata.getMlMetadata(currentState);
                        PersistentTasksCustomMetaData persistentTasks =
                                currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                        MlMetadata newMetadata = new MlMetadata.Builder(currentMetadata)
                                .removeDatafeed(request.getDatafeedId(), persistentTasks).build();
                        return ClusterState.builder(currentState).metaData(
                                MetaData.builder(currentState.getMetaData()).putCustom(MlMetadata.TYPE, newMetadata).build())
                                .build();
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDatafeedAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
