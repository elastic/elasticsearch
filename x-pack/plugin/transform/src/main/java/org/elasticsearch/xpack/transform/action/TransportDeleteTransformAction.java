/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ClientHelper.executeWithHeadersAsync;

public class TransportDeleteTransformAction extends AcknowledgedTransportMasterNodeAction<Request> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteTransformAction.class);

    private final TransformConfigManager transformConfigManager;
    private final TransformAuditor auditor;
    private final Client client;

    @Inject
    public TransportDeleteTransformAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransformServices transformServices,
        Client client
    ) {
        super(
            DeleteTransformAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.transformConfigManager = transformServices.configManager();
        this.auditor = transformServices.auditor();
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        final boolean transformIsRunning = TransformTask.getTransformTask(request.getId(), state) != null;
        if (transformIsRunning && request.isForce() == false) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot delete transform [" + request.getId() + "] as the task is running. Stop the task first",
                    RestStatus.CONFLICT
                )
            );
            return;
        }

        // <3> Delete transform config
        ActionListener<AcknowledgedResponse> deleteDestIndexListener = ActionListener.wrap(
            unusedAcknowledgedResponse -> transformConfigManager.deleteTransform(request.getId(), ActionListener.wrap(r -> {
                logger.info("[{}] deleted transform", request.getId());
                auditor.info(request.getId(), "Deleted transform.");
                listener.onResponse(AcknowledgedResponse.of(r));
            }, listener::onFailure)),
            listener::onFailure
        );

        // <2> Delete destination index if requested
        ActionListener<StopTransformAction.Response> stopTransformActionListener = ActionListener.wrap(unusedStopResponse -> {
            if (request.isDeleteDestIndex()) {
                deleteDestinationIndex(parentTaskId, request.getId(), request.ackTimeout(), deleteDestIndexListener);
            } else {
                deleteDestIndexListener.onResponse(null);
            }
        }, listener::onFailure);

        // <1> Stop transform if it's currently running
        stopTransform(transformIsRunning, parentTaskId, request.getId(), request.ackTimeout(), stopTransformActionListener);
    }

    private void stopTransform(
        boolean transformIsRunning,
        TaskId parentTaskId,
        String transformId,
        TimeValue timeout,
        ActionListener<StopTransformAction.Response> listener
    ) {
        if (transformIsRunning == false) {
            listener.onResponse(null);
            return;
        }
        StopTransformAction.Request stopTransformRequest = new StopTransformAction.Request(transformId, true, true, timeout, true, false);
        stopTransformRequest.setParentTask(parentTaskId);
        executeAsyncWithOrigin(client, TRANSFORM_ORIGIN, StopTransformAction.INSTANCE, stopTransformRequest, listener);
    }

    private void deleteDestinationIndex(
        TaskId parentTaskId,
        String transformId,
        TimeValue timeout,
        ActionListener<AcknowledgedResponse> listener
    ) {
        // <3> Check if the error is "index not found" error. If so, just move on. The index is already deleted.
        ActionListener<AcknowledgedResponse> deleteDestIndexListener = ActionListener.wrap(listener::onResponse, e -> {
            if (e instanceof IndexNotFoundException) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                listener.onFailure(e);
            }
        });

        // <2> Delete destination index
        ActionListener<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>> getTransformConfigurationListener = ActionListener.wrap(
            transformConfigAndVersion -> {
                TransformConfig config = transformConfigAndVersion.v1();
                String destIndex = config.getDestination().getIndex();
                DeleteIndexRequest deleteDestIndexRequest = new DeleteIndexRequest(destIndex);
                deleteDestIndexRequest.ackTimeout(timeout);
                deleteDestIndexRequest.setParentTask(parentTaskId);
                executeWithHeadersAsync(
                    config.getHeaders(),
                    TRANSFORM_ORIGIN,
                    client,
                    TransportDeleteIndexAction.TYPE,
                    deleteDestIndexRequest,
                    deleteDestIndexListener
                );
            },
            listener::onFailure
        );

        // <1> Fetch transform configuration
        transformConfigManager.getTransformConfigurationForUpdate(transformId, getTransformConfigurationListener);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
