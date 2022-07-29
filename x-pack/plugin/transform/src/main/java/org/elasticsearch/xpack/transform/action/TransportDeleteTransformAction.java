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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

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
            ThreadPool.Names.SAME
        );
        this.transformConfigManager = transformServices.getConfigManager();
        this.auditor = transformServices.getAuditor();
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
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

        ActionListener<StopTransformAction.Response> stopTransformActionListener = ActionListener.wrap(
            unusedStopResponse -> transformConfigManager.deleteTransform(request.getId(), ActionListener.wrap(r -> {
                logger.debug("[{}] deleted transform", request.getId());
                auditor.info(request.getId(), "Deleted transform.");
                listener.onResponse(AcknowledgedResponse.of(r));
            }, listener::onFailure)),
            listener::onFailure
        );

        if (transformIsRunning == false) {
            stopTransformActionListener.onResponse(null);
            return;
        }
        StopTransformAction.Request stopTransformRequest = new StopTransformAction.Request(request.getId(), true, true, null, true, false);
        executeAsyncWithOrigin(client, TRANSFORM_ORIGIN, StopTransformAction.INSTANCE, stopTransformRequest, stopTransformActionListener);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
