/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ElasticsearchStatusException;
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
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.dataframe.action.DeleteDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.DeleteDataFrameTransformAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.StopDataFrameTransformAction;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ClientHelper.DATA_FRAME_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteDataFrameTransformAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private final DataFrameTransformsConfigManager transformsConfigManager;
    private final DataFrameAuditor auditor;
    private final Client client;

    @Inject
    public TransportDeleteDataFrameTransformAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool,
                                                   ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver,
                                                   DataFrameTransformsConfigManager transformsConfigManager, DataFrameAuditor auditor,
                                                   Client client) {
        super(DeleteDataFrameTransformAction.NAME, transportService, clusterService, threadPool, actionFilters,
                Request::new, indexNameExpressionResolver);
        this.transformsConfigManager = transformsConfigManager;
        this.auditor = auditor;
        this.client = client;
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
    protected void masterOperation(Task task, Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        final PersistentTasksCustomMetaData pTasksMeta = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (pTasksMeta != null && pTasksMeta.getTask(request.getId()) != null && request.isForce() == false) {
            listener.onFailure(new ElasticsearchStatusException("Cannot delete data frame [" + request.getId() +
                    "] as the task is running. Stop the task first", RestStatus.CONFLICT));
        } else {
            ActionListener<Void> stopTransformActionListener = ActionListener.wrap(
                stopResponse -> transformsConfigManager.deleteTransform(request.getId(),
                    ActionListener.wrap(
                        r -> {
                            auditor.info(request.getId(), "Deleted data frame transform.");
                            listener.onResponse(new AcknowledgedResponse(r));
                        },
                        listener::onFailure)),
                listener::onFailure
            );

            if (pTasksMeta != null && pTasksMeta.getTask(request.getId()) != null) {
                executeAsyncWithOrigin(client,
                    DATA_FRAME_ORIGIN,
                    StopDataFrameTransformAction.INSTANCE,
                    new StopDataFrameTransformAction.Request(request.getId(), true, true, null, true),
                    ActionListener.wrap(
                        r -> stopTransformActionListener.onResponse(null),
                        stopTransformActionListener::onFailure));
            } else {
                stopTransformActionListener.onResponse(null);
            }
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
