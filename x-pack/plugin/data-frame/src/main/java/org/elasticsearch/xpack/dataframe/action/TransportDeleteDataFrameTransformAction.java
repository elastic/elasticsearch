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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.dataframe.action.DeleteDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.DeleteDataFrameTransformAction.Request;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;

import java.io.IOException;

public class TransportDeleteDataFrameTransformAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private final DataFrameTransformsConfigManager transformsConfigManager;
    private final DataFrameAuditor auditor;

    @Inject
    public TransportDeleteDataFrameTransformAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool,
                                                   ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver,
                                                   DataFrameTransformsConfigManager transformsConfigManager, DataFrameAuditor auditor) {
        super(DeleteDataFrameTransformAction.NAME, transportService, clusterService, threadPool, actionFilters,
                Request::new, indexNameExpressionResolver);
        this.transformsConfigManager = transformsConfigManager;
        this.auditor = auditor;
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
    protected AcknowledgedResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected void masterOperation(Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
        PersistentTasksCustomMetaData pTasksMeta = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (pTasksMeta != null && pTasksMeta.getTask(request.getId()) != null) {
            listener.onFailure(new ElasticsearchStatusException("Cannot delete data frame [" + request.getId() +
                    "] as the task is running. Stop the task first", RestStatus.CONFLICT));
        } else {
            // Task is not running, delete the configuration document
            transformsConfigManager.deleteTransform(request.getId(), ActionListener.wrap(
                    r -> {
                        auditor.info(request.getId(), "Deleted data frame transform.");
                        listener.onResponse(new AcknowledgedResponse(r));
                    },
                    listener::onFailure));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
