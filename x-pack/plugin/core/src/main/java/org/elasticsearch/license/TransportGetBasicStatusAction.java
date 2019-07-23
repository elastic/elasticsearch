/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportGetBasicStatusAction extends TransportMasterNodeReadAction<GetBasicStatusRequest, GetBasicStatusResponse> {

    @Inject
    public TransportGetBasicStatusAction(TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetBasicStatusAction.NAME, transportService, clusterService, threadPool, actionFilters,
                GetBasicStatusRequest::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetBasicStatusResponse read(StreamInput in) throws IOException {
        return new GetBasicStatusResponse(in);
    }

    @Override
    protected void masterOperation(Task task, GetBasicStatusRequest request, ClusterState state,
                                   ActionListener<GetBasicStatusResponse> listener) throws Exception {
        LicensesMetaData licensesMetaData = state.metaData().custom(LicensesMetaData.TYPE);
        if (licensesMetaData == null) {
            listener.onResponse(new GetBasicStatusResponse(true));
        } else {
            License license = licensesMetaData.getLicense();
            listener.onResponse(new GetBasicStatusResponse(license == null || license.type().equals("basic") == false));
        }

    }

    @Override
    protected ClusterBlockException checkBlock(GetBasicStatusRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
