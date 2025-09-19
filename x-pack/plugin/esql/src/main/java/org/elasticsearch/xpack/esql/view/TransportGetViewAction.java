/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;

public class TransportGetViewAction extends AcknowledgedTransportMasterNodeAction<GetViewAction.Request> {
    private final ViewService viewService;

    @Inject
    public TransportGetViewAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ViewService viewService
    ) {
        super(
            GetViewAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetViewAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.viewService = viewService;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetViewAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        View view = viewService.get(request.name());
        if (view == null) {
            listener.onResponse(AcknowledgedResponse.FALSE);
        } else {
            listener.onResponse(AcknowledgedResponse.TRUE);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetViewAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
