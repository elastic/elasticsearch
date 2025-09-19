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

import java.util.Set;

public class TransportListViewsAction extends AcknowledgedTransportMasterNodeAction<ListViewsAction.Request> {
    private final ViewService viewService;

    @Inject
    public TransportListViewsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ViewService viewService
    ) {
        super(
            ListViewsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ListViewsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.viewService = viewService;
    }

    @Override
    protected void masterOperation(
        Task task,
        ListViewsAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        Set<String> view = viewService.list();
        if (view == null) {
            listener.onResponse(AcknowledgedResponse.FALSE);
        } else {
            listener.onResponse(AcknowledgedResponse.TRUE);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(ListViewsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
