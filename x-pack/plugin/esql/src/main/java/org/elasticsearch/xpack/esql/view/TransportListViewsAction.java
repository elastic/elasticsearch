/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.LinkedHashMap;
import java.util.Map;

public class TransportListViewsAction extends HandledTransportAction<ListViewsAction.Request, ListViewsAction.Response> {
    public static final ActionType<RemoteInfoResponse> TYPE = new ActionType<>(ListViewsAction.NAME);
    private final ClusterViewService viewService;

    @Inject
    public TransportListViewsAction(TransportService transportService, ActionFilters actionFilters, ClusterViewService viewService) {
        super(ListViewsAction.NAME, transportService, actionFilters, ListViewsAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.viewService = viewService;
    }

    @Override
    protected void doExecute(Task task, ListViewsAction.Request request, ActionListener<ListViewsAction.Response> listener) {
        Map<String, View> views = new LinkedHashMap<>();
        for (String name : viewService.list()) {
            View view = viewService.get(name);
            if (view != null) {
                views.put(name, viewService.get(name));
            }
        }
        listener.onResponse(new ListViewsAction.Response(views));
    }
}
