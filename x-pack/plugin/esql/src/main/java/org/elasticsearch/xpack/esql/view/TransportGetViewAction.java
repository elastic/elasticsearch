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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

public class TransportGetViewAction extends HandledTransportAction<GetViewAction.Request, GetViewAction.Response> {
    public static final ActionType<RemoteInfoResponse> TYPE = new ActionType<>(GetViewAction.NAME);
    private final ClusterViewService viewService;

    @Inject
    public TransportGetViewAction(TransportService transportService, ActionFilters actionFilters, ClusterViewService viewService) {
        super(GetViewAction.NAME, transportService, actionFilters, GetViewAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.viewService = viewService;
    }

    @Override
    protected void doExecute(Task task, GetViewAction.Request request, ActionListener<GetViewAction.Response> listener) {
        ProjectId projectId = viewService.getProjectId();
        TreeMap<String, View> views = new TreeMap<>();
        List<String> missing = new ArrayList<>();
        Collection<String> names = request.names();
        if (names.isEmpty()) {
            names = Collections.unmodifiableSet(viewService.list(projectId));
        }
        for (String name : names) {
            View view = viewService.get(projectId, name);
            if (view == null) {
                missing.add(name);
            } else {
                views.put(name, view);
            }
        }
        if (missing.isEmpty() == false) {
            listener.onFailure(new IllegalArgumentException("Views do not exist: " + String.join(", ", missing)));
        } else {
            listener.onResponse(new GetViewAction.Response(views));
        }
    }
}
