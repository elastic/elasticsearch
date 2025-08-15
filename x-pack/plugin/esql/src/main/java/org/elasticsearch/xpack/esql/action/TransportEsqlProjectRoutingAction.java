/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportEsqlProjectRoutingAction extends TransportAction<EsqlProjectRoutingRequest, EsqlProjectRoutingResponse> {
    @Inject
    public TransportEsqlProjectRoutingAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(EsqlProjectRoutingAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    @Override
    protected void doExecute(Task task, EsqlProjectRoutingRequest request, ActionListener<EsqlProjectRoutingResponse> listener) {
        // whatever fancy ES|QL needs to happen here can happen here
        listener.onResponse(new EsqlProjectRoutingResponse(List.of("project1", "project2", "project3")));
    }
}
