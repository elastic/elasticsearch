/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportClearScrollAction extends HandledTransportAction<ClearScrollRequest, ClearScrollResponse> {

    public static final String NAME = "indices:data/read/scroll/clear";

    public static final ActionType<ClearScrollResponse> TYPE = new ActionType<>(NAME);
    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;

    @Inject
    public TransportClearScrollAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService
    ) {
        super(TYPE.name(), transportService, actionFilters, ClearScrollRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
    }

    @Override
    protected void doExecute(Task task, ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        Runnable runnable = new ClearScrollController(request, listener, clusterService.state().nodes(), logger, searchTransportService);
        runnable.run();
    }

}
