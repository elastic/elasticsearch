/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.entsearch.engine.EngineIndexService;

public class TransportListEnginesAction extends HandledTransportAction<ListEnginesAction.Request, ListEnginesAction.Response> {

    private final EngineIndexService engineIndexService;

    @Inject
    public TransportListEnginesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineIndexService engineIndexService
    ) {
        super(ListEnginesAction.NAME, transportService, actionFilters, ListEnginesAction.Request::new);
        this.engineIndexService = engineIndexService;
    }

    @Override
    protected void doExecute(Task task, ListEnginesAction.Request request, ActionListener<ListEnginesAction.Response> listener) {
        final PageParams pageParams = request.pageParams();
        engineIndexService.listEngines(
            request.query(),
            pageParams.getFrom(),
            pageParams.getSize(),
            listener.map(r -> new ListEnginesAction.Response(r.engineListItems(), r.totalResults()))
        );
    }
}
