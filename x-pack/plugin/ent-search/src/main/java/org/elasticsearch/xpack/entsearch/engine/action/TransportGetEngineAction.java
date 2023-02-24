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
import org.elasticsearch.xpack.entsearch.engine.EngineIndexService;

public class TransportGetEngineAction extends HandledTransportAction<GetEngineAction.Request, GetEngineAction.Response> {

    private final EngineIndexService engineIndexService;

    @Inject
    public TransportGetEngineAction(TransportService transportService, ActionFilters actionFilters, EngineIndexService engineIndexService) {
        super(GetEngineAction.NAME, transportService, actionFilters, GetEngineAction.Request::new);
        this.engineIndexService = engineIndexService;
    }

    @Override
    protected void doExecute(Task task, GetEngineAction.Request request, ActionListener<GetEngineAction.Response> listener) {
        engineIndexService.getEngine(request.getEngineId(), listener.map(GetEngineAction.Response::new));
    }

}
