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
import org.elasticsearch.xpack.entsearch.engine.Engine;
import org.elasticsearch.xpack.entsearch.engine.EngineIndexService;

public class TransportPutEngineAction extends HandledTransportAction<PutEngineAction.Request, PutEngineAction.Response> {

    private final EngineIndexService engineIndexService;

    @Inject
    public TransportPutEngineAction(TransportService transportService, ActionFilters actionFilters, EngineIndexService engineIndexService) {
        super(PutEngineAction.NAME, transportService, actionFilters, PutEngineAction.Request::new);
        this.engineIndexService = engineIndexService;
    }

    @Override
    protected void doExecute(Task task, PutEngineAction.Request request, ActionListener<PutEngineAction.Response> listener) {
        Engine engine = request.getEngine();
        engineIndexService.putEngine(engine, listener.map(r -> new PutEngineAction.Response(r.getResult())));
    }
}
