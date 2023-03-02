/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.entsearch.engine.Engine;
import org.elasticsearch.xpack.entsearch.engine.EngineIndexService;

public class TransportPutEngineAction extends EngineTransportAction<PutEngineAction.Request, PutEngineAction.Response> {

    private final EngineIndexService engineIndexService;

    @Inject
    public TransportPutEngineAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineIndexService engineIndexService,
        XPackLicenseState licenseState
    ) {
        super(PutEngineAction.NAME, transportService, actionFilters, PutEngineAction.Request::new, licenseState);
        this.engineIndexService = engineIndexService;
    }

    @Override
    protected void doExecute(PutEngineAction.Request request, ActionListener<PutEngineAction.Response> listener) {
        Engine engine = request.getEngine();
        boolean create = request.create();
        engineIndexService.putEngine(engine, create, listener.map(r -> new PutEngineAction.Response(r.getResult())));
    }
}
