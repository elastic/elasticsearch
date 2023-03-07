/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.entsearch.engine.EngineIndexService;

public class TransportDeleteEngineAction extends EngineTransportAction<DeleteEngineAction.Request, AcknowledgedResponse> {

    private final EngineIndexService engineIndexService;

    @Inject
    public TransportDeleteEngineAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineIndexService engineIndexService,
        XPackLicenseState licenseState
    ) {
        super(DeleteEngineAction.NAME, transportService, actionFilters, DeleteEngineAction.Request::new, licenseState);
        this.engineIndexService = engineIndexService;
    }

    @Override
    protected void doExecute(DeleteEngineAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        String engineId = request.getEngineId();
        engineIndexService.deleteEngineAndAlias(engineId, listener.map(v -> AcknowledgedResponse.TRUE));
    }
}
