/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.search.SearchApplication;
import org.elasticsearch.xpack.application.search.SearchApplicationIndexService;

public class TransportPutSearchApplicationAction extends SearchApplicationTransportAction<
    PutSearchApplicationAction.Request,
    PutSearchApplicationAction.Response> {

    private final SearchApplicationIndexService indexService;

    @Inject
    public TransportPutSearchApplicationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SearchApplicationIndexService indexService,
        XPackLicenseState licenseState
    ) {
        super(PutSearchApplicationAction.NAME, transportService, actionFilters, PutSearchApplicationAction.Request::new, licenseState);
        this.indexService = indexService;
    }

    @Override
    protected void doExecute(PutSearchApplicationAction.Request request, ActionListener<PutSearchApplicationAction.Response> listener) {
        SearchApplication app = request.getSearchApplication();
        boolean create = request.create();
        indexService.putSearchApplication(app, create, listener.map(r -> new PutSearchApplicationAction.Response(r.getResult())));
    }
}
