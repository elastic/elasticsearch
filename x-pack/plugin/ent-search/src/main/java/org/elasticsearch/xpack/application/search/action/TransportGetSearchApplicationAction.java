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
import org.elasticsearch.xpack.application.search.SearchApplicationIndexService;

public class TransportGetSearchApplicationAction extends SearchApplicationTransportAction<
    GetSearchApplicationAction.Request,
    GetSearchApplicationAction.Response> {

    private final SearchApplicationIndexService indexService;

    @Inject
    public TransportGetSearchApplicationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SearchApplicationIndexService indexService,
        XPackLicenseState licenseState
    ) {
        super(GetSearchApplicationAction.NAME, transportService, actionFilters, GetSearchApplicationAction.Request::new, licenseState);
        this.indexService = indexService;
    }

    @Override
    protected void doExecute(GetSearchApplicationAction.Request request, ActionListener<GetSearchApplicationAction.Response> listener) {
        indexService.getSearchApplication(request.getName(), listener.map(GetSearchApplicationAction.Response::new));
    }

}
