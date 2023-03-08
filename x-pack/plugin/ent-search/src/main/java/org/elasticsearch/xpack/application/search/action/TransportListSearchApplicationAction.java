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
import org.elasticsearch.xpack.core.action.util.PageParams;

public class TransportListSearchApplicationAction extends SearchApplicationTransportAction<
    ListSearchApplicationAction.Request,
    ListSearchApplicationAction.Response> {

    private final SearchApplicationIndexService indexService;

    @Inject
    public TransportListSearchApplicationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SearchApplicationIndexService indexService,
        XPackLicenseState licenseState
    ) {
        super(ListSearchApplicationAction.NAME, transportService, actionFilters, ListSearchApplicationAction.Request::new, licenseState);
        this.indexService = indexService;
    }

    @Override
    protected void doExecute(ListSearchApplicationAction.Request request, ActionListener<ListSearchApplicationAction.Response> listener) {
        final PageParams pageParams = request.pageParams();
        indexService.listSearchApplication(
            request.query(),
            pageParams.getFrom(),
            pageParams.getSize(),
            listener.map(r -> new ListSearchApplicationAction.Response(r.items(), r.totalResults()))
        );
    }
}
