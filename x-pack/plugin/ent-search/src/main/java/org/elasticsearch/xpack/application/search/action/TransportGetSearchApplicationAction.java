/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;

public class TransportGetSearchApplicationAction extends SearchApplicationTransportAction<
    GetSearchApplicationAction.Request,
    GetSearchApplicationAction.Response> {

    @Inject
    public TransportGetSearchApplicationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry,
        BigArrays bigArrays,
        XPackLicenseState licenseState
    ) {
        super(
            GetSearchApplicationAction.NAME,
            transportService,
            actionFilters,
            GetSearchApplicationAction.Request::new,
            client,
            clusterService,
            namedWriteableRegistry,
            bigArrays,
            licenseState
        );
    }

    @Override
    protected void doExecute(GetSearchApplicationAction.Request request, ActionListener<GetSearchApplicationAction.Response> listener) {
        systemIndexService.getSearchApplication(request.getName(), listener.map(GetSearchApplicationAction.Response::new));
    }

}
