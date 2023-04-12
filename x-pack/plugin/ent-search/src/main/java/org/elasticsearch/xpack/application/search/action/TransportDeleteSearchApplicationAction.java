/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteSearchApplicationAction extends SearchApplicationTransportAction<
    DeleteSearchApplicationAction.Request,
    AcknowledgedResponse> {

    @Inject
    public TransportDeleteSearchApplicationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry,
        BigArrays bigArrays,
        XPackLicenseState licenseState
    ) {
        super(
            DeleteSearchApplicationAction.NAME,
            transportService,
            actionFilters,
            DeleteSearchApplicationAction.Request::new,
            client,
            clusterService,
            namedWriteableRegistry,
            bigArrays,
            licenseState
        );
    }

    @Override
    protected void doExecute(DeleteSearchApplicationAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        String name = request.getName();
        systemIndexService.deleteSearchApplicationAndAlias(name, listener.map(v -> AcknowledgedResponse.TRUE));
    }
}
