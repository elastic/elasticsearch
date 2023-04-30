/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.search.SearchApplicationIndexService;
import org.elasticsearch.xpack.application.utils.LicenseUtils;

/**
 * Base class for {@link org.elasticsearch.xpack.application.search.SearchApplication} related actions, that checks license features
 */
public abstract class SearchApplicationTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends
    HandledTransportAction<Request, Response> {

    protected final XPackLicenseState licenseState;

    protected final SearchApplicationIndexService systemIndexService;

    public SearchApplicationTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry,
        BigArrays bigArrays,
        XPackLicenseState licenseState
    ) {
        super(actionName, transportService, actionFilters, request);
        this.licenseState = licenseState;
        this.systemIndexService = new SearchApplicationIndexService(client, clusterService, namedWriteableRegistry, bigArrays);

    }

    @Override
    public final void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        LicenseUtils.runIfSupportedLicense(licenseState, () -> doExecute(request, listener), listener::onFailure);
    }

    protected abstract void doExecute(Request request, ActionListener<Response> listener);
}
