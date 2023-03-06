/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.entsearch.utils.LicenseUtils;

public abstract class EngineTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends HandledTransportAction<
    Request,
    Response> {

    protected final XPackLicenseState licenseState;

    public EngineTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        XPackLicenseState licenseState
    ) {
        super(actionName, transportService, actionFilters, request);
        this.licenseState = licenseState;
    }

    @Override
    public final void doExecute(Task task, final Request request, ActionListener<Response> listener) {
        if (LicenseUtils.supportedLicense(licenseState)) {
            doExecute(request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(licenseState));
        }
    }

    protected abstract void doExecute(Request request, ActionListener<Response> listener);
}
