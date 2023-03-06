/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;

public abstract class EnterpriseSearchTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends
    HandledTransportAction<Request, Response> {

    protected final XPackLicenseState licenseState;

    public static final LicensedFeature.Momentary LICENSED_ENGINE_FEATURE = LicensedFeature.momentary(
        null,
        XPackField.ENTEPRISE_SEARCH,
        License.OperationMode.PLATINUM
    );

    public EnterpriseSearchTransportAction(
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
        if (LICENSED_ENGINE_FEATURE.check(licenseState)) {
            doExecute(request, listener);
        } else {
            listener.onFailure(newComplianceException(licenseState));
        }
    }

    protected abstract void doExecute(Request request, ActionListener<Response> listener);

    private static ElasticsearchSecurityException newComplianceException(XPackLicenseState licenseState) {
        String licenseStatus = licenseState.statusDescription();

        ElasticsearchSecurityException e = new ElasticsearchSecurityException(
            "Current license is non-compliant for engines. Current license is {}. "
                + "Engines require an active trial or platinum license.",
            RestStatus.FORBIDDEN,
            licenseStatus
        );
        return e;
    }
}
