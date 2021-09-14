/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.saml.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

public abstract class IdpBaseRestHandler extends BaseRestHandler {

    private static License.OperationMode MINIMUM_ALLOWED_LICENSE = License.OperationMode.ENTERPRISE;

    protected final XPackLicenseState licenseState;

    protected IdpBaseRestHandler(XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }

    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        RestChannelConsumer consumer = innerPrepareRequest(request, client);
        if (isIdpFeatureAllowed()) {
            return consumer;
        } else {
            return channel -> channel.sendResponse(new BytesRestResponse(channel,
                LicenseUtils.newComplianceException("Identity Provider")));
        }
    }

    protected boolean isIdpFeatureAllowed() {
        return licenseState.isAllowedByLicense(MINIMUM_ALLOWED_LICENSE);
    }

    /**
     * Implementers should implement this method as they normally would for
     * {@link BaseRestHandler#prepareRequest(RestRequest, NodeClient)} and ensure that all request
     * parameters are consumed prior to returning a value.
     */
    protected abstract RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException;
}


