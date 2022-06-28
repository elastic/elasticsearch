/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.rest.XPackRestHandler;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutLicenseAction extends XPackRestHandler {

    RestPutLicenseAction() {}

    @Override
    public List<Route> routes() {
        return org.elasticsearch.core.List.of(
            // TODO: remove POST endpoint?
            Route.builder(POST, "/_license").replaces(POST, URI_BASE + "/license", RestApiVersion.V_7).build(),
            Route.builder(PUT, "/_license").replaces(PUT, URI_BASE + "/license", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "put_license";
    }

    @Override
    public RestChannelConsumer doPrepareRequest(final RestRequest request, final XPackClient client) {
        if (request.hasContent() == false) {
            throw new IllegalArgumentException("The license must be provided in the request body");
        }
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest();
        putLicenseRequest.license(request.content(), request.getXContentType());
        putLicenseRequest.acknowledge(request.paramAsBoolean("acknowledge", false));
        putLicenseRequest.timeout(request.paramAsTime("timeout", putLicenseRequest.timeout()));
        putLicenseRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putLicenseRequest.masterNodeTimeout()));

        if (License.LicenseType.isBasic(putLicenseRequest.license().type())) {
            throw new IllegalArgumentException(
                "Installing basic licenses is no longer allowed. Use the POST "
                    + "/_license/start_basic API to install a basic license that does not expire."
            );
        }

        return channel -> client.es()
            .admin()
            .cluster()
            .execute(PutLicenseAction.INSTANCE, putLicenseRequest, new RestToXContentListener<>(channel));
    }

}
