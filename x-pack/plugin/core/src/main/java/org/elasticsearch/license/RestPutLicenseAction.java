/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutLicenseAction extends BaseRestHandler {

    RestPutLicenseAction() {}

    @Override
    public List<Route> routes() {
        // TODO: remove POST endpoint?
        return List.of(
            Route.builder(POST, "/_license").replaces(POST, "/_xpack/license", RestApiVersion.V_7).build(),
            Route.builder(PUT, "/_license").replaces(PUT, "/_xpack/license", RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "put_license";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
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

        return channel -> client.execute(PutLicenseAction.INSTANCE, putLicenseRequest, new RestToXContentListener<>(channel));
    }

}
