/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetServiceAccountAction extends SecurityBaseRestHandler {

    public RestGetServiceAccountAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_security/service"),
            new Route(GET, "/_security/service/{namespace}"),
            new Route(GET, "/_security/service/{namespace}/{service}")
        );
    }

    @Override
    public String getName() {
        return "xpack_security_get_service_account";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String namespace = request.param("namespace");
        final String serviceName = request.param("service");
        final GetServiceAccountRequest getServiceAccountRequest = new GetServiceAccountRequest(namespace, serviceName);
        return channel -> client.execute(GetServiceAccountAction.INSTANCE, getServiceAccountRequest,
            new RestToXContentListener<>(channel));
    }
}
