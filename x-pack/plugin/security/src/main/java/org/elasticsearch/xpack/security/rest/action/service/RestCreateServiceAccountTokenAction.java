/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.INTERNAL)
public class RestCreateServiceAccountTokenAction extends SecurityBaseRestHandler {

    public RestCreateServiceAccountTokenAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_security/service/{namespace}/{service}/credential/token/{name}"),
            new Route(PUT, "/_security/service/{namespace}/{service}/credential/token/{name}"),
            new Route(POST, "/_security/service/{namespace}/{service}/credential/token")
        );
    }

    @Override
    public String getName() {
        return "xpack_security_create_service_account_token";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        String tokenName = request.param("name");
        if (Strings.isNullOrEmpty(tokenName)) {
            tokenName = "token_" + UUIDs.base64UUID();
        }
        final CreateServiceAccountTokenRequest createServiceAccountTokenRequest = new CreateServiceAccountTokenRequest(
            request.param("namespace"),
            request.param("service"),
            tokenName
        );
        final String refreshPolicy = request.param("refresh");
        if (refreshPolicy != null) {
            createServiceAccountTokenRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(refreshPolicy));
        }

        return channel -> client.execute(
            CreateServiceAccountTokenAction.INSTANCE,
            createServiceAccountTokenRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
