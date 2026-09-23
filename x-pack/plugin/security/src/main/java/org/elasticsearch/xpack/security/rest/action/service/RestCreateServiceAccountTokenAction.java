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
import org.elasticsearch.xpack.core.security.action.service.CreateUserManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.PUBLIC)
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
        final String namespace = request.param("namespace");
        String tokenName = request.param("name");
        if (Strings.isNullOrEmpty(tokenName)) {
            tokenName = "token_" + UUIDs.base64UUID();
        }
        final CreateServiceAccountTokenRequest createServiceAccountTokenRequest = new CreateServiceAccountTokenRequest(
            namespace,
            request.param("service"),
            tokenName
        );
        final String refreshPolicy = request.param("refresh");
        if (refreshPolicy != null) {
            createServiceAccountTokenRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(refreshPolicy));
        }

        // One route serves both kinds of account, and the reserved namespace is what tells them apart. Selecting the
        // action here, ahead of authorization, is what lets the privilege check judge the kind the caller asked for;
        // nothing downstream of that check may infer the kind from the namespace again.
        final var action = ServiceAccountSettings.BUILTIN_NAMESPACE.equalsIgnoreCase(namespace)
            ? CreateServiceAccountTokenAction.INSTANCE
            : CreateUserManagedServiceAccountTokenAction.INSTANCE;
        return channel -> client.execute(action, createServiceAccountTokenRequest, new RestToXContentListener<>(channel));
    }
}
