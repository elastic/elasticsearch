/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.service.PutServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.PutServiceAccountRequest;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.INTERNAL)
public class RestPutServiceAccountAction extends SecurityBaseRestHandler {

    private static final RoleDescriptor.Parser ROLE_DESCRIPTOR_PARSER = RoleDescriptor.parserBuilder().build();

    public RestPutServiceAccountAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(PUT, "/_security/service/{namespace}/{service}"),
            new Route(POST, "/_security/service/{namespace}/{service}")
        );
    }

    @Override
    public String getName() {
        return "xpack_security_put_service_account";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String namespace = request.param("namespace");
        final String serviceName = request.param("service");
        final String principal = namespace + "/" + serviceName;

        final RoleDescriptor roleDescriptor;
        try (XContentParser parser = request.contentParser()) {
            roleDescriptor = ROLE_DESCRIPTOR_PARSER.parse(principal, parser, false);
        }

        final PutServiceAccountRequest putRequest = new PutServiceAccountRequest(namespace, serviceName, roleDescriptor);
        final String refreshPolicy = request.param("refresh");
        if (refreshPolicy != null) {
            putRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(refreshPolicy));
        }

        return channel -> client.execute(PutServiceAccountAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
    }
}
