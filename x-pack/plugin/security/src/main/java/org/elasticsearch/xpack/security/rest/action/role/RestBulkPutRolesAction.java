/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.rest.action.role;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.role.BulkPutRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.role.BulkPutRoleRequestBuilderFactory;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest endpoint to bulk add a Roles to the security index
 */
public class RestBulkPutRolesAction extends NativeRoleBaseRestHandler {

    private final BulkPutRoleRequestBuilderFactory builderFactory;

    public RestBulkPutRolesAction(Settings settings, XPackLicenseState licenseState, BulkPutRoleRequestBuilderFactory builderFactory) {
        super(settings, licenseState);
        this.builderFactory = builderFactory;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_security/role"));
    }

    @Override
    public String getName() {
        return "security_bulk_put_roles_action";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final BulkPutRoleRequestBuilder requestBuilder = builderFactory.create(client)
            .content(request.requiredContent(), request.getXContentType());

        if (request.param("refresh") != null) {
            requestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(request.param("refresh")));
        }

        return channel -> requestBuilder.execute(new RestToXContentListener<>(channel));
    }
}
