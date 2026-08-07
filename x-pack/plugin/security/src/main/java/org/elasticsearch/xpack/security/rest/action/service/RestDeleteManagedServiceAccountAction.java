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
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.INTERNAL)
public class RestDeleteManagedServiceAccountAction extends SecurityBaseRestHandler {

    public RestDeleteManagedServiceAccountAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_security/service/{namespace}/{service}"));
    }

    @Override
    public String getName() {
        return "xpack_security_delete_managed_service_account";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final DeleteManagedServiceAccountRequest deleteRequest = new DeleteManagedServiceAccountRequest(
            request.param("namespace"),
            request.param("service")
        );
        final String refreshPolicy = request.param("refresh");
        if (refreshPolicy != null) {
            deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(refreshPolicy));
        }
        return channel -> client.execute(DeleteManagedServiceAccountAction.INSTANCE, deleteRequest, new RestToXContentListener<>(channel));
    }
}
