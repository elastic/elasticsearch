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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenResponse;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteServiceAccountTokenAction extends SecurityBaseRestHandler {

    public RestDeleteServiceAccountTokenAction(Settings settings, XPackLicenseState licenseState) {
        super(settings, licenseState);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_security/service/{namespace}/{service}/credential/token/{name}"));
    }

    @Override
    public String getName() {
        return "xpack_security_delete_service_account_token";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final DeleteServiceAccountTokenRequest deleteServiceAccountTokenRequest = new DeleteServiceAccountTokenRequest(
            request.param("namespace"),
            request.param("service"),
            request.param("name")
        );
        final String refreshPolicy = request.param("refresh");
        if (refreshPolicy != null) {
            deleteServiceAccountTokenRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(refreshPolicy));
        }
        return channel -> client.execute(
            DeleteServiceAccountTokenAction.INSTANCE,
            deleteServiceAccountTokenRequest,
            new RestToXContentListener<>(channel) {
                @Override
                protected RestStatus getStatus(DeleteServiceAccountTokenResponse response) {
                    return response.found() ? RestStatus.OK : RestStatus.NOT_FOUND;
                }
            }
        );
    }
}
