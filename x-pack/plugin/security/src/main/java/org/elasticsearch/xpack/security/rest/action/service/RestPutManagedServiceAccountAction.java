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
import org.elasticsearch.xpack.core.security.action.service.PutManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.PutManagedServiceAccountRequest;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

@ServerlessScope(Scope.INTERNAL)
public class RestPutManagedServiceAccountAction extends SecurityBaseRestHandler {

    public static final String MANAGED_SERVICE_ACCOUNTS_CAPABILITY = "managed_service_accounts";

    private final boolean managedServiceAccountsAvailable;

    public RestPutManagedServiceAccountAction(Settings settings, XPackLicenseState licenseState, boolean managedServiceAccountsAvailable) {
        super(settings, licenseState);
        this.managedServiceAccountsAvailable = managedServiceAccountsAvailable;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_security/service/{namespace}/{service}"));
    }

    @Override
    public String getName() {
        return "xpack_security_put_managed_service_account";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(RestRequest request, NodeClient client) throws IOException {
        final PutManagedServiceAccountRequest putRequest = PutManagedServiceAccountRequest.parse(
            request.param("namespace"),
            request.param("service"),
            request.contentParser()
        );
        final String refreshPolicy = request.param("refresh");
        if (refreshPolicy != null) {
            putRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.parse(refreshPolicy));
        }
        return channel -> client.execute(PutManagedServiceAccountAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        if (managedServiceAccountsAvailable) {
            return Set.of(MANAGED_SERVICE_ACCOUNTS_CAPABILITY);
        }
        return Set.of();
    }
}
