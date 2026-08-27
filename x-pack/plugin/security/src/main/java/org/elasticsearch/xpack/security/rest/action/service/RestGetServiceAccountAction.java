/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountManagedBy;
import org.elasticsearch.xpack.security.rest.action.SecurityBaseRestHandler;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

@ServerlessScope(Scope.INTERNAL)
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
        final GetServiceAccountRequest getServiceAccountRequest = new GetServiceAccountRequest(
            namespace,
            serviceName,
            managedBy(request, namespace)
        );
        return channel -> client.execute(GetServiceAccountAction.INSTANCE, getServiceAccountRequest, new RestToXContentListener<>(channel));
    }

    /**
     * The kinds of account to report. Omitting {@code managed_by} reports built-in accounts only when no namespace is
     * given, which keeps the whole-cluster listing's response shape for callers that read a role descriptor from every
     * entry. A request scoped to a namespace has no such shape to keep: the reserved namespace holds no user-managed
     * account and every other namespace held no account at all before this feature, so a scoped request reports both
     * kinds and finds an account the caller created without having to ask for it by kind.
     */
    private static EnumSet<ServiceAccountManagedBy> managedBy(RestRequest request, String namespace) {
        final String[] values = request.paramAsStringArray("managed_by", null);
        if (values == null) {
            return namespace == null ? EnumSet.of(ServiceAccountManagedBy.ELASTIC) : EnumSet.allOf(ServiceAccountManagedBy.class);
        }
        final EnumSet<ServiceAccountManagedBy> managedBy = EnumSet.noneOf(ServiceAccountManagedBy.class);
        for (String value : values) {
            managedBy.add(ServiceAccountManagedBy.fromValue(value));
        }
        return managedBy;
    }
}
