/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.support.HttpTlsRuntimeCheck;

import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TransportGetServiceAccountAction extends HandledTransportAction<GetServiceAccountRequest, GetServiceAccountResponse> {

    private final HttpTlsRuntimeCheck httpTlsRuntimeCheck;

    @Inject
    public TransportGetServiceAccountAction(TransportService transportService, ActionFilters actionFilters,
                                            HttpTlsRuntimeCheck httpTlsRuntimeCheck) {
        super(GetServiceAccountAction.NAME, transportService, actionFilters, GetServiceAccountRequest::new);
        this.httpTlsRuntimeCheck = httpTlsRuntimeCheck;
    }

    @Override
    protected void doExecute(Task task, GetServiceAccountRequest request, ActionListener<GetServiceAccountResponse> listener) {
        httpTlsRuntimeCheck.checkTlsThenExecute(listener::onFailure, "get service accounts", () -> {
            final Predicate<ServiceAccount> filter;
            if (request.getNamespace() == null && request.getServiceName() == null) {
                filter = v -> true;
            } else if (request.getServiceName() == null) {
                filter = v -> v.id().namespace().equals(request.getNamespace());
            } else {
                filter = v -> v.id().namespace().equals(request.getNamespace()) && v.id().serviceName().equals(request.getServiceName());
            }
            final Map<String, RoleDescriptor> results = ServiceAccountService.getServiceAccounts()
                .values()
                .stream()
                .filter(filter)
                .collect(Collectors.toMap(v -> v.id().asPrincipal(), ServiceAccount::roleDescriptor));
            listener.onResponse(new GetServiceAccountResponse(results));
        });
    }
}
