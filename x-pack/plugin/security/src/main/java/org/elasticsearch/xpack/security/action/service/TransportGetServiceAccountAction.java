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
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

import java.util.function.Predicate;

public class TransportGetServiceAccountAction extends HandledTransportAction<GetServiceAccountRequest, GetServiceAccountResponse> {

    @Inject
    public TransportGetServiceAccountAction(TransportService transportService, ActionFilters actionFilters) {
        super(GetServiceAccountAction.NAME, transportService, actionFilters, GetServiceAccountRequest::new);
    }

    @Override
    protected void doExecute(Task task, GetServiceAccountRequest request, ActionListener<GetServiceAccountResponse> listener) {
        Predicate<ServiceAccount> filter = v -> true;
        if (request.getNamespace() != null) {
            filter = filter.and(v -> v.id().namespace().equals(request.getNamespace()));
        }
        if (request.getServiceName() != null) {
            filter = filter.and(v -> v.id().serviceName().equals(request.getServiceName()));
        }
        final ServiceAccountInfo[] serviceAccountInfos = ServiceAccountService.getServiceAccounts()
            .values()
            .stream()
            .filter(filter)
            .map(v -> new ServiceAccountInfo(v.id().asPrincipal(), v.roleDescriptor()))
            .toArray(ServiceAccountInfo[]::new);
        listener.onResponse(new GetServiceAccountResponse(serviceAccountInfos));
    }
}
