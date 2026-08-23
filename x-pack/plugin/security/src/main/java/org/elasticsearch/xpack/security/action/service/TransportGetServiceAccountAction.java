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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountManagedBy;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.support.ManagedServiceAccountIdValidator;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

public class TransportGetServiceAccountAction extends HandledTransportAction<GetServiceAccountRequest, GetServiceAccountResponse> {

    private final ServiceAccountService serviceAccountService;

    @Inject
    public TransportGetServiceAccountAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServiceAccountService serviceAccountService
    ) {
        super(
            GetServiceAccountAction.NAME,
            transportService,
            actionFilters,
            GetServiceAccountRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.serviceAccountService = serviceAccountService;
    }

    @Override
    protected void doExecute(Task task, GetServiceAccountRequest request, ActionListener<GetServiceAccountResponse> listener) {
        final List<ServiceAccountInfo> builtInInfos;
        if (request.getManagedBy().contains(ServiceAccountManagedBy.ELASTIC)) {
            Predicate<ServiceAccount> builtInFilter = Predicates.always();
            if (request.getNamespace() != null) {
                builtInFilter = builtInFilter.and(v -> v.id().namespace().equals(request.getNamespace()));
            }
            if (request.getServiceName() != null) {
                builtInFilter = builtInFilter.and(v -> v.id().serviceName().equals(request.getServiceName()));
            }
            builtInInfos = ServiceAccountService.getBuiltInServiceAccounts()
                .values()
                .stream()
                .filter(builtInFilter)
                .map(v -> ServiceAccountInfo.builtIn(v.id().asPrincipal(), v.roleDescriptor()))
                .toList();
        } else {
            builtInInfos = List.of();
        }

        if (request.getManagedBy().contains(ServiceAccountManagedBy.USER) == false || requestsBuiltInAccountsOnly(request)) {
            listener.onResponse(new GetServiceAccountResponse(builtInInfos.toArray(ServiceAccountInfo[]::new)));
            return;
        }

        serviceAccountService.getManagedAccountInfos(request.getNamespace(), request.getServiceName(), ActionListener.wrap(managedInfos -> {
            final List<ServiceAccountInfo> allInfos = new ArrayList<>(builtInInfos.size() + managedInfos.size());
            allInfos.addAll(builtInInfos);
            allInfos.addAll(managedInfos);
            allInfos.sort(Comparator.comparing(ServiceAccountInfo::getPrincipal));
            listener.onResponse(new GetServiceAccountResponse(allInfos.toArray(ServiceAccountInfo[]::new)));
        }, listener::onFailure));
    }

    private static boolean requestsBuiltInAccountsOnly(GetServiceAccountRequest request) {
        return ManagedServiceAccountIdValidator.BUILTIN_NAMESPACE.equals(request.getNamespace());
    }
}
