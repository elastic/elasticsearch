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
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

/**
 * Reports the service accounts a request selects. Built-in accounts are known to every node, while user-managed ones
 * have to be read from the account store, so a request naming both kinds is answered from two sources and merged.
 */
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
        final List<ServiceAccountInfo> builtInInfos = request.getManagedBy().contains(ServiceAccountManagedBy.ELASTIC)
            ? builtInAccountInfos(request)
            : List.of();
        if (request.getManagedBy().contains(ServiceAccountManagedBy.USER) == false) {
            listener.onResponse(newResponse(builtInInfos, List.of()));
            return;
        }
        serviceAccountService.getUserManagedAccountInfos(
            request.getNamespace(),
            request.getServiceName(),
            listener.map(userManagedInfos -> newResponse(builtInInfos, userManagedInfos))
        );
    }

    private static List<ServiceAccountInfo> builtInAccountInfos(GetServiceAccountRequest request) {
        Predicate<ServiceAccount> filter = Predicates.always();
        if (request.getNamespace() != null) {
            filter = filter.and(v -> v.id().namespace().equals(request.getNamespace()));
        }
        if (request.getServiceName() != null) {
            filter = filter.and(v -> v.id().serviceName().equals(request.getServiceName()));
        }
        return ServiceAccountService.getBuiltInServiceAccounts()
            .values()
            .stream()
            .filter(filter)
            .<ServiceAccountInfo>map(v -> new ServiceAccountInfo.BuiltIn(v.id().asPrincipal(), v.roleDescriptor()))
            .toList();
    }

    private static GetServiceAccountResponse newResponse(List<ServiceAccountInfo> builtInInfos, List<ServiceAccountInfo> userManagedInfos) {
        final List<ServiceAccountInfo> infos = new ArrayList<>(builtInInfos.size() + userManagedInfos.size());
        infos.addAll(builtInInfos);
        infos.addAll(userManagedInfos);
        infos.sort(Comparator.comparing(ServiceAccountInfo::principal));
        return new GetServiceAccountResponse(infos.toArray(ServiceAccountInfo[]::new));
    }
}
