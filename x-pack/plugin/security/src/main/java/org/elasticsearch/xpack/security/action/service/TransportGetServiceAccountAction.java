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
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.security.authc.service.IndexUserServiceAccountStore;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

import java.util.ArrayList;
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
        Predicate<ServiceAccount> filter = Predicates.always();
        if (request.getNamespace() != null) {
            filter = filter.and(v -> v.id().namespace().equals(request.getNamespace()));
        }
        if (request.getServiceName() != null) {
            filter = filter.and(v -> v.id().serviceName().equals(request.getServiceName()));
        }
        // When the caller has not opted into user-defined accounts, preserve historical behaviour and emit only
        // built-in ones with no `type` field (so existing clients see the response shape they expect).
        final boolean includeUserDefined = request.isIncludeUserDefined();
        final List<ServiceAccountInfo> infos = new ArrayList<>();
        for (ServiceAccount account : ServiceAccountService.getServiceAccounts().values()) {
            if (filter.test(account)) {
                infos.add(
                    new ServiceAccountInfo(
                        account.id().asPrincipal(),
                        account.roleDescriptor(),
                        includeUserDefined ? ServiceAccountInfo.Type.BUILT_IN : null
                    )
                );
            }
        }
        if (includeUserDefined == false) {
            listener.onResponse(new GetServiceAccountResponse(infos.toArray(new ServiceAccountInfo[0])));
            return;
        }

        final IndexUserServiceAccountStore store = serviceAccountService.getIndexUserServiceAccountStore();
        if (store == null) {
            listener.onResponse(new GetServiceAccountResponse(infos.toArray(new ServiceAccountInfo[0])));
            return;
        }
        store.findAllAccounts(ActionListener.wrap(userDefinedAccounts -> {
            for (var userDefined : userDefinedAccounts) {
                if (request.getNamespace() != null && false == userDefined.principal().startsWith(request.getNamespace() + "/")) {
                    continue;
                }
                if (request.getServiceName() != null && false == userDefined.principal().endsWith("/" + request.getServiceName())) {
                    continue;
                }
                infos.add(
                    new ServiceAccountInfo(userDefined.principal(), userDefined.roleDescriptor(), ServiceAccountInfo.Type.USER_DEFINED)
                );
            }
            listener.onResponse(new GetServiceAccountResponse(infos.toArray(new ServiceAccountInfo[0])));
        }, listener::onFailure));
    }
}
