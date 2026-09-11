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
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authc.service.UserManagedServiceAccountStore.PutResult;

/**
 * Creates a user-managed service account, or replaces one of the same name. The account's name is validated by the
 * request and the reserved namespace is refused by the account store, so nothing is checked again here.
 */
public class TransportPutUserManagedServiceAccountAction extends HandledTransportAction<
    PutUserManagedServiceAccountRequest,
    PutUserManagedServiceAccountResponse> {

    private final ServiceAccountService serviceAccountService;

    @Inject
    public TransportPutUserManagedServiceAccountAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServiceAccountService serviceAccountService
    ) {
        super(
            PutUserManagedServiceAccountAction.NAME,
            transportService,
            actionFilters,
            PutUserManagedServiceAccountRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.serviceAccountService = serviceAccountService;
    }

    @Override
    protected void doExecute(
        Task task,
        PutUserManagedServiceAccountRequest request,
        ActionListener<PutUserManagedServiceAccountResponse> listener
    ) {
        serviceAccountService.putUserManagedAccount(
            request.getAccountId(),
            request.getRoles(),
            request.isEnabled(),
            request.getRefreshPolicy(),
            listener.map(result -> new PutUserManagedServiceAccountResponse(result == PutResult.CREATED))
        );
    }
}
