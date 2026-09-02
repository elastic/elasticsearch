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
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

/**
 * Deletes a user-managed service account. Whether the account's surviving tokens block the delete is decided by
 * {@code force}, which the service layer applies.
 */
public class TransportDeleteUserManagedServiceAccountAction extends HandledTransportAction<
    DeleteUserManagedServiceAccountRequest,
    DeleteUserManagedServiceAccountResponse> {

    private final ServiceAccountService serviceAccountService;

    @Inject
    public TransportDeleteUserManagedServiceAccountAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServiceAccountService serviceAccountService
    ) {
        super(
            DeleteUserManagedServiceAccountAction.NAME,
            transportService,
            actionFilters,
            DeleteUserManagedServiceAccountRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.serviceAccountService = serviceAccountService;
    }

    @Override
    protected void doExecute(
        Task task,
        DeleteUserManagedServiceAccountRequest request,
        ActionListener<DeleteUserManagedServiceAccountResponse> listener
    ) {
        serviceAccountService.deleteUserManagedAccount(
            request.getAccountId(),
            request.isForce(),
            request.getRefreshPolicy(),
            listener.map(DeleteUserManagedServiceAccountResponse::new)
        );
    }
}
