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
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

public class TransportDeleteManagedServiceAccountAction extends HandledTransportAction<
    DeleteManagedServiceAccountRequest,
    DeleteManagedServiceAccountResponse> {

    private final ServiceAccountService serviceAccountService;

    @Inject
    public TransportDeleteManagedServiceAccountAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServiceAccountService serviceAccountService
    ) {
        super(
            DeleteManagedServiceAccountAction.NAME,
            transportService,
            actionFilters,
            DeleteManagedServiceAccountRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.serviceAccountService = serviceAccountService;
    }

    @Override
    protected void doExecute(
        Task task,
        DeleteManagedServiceAccountRequest request,
        ActionListener<DeleteManagedServiceAccountResponse> listener
    ) {
        serviceAccountService.deleteManagedAccount(request, listener);
    }
}
