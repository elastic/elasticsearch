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
import org.elasticsearch.xpack.core.security.action.service.PutManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.PutManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.PutManagedServiceAccountResponse;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

public class TransportPutManagedServiceAccountAction extends HandledTransportAction<
    PutManagedServiceAccountRequest,
    PutManagedServiceAccountResponse> {

    private final ServiceAccountService serviceAccountService;

    @Inject
    public TransportPutManagedServiceAccountAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServiceAccountService serviceAccountService
    ) {
        super(
            PutManagedServiceAccountAction.NAME,
            transportService,
            actionFilters,
            PutManagedServiceAccountRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.serviceAccountService = serviceAccountService;
    }

    @Override
    protected void doExecute(
        Task task,
        PutManagedServiceAccountRequest request,
        ActionListener<PutManagedServiceAccountResponse> listener
    ) {
        serviceAccountService.putManagedAccount(request, listener);
    }
}
