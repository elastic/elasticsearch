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
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.CreateUserManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

/**
 * Creates a token for a user-managed service account. The authentication is passed on because the token document
 * records who created it, the same as for a built-in account.
 */
public class TransportCreateUserManagedServiceAccountTokenAction extends HandledTransportAction<
    CreateServiceAccountTokenRequest,
    CreateServiceAccountTokenResponse> {

    private final ServiceAccountService serviceAccountService;
    private final SecurityContext securityContext;

    @Inject
    public TransportCreateUserManagedServiceAccountTokenAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServiceAccountService serviceAccountService,
        SecurityContext securityContext
    ) {
        super(
            CreateUserManagedServiceAccountTokenAction.NAME,
            transportService,
            actionFilters,
            CreateServiceAccountTokenRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.serviceAccountService = serviceAccountService;
        this.securityContext = securityContext;
    }

    @Override
    protected void doExecute(
        Task task,
        CreateServiceAccountTokenRequest request,
        ActionListener<CreateServiceAccountTokenResponse> listener
    ) {
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        } else {
            serviceAccountService.createUserManagedToken(authentication, request, listener);
        }
    }
}
