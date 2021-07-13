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
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.service.IndexServiceAccountTokenStore;
import org.elasticsearch.xpack.security.authc.support.HttpTlsRuntimeCheck;

public class TransportCreateServiceAccountTokenAction
    extends HandledTransportAction<CreateServiceAccountTokenRequest, CreateServiceAccountTokenResponse> {

    private final IndexServiceAccountTokenStore indexServiceAccountTokenStore;
    private final SecurityContext securityContext;
    private final HttpTlsRuntimeCheck httpTlsRuntimeCheck;

    @Inject
    public TransportCreateServiceAccountTokenAction(TransportService transportService, ActionFilters actionFilters,
                                                    IndexServiceAccountTokenStore indexServiceAccountTokenStore,
                                                    SecurityContext securityContext,
                                                    HttpTlsRuntimeCheck httpTlsRuntimeCheck) {
        super(CreateServiceAccountTokenAction.NAME, transportService, actionFilters, CreateServiceAccountTokenRequest::new);
        this.indexServiceAccountTokenStore = indexServiceAccountTokenStore;
        this.securityContext = securityContext;
        this.httpTlsRuntimeCheck = httpTlsRuntimeCheck;
    }

    @Override
    protected void doExecute(Task task, CreateServiceAccountTokenRequest request,
                             ActionListener<CreateServiceAccountTokenResponse> listener) {
        httpTlsRuntimeCheck.checkTlsThenExecute(listener::onFailure, "create service account token", () -> {
            final Authentication authentication = securityContext.getAuthentication();
            if (authentication == null) {
                listener.onFailure(new IllegalStateException("authentication is required"));
            } else {
                indexServiceAccountTokenStore.createToken(authentication, request, listener);
            }
        });
    }
}
