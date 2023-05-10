/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.util.Set;

/**
 * Implementation of the action needed to create an API key
 */
public final class TransportCreateCrossClusterApiKeyAction extends HandledTransportAction<
    CreateCrossClusterApiKeyRequest,
    CreateApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final SecurityContext securityContext;

    @Inject
    public TransportCreateCrossClusterApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ApiKeyService apiKeyService,
        SecurityContext context
    ) {
        super(CreateCrossClusterApiKeyAction.NAME, transportService, actionFilters, CreateCrossClusterApiKeyRequest::new);
        this.apiKeyService = apiKeyService;
        this.securityContext = context;
    }

    @Override
    protected void doExecute(Task task, CreateCrossClusterApiKeyRequest request, ActionListener<CreateApiKeyResponse> listener) {
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            listener.onFailure(new IllegalStateException("authentication is required"));
        } else {
            apiKeyService.createApiKey(authentication, request, Set.of(), listener);
        }
    }
}
