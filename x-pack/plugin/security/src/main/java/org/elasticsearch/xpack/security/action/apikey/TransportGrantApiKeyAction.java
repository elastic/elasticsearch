/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.security.action.TransportGrantAction;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.ApiKeyGenerator;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

/**
 * Implementation of the action needed to create an API key on behalf of another user (using an OAuth style "grant")
 */
public final class TransportGrantApiKeyAction extends TransportGrantAction<GrantApiKeyRequest, CreateApiKeyResponse> {

    private final ApiKeyGenerator generator;

    @Inject
    public TransportGrantApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ApiKeyService apiKeyService,
        AuthenticationService authenticationService,
        CompositeRolesStore rolesStore,
        NamedXContentRegistry xContentRegistry
    ) {
        this(
            transportService,
            actionFilters,
            threadPool.getThreadContext(),
            new ApiKeyGenerator(apiKeyService, rolesStore, xContentRegistry),
            authenticationService
        );
    }

    // Constructor for testing
    TransportGrantApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadContext threadContext,
        ApiKeyGenerator generator,
        AuthenticationService authenticationService
    ) {
        super(GrantApiKeyAction.NAME, transportService, actionFilters, GrantApiKeyRequest::new, authenticationService, threadContext);
        this.generator = generator;
    }

    @Override
    protected void doExecute(Task task, GrantApiKeyRequest request, ActionListener<CreateApiKeyResponse> listener) {
        executeWithGrantAuthentication(
            request,
            listener.delegateFailure((l, authentication) -> generator.generateApiKey(authentication, request.getApiKeyRequest(), listener))
        );
    }
}
