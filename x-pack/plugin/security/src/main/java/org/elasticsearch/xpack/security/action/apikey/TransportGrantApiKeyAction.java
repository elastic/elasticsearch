/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.action.TransportGrantAction;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.support.ApiKeyUserRoleDescriptorResolver;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

/**
 * Implementation of the action needed to create an API key on behalf of another user (using an OAuth style "grant")
 */
public final class TransportGrantApiKeyAction extends TransportGrantAction<GrantApiKeyRequest, CreateApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final ApiKeyUserRoleDescriptorResolver resolver;

    @Inject
    public TransportGrantApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        ApiKeyService apiKeyService,
        CompositeRolesStore rolesStore,
        NamedXContentRegistry xContentRegistry
    ) {
        this(
            transportService,
            actionFilters,
            threadPool.getThreadContext(),
            authenticationService,
            authorizationService,
            apiKeyService,
            new ApiKeyUserRoleDescriptorResolver(rolesStore, xContentRegistry)
        );
    }

    TransportGrantApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadContext threadContext,
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        ApiKeyService apiKeyService,
        ApiKeyUserRoleDescriptorResolver resolver
    ) {
        super(GrantApiKeyAction.NAME, transportService, actionFilters, authenticationService, authorizationService, threadContext);
        this.apiKeyService = apiKeyService;
        this.resolver = resolver;
    }

    @Override
    protected void doExecuteWithGrantAuthentication(
        Task task,
        GrantApiKeyRequest request,
        Authentication authentication,
        ActionListener<CreateApiKeyResponse> listener
    ) {
        resolver.resolveUserRoleDescriptors(
            authentication,
            ActionListener.wrap(
                roleDescriptors -> apiKeyService.createApiKey(authentication, request.getApiKeyRequest(), roleDescriptors, listener),
                listener::onFailure
            )
        );
    }
}
