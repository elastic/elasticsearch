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
import org.elasticsearch.xpack.core.security.action.Grant;
import org.elasticsearch.xpack.core.security.action.apikey.BulkGrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.BulkGrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkGrantApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.CustomTokenAuthenticator;
import org.elasticsearch.xpack.security.action.TransportGrantAction;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.PluggableAuthenticatorChain;
import org.elasticsearch.xpack.security.authc.support.ApiKeyUserRoleDescriptorResolver;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.List;

/**
 * Implementation of the action needed to create multiple API keys on behalf of another user (using an OAuth style "grant").
 * Grant credentials are authenticated once; all keys are created under that authentication.
 */
public final class TransportBulkGrantApiKeyAction extends TransportGrantAction<BulkGrantApiKeyRequest, BulkGrantApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final ApiKeyUserRoleDescriptorResolver resolver;
    private final List<CustomTokenAuthenticator> customTokenAuthenticators;

    @Inject
    public TransportBulkGrantApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        ApiKeyService apiKeyService,
        CompositeRolesStore rolesStore,
        NamedXContentRegistry xContentRegistry,
        PluggableAuthenticatorChain pluggableAuthenticatorChain
    ) {
        this(
            transportService,
            actionFilters,
            threadPool.getThreadContext(),
            authenticationService,
            authorizationService,
            apiKeyService,
            new ApiKeyUserRoleDescriptorResolver(rolesStore, xContentRegistry),
            pluggableAuthenticatorChain
        );

    }

    TransportBulkGrantApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadContext threadContext,
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        ApiKeyService apiKeyService,
        ApiKeyUserRoleDescriptorResolver resolver,
        PluggableAuthenticatorChain pluggableAuthenticatorChain
    ) {
        super(BulkGrantApiKeyAction.NAME, transportService, actionFilters, authenticationService, authorizationService, threadContext);
        this.apiKeyService = apiKeyService;
        this.resolver = resolver;
        this.customTokenAuthenticators = pluggableAuthenticatorChain.getCustomAuthenticators()
            .stream()
            .filter(CustomTokenAuthenticator.class::isInstance)
            .map(CustomTokenAuthenticator.class::cast)
            .toList();
    }

    @Override
    protected void doExecuteWithGrantAuthentication(
        Task task,
        BulkGrantApiKeyRequest request,
        Authentication authentication,
        ActionListener<BulkGrantApiKeyResponse> listener
    ) {
        resolver.resolveUserRoleDescriptors(
            authentication,
            ActionListener.wrap(
                roleDescriptors -> apiKeyService.bulkCreateApiKeys(authentication, request.getApiKeyRequests(), roleDescriptors, listener),
                listener::onFailure
            )
        );
    }

    @Override
    protected AuthenticationToken extractAccessToken(Grant grant) {
        for (CustomTokenAuthenticator customTokenAuthenticator : customTokenAuthenticators) {
            AuthenticationToken token = customTokenAuthenticator.extractGrantAccessToken(grant);
            if (token != null) {
                return token;
            }
        }
        return super.extractAccessToken(grant);
    }
}
