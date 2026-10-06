/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.action.Grant;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
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

import static org.elasticsearch.xpack.core.security.action.Grant.USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE;

/**
 * Implementation of the action needed to create an API key on behalf of another user (using an OAuth style "grant")
 */
public final class TransportGrantApiKeyAction extends TransportGrantAction<GrantApiKeyRequest, CreateApiKeyResponse> {

    private final ApiKeyService apiKeyService;
    private final ApiKeyUserRoleDescriptorResolver resolver;
    private final List<CustomTokenAuthenticator> customTokenAuthenticators;
    private final boolean stateless;

    @Inject
    public TransportGrantApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        ApiKeyService apiKeyService,
        CompositeRolesStore rolesStore,
        NamedXContentRegistry xContentRegistry,
        PluggableAuthenticatorChain pluggableAuthenticatorChain,
        Settings settings
    ) {
        this(
            transportService,
            actionFilters,
            threadPool.getThreadContext(),
            authenticationService,
            authorizationService,
            apiKeyService,
            new ApiKeyUserRoleDescriptorResolver(rolesStore, xContentRegistry),
            pluggableAuthenticatorChain,
            DiscoveryNode.isStateless(settings)
        );
    }

    TransportGrantApiKeyAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadContext threadContext,
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        ApiKeyService apiKeyService,
        ApiKeyUserRoleDescriptorResolver resolver,
        PluggableAuthenticatorChain pluggableAuthenticatorChain,
        boolean stateless
    ) {
        super(GrantApiKeyAction.NAME, transportService, actionFilters, authenticationService, authorizationService, threadContext);
        this.apiKeyService = apiKeyService;
        this.resolver = resolver;
        this.customTokenAuthenticators = pluggableAuthenticatorChain.getCustomAuthenticators()
            .stream()
            .filter(CustomTokenAuthenticator.class::isInstance)
            .map(CustomTokenAuthenticator.class::cast)
            .toList();
        this.stateless = stateless;
    }

    @Override
    protected AuthenticationToken getAuthenticationToken(Grant grant) {
        // The user-managed service account REST handlers have no ServerlessScope, so they are not activated in
        // serverless. This grant can only name an account created through those APIs.
        if (stateless && USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE.equals(grant.getType())) {
            if (grant.getServiceAccountToken() != null) {
                grant.getServiceAccountToken().close();
            }
            throw new ElasticsearchStatusException(
                "grant_type [{}] is not available when running in serverless mode",
                RestStatus.BAD_REQUEST,
                USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE
            );
        }
        return super.getAuthenticationToken(grant);
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
