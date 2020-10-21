/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.support.ApiKeyGenerator;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

/**
 * Implementation of the action needed to create an API key on behalf of another user (using an OAuth style "grant")
 */
public final class TransportGrantApiKeyAction extends HandledTransportAction<GrantApiKeyRequest, CreateApiKeyResponse> {

    private final ThreadContext threadContext;
    private final ApiKeyGenerator generator;
    private final AuthenticationService authenticationService;
    private final TokenService tokenService;

    @Inject
    public TransportGrantApiKeyAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool,
                                      ApiKeyService apiKeyService, AuthenticationService authenticationService, TokenService tokenService,
                                      CompositeRolesStore rolesStore, NamedXContentRegistry xContentRegistry) {
        this(transportService, actionFilters, threadPool.getThreadContext(),
            new ApiKeyGenerator(apiKeyService, rolesStore, xContentRegistry), authenticationService, tokenService
        );
    }

    // Constructor for testing
    TransportGrantApiKeyAction(TransportService transportService, ActionFilters actionFilters, ThreadContext threadContext,
                                      ApiKeyGenerator generator, AuthenticationService authenticationService, TokenService tokenService) {
        super(GrantApiKeyAction.NAME, transportService, actionFilters, GrantApiKeyRequest::new);
        this.threadContext = threadContext;
        this.generator = generator;
        this.authenticationService = authenticationService;
        this.tokenService = tokenService;
    }

    @Override
    protected void doExecute(Task task, GrantApiKeyRequest request, ActionListener<CreateApiKeyResponse> listener) {
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            resolveAuthentication(request.getGrant(), request, ActionListener.wrap(
                authentication -> generator.generateApiKey(authentication, request.getApiKeyRequest(), listener),
                listener::onFailure
            ));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void resolveAuthentication(GrantApiKeyRequest.Grant grant, TransportRequest transportRequest,
                                       ActionListener<Authentication> listener) {
        switch (grant.getType()) {
            case GrantApiKeyRequest.PASSWORD_GRANT_TYPE:
                final UsernamePasswordToken token = new UsernamePasswordToken(grant.getUsername(), grant.getPassword());
                authenticationService.authenticate(super.actionName, transportRequest, token, listener);
                return;
            case GrantApiKeyRequest.ACCESS_TOKEN_GRANT_TYPE:
                tokenService.authenticateToken(grant.getAccessToken(), listener);
                return;
            default:
                listener.onFailure(new ElasticsearchSecurityException("the grant type [{}] is not supported", grant.getType()));
                return;
        }
    }


}
