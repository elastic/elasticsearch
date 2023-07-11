/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.oidc;

import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.openid.connect.sdk.Nonce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.oidc.OpenIdConnectRealm;
import org.elasticsearch.xpack.security.authc.oidc.OpenIdConnectToken;

import java.util.Map;

public class TransportOpenIdConnectAuthenticateAction extends HandledTransportAction<
    OpenIdConnectAuthenticateRequest,
    OpenIdConnectAuthenticateResponse> {

    private final ThreadPool threadPool;
    private final AuthenticationService authenticationService;
    private final TokenService tokenService;
    private final SecurityContext securityContext;
    private static final Logger logger = LogManager.getLogger(TransportOpenIdConnectAuthenticateAction.class);

    @Inject
    public TransportOpenIdConnectAuthenticateAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        AuthenticationService authenticationService,
        TokenService tokenService,
        SecurityContext securityContext
    ) {
        super(OpenIdConnectAuthenticateAction.NAME, transportService, actionFilters, OpenIdConnectAuthenticateRequest::new);
        this.threadPool = threadPool;
        this.authenticationService = authenticationService;
        this.tokenService = tokenService;
        this.securityContext = securityContext;
    }

    @Override
    protected void doExecute(
        Task task,
        OpenIdConnectAuthenticateRequest request,
        ActionListener<OpenIdConnectAuthenticateResponse> listener
    ) {
        final OpenIdConnectToken token = new OpenIdConnectToken(
            request.getRedirectUri(),
            new State(request.getState()),
            new Nonce(request.getNonce()),
            request.getRealm()
        );
        final ThreadContext threadContext = threadPool.getThreadContext();
        Authentication originatingAuthentication = securityContext.getAuthentication();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            authenticationService.authenticate(OpenIdConnectAuthenticateAction.NAME, request, token, ActionListener.wrap(authentication -> {
                AuthenticationResult<User> result = threadContext.getTransient(AuthenticationResult.THREAD_CONTEXT_KEY);
                if (result == null) {
                    listener.onFailure(new IllegalStateException("Cannot find User AuthenticationResult on thread context"));
                    return;
                }
                @SuppressWarnings("unchecked")
                final Map<String, Object> tokenMetadata = (Map<String, Object>) result.getMetadata()
                    .get(OpenIdConnectRealm.CONTEXT_TOKEN_DATA);
                tokenService.createOAuth2Tokens(
                    authentication,
                    originatingAuthentication,
                    tokenMetadata,
                    true,
                    listener.delegateFailureAndWrap((l, tokenResult) -> {
                        final TimeValue expiresIn = tokenService.getExpirationDelay();
                        l.onResponse(
                            new OpenIdConnectAuthenticateResponse(
                                authentication,
                                tokenResult.getAccessToken(),
                                tokenResult.getRefreshToken(),
                                expiresIn
                            )
                        );
                    })
                );
            }, e -> {
                logger.debug(() -> "OpenIDConnectToken [" + token + "] could not be authenticated", e);
                listener.onFailure(e);
            }));
        }
    }
}
