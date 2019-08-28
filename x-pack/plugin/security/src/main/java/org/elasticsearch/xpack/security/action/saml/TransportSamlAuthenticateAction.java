/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.saml;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlToken;

import java.util.Map;

/**
 * Transport action responsible for taking saml content and turning it into a token.
 */
public final class TransportSamlAuthenticateAction extends HandledTransportAction<SamlAuthenticateRequest, SamlAuthenticateResponse> {

    private final ThreadPool threadPool;
    private final AuthenticationService authenticationService;
    private final TokenService tokenService;

    @Inject
    public TransportSamlAuthenticateAction(ThreadPool threadPool, TransportService transportService,
                                           ActionFilters actionFilters, AuthenticationService authenticationService,
                                           TokenService tokenService) {
        super(SamlAuthenticateAction.NAME, transportService, actionFilters, SamlAuthenticateRequest::new);
        this.threadPool = threadPool;
        this.authenticationService = authenticationService;
        this.tokenService = tokenService;
    }

    @Override
    protected void doExecute(Task task, SamlAuthenticateRequest request, ActionListener<SamlAuthenticateResponse> listener) {
        final SamlToken saml = new SamlToken(request.getSaml(), request.getValidRequestIds(), request.getRealm());
        logger.trace("Attempting to authenticate SamlToken [{}]", saml);
        final ThreadContext threadContext = threadPool.getThreadContext();
        Authentication originatingAuthentication = Authentication.getAuthentication(threadContext);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            authenticationService.authenticate(SamlAuthenticateAction.NAME, request, saml, ActionListener.wrap(authentication -> {
                AuthenticationResult result = threadContext.getTransient(AuthenticationResult.THREAD_CONTEXT_KEY);
                if (result == null) {
                    listener.onFailure(new IllegalStateException("Cannot find AuthenticationResult on thread context"));
                    return;
                }
                assert authentication != null : "authentication should never be null at this point";
                final Map<String, Object> tokenMeta = (Map<String, Object>) result.getMetadata().get(SamlRealm.CONTEXT_TOKEN_DATA);
                tokenService.createOAuth2Tokens(authentication, originatingAuthentication,
                        tokenMeta, true, ActionListener.wrap(tuple -> {
                            final TimeValue expiresIn = tokenService.getExpirationDelay();
                            listener.onResponse(
                                    new SamlAuthenticateResponse(authentication.getUser().principal(), tuple.v1(), tuple.v2(), expiresIn));
                        }, listener::onFailure));
            }, e -> {
                logger.debug(() -> new ParameterizedMessage("SamlToken [{}] could not be authenticated", saml), e);
                listener.onFailure(e);
            }));
        }
    }
}
