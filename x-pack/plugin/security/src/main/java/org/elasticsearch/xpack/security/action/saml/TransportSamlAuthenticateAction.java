/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.saml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.saml.SamlRealm;
import org.elasticsearch.xpack.security.authc.saml.SamlToken;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Transport action responsible for taking saml content and turning it into a token.
 */
public final class TransportSamlAuthenticateAction extends HandledTransportAction<SamlAuthenticateRequest, SamlAuthenticateResponse> {

    private final ThreadPool threadPool;
    private final AuthenticationService authenticationService;
    private final TokenService tokenService;
    private final SecurityContext securityContext;
    private final Executor genericExecutor;

    @Inject
    public TransportSamlAuthenticateAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        AuthenticationService authenticationService,
        TokenService tokenService,
        SecurityContext securityContext
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(
            SamlAuthenticateAction.NAME,
            transportService,
            actionFilters,
            SamlAuthenticateRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.threadPool = threadPool;
        this.authenticationService = authenticationService;
        this.tokenService = tokenService;
        this.securityContext = securityContext;
        this.genericExecutor = threadPool.generic();
    }

    @Override
    protected void doExecute(Task task, SamlAuthenticateRequest request, ActionListener<SamlAuthenticateResponse> listener) {
        // workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        genericExecutor.execute(ActionRunnable.wrap(listener, l -> doExecuteForked(task, request, l)));
    }

    private void doExecuteForked(Task task, SamlAuthenticateRequest request, ActionListener<SamlAuthenticateResponse> listener) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        final SamlToken saml = new SamlToken(request.getSaml(), request.getValidRequestIds(), request.getRealm());
        logger.trace("Attempting to authenticate SamlToken [{}]", saml);
        final ThreadContext threadContext = threadPool.getThreadContext();
        Authentication originatingAuthentication = securityContext.getAuthentication();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            authenticationService.authenticate(SamlAuthenticateAction.NAME, request, saml, ActionListener.wrap(authentication -> {
                AuthenticationResult<User> result = threadContext.getTransient(AuthenticationResult.THREAD_CONTEXT_KEY);
                if (result == null) {
                    listener.onFailure(new IllegalStateException("Cannot find User AuthenticationResult on thread context"));
                    return;
                }
                assert authentication != null : "authentication should never be null at this point";
                assert false == authentication.isRunAs() : "saml realm authentication cannot have run-as";
                @SuppressWarnings("unchecked")
                final Map<String, Object> tokenMeta = (Map<String, Object>) result.getMetadata().get(SamlRealm.CONTEXT_TOKEN_DATA);
                tokenService.createOAuth2Tokens(
                    authentication,
                    originatingAuthentication,
                    tokenMeta,
                    true,
                    ActionListener.wrap(tokenResult -> {
                        final TimeValue expiresIn = tokenService.getExpirationDelay();
                        listener.onResponse(
                            new SamlAuthenticateResponse(
                                authentication,
                                tokenResult.getAccessToken(),
                                tokenResult.getRefreshToken(),
                                expiresIn
                            )
                        );
                    }, listener::onFailure)
                );
            }, e -> {
                logger.debug(() -> "SamlToken [" + saml + "] could not be authenticated", e);
                listener.onFailure(e);
            }));
        }
    }
}
