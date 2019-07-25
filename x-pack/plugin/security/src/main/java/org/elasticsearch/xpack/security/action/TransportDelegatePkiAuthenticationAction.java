/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationRequest;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.pki.X509AuthenticationToken;

import java.util.Map;

public class TransportDelegatePkiAuthenticationAction
        extends HandledTransportAction<DelegatePkiAuthenticationRequest, DelegatePkiAuthenticationResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDelegatePkiAuthenticationAction.class);

    private final ThreadPool threadPool;
    private final AuthenticationService authenticationService;
    private final TokenService tokenService;

    @Inject
    public TransportDelegatePkiAuthenticationAction(ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
            AuthenticationService authenticationService, TokenService tokenService) {
        super(DelegatePkiAuthenticationAction.NAME, transportService, actionFilters, DelegatePkiAuthenticationRequest::new);
        this.threadPool = threadPool;
        this.authenticationService = authenticationService;
        this.tokenService = tokenService;
    }

    @Override
    protected void doExecute(Task task, DelegatePkiAuthenticationRequest request,
            ActionListener<DelegatePkiAuthenticationResponse> listener) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        Authentication delegateeAuthentication = Authentication.getAuthentication(threadContext);
        final X509AuthenticationToken x509DelegatedToken = new X509AuthenticationToken(request.getCertificates(), true);
        logger.trace("Attempting to authenticate delegated x509Token [{}]", x509DelegatedToken);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            authenticationService.authenticate(DelegatePkiAuthenticationAction.NAME, request, x509DelegatedToken, ActionListener.wrap(authentication -> {
                assert authentication != null : "authentication should never be null at this point";
                tokenService.createOAuth2Tokens(authentication, delegateeAuthentication, Map.of(), false, ActionListener.wrap(tuple -> {
                    final TimeValue expiresIn = tokenService.getExpirationDelay();
                    listener.onResponse(new DelegatePkiAuthenticationResponse(tuple.v1(), expiresIn));
                }, listener::onFailure));
            }, e -> {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("Delegated x509Token [{}] could not be authenticated",
                        x509DelegatedToken), e);
                listener.onFailure(e);
            }));
        }
    }
}
