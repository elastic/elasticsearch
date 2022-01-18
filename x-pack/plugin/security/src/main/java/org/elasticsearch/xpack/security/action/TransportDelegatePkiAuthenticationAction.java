/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationRequest;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.pki.X509AuthenticationToken;

import java.security.cert.X509Certificate;
import java.util.Map;

/**
 * Implements the exchange of an {@code X509Certificate} chain into an access token. The certificate chain is represented as an array where
 * the first element is the target certificate containing the subject distinguished name that is requesting access. This may be followed by
 * additional certificates, with each subsequent certificate being the one used to certify the previous one. The certificate chain is
 * validated according to RFC 5280, by sequentially considering the trust configuration of every installed {@code PkiRealm} that has
 * {@code PkiRealmSettings#DELEGATION_ENABLED_SETTING} set to {@code true} (default is {@code false}). A successfully trusted target
 * certificate is also subject to the validation of the subject distinguished name according to that respective's realm
 * {@code PkiRealmSettings#USERNAME_PATTERN_SETTING}.
 *
 * IMPORTANT: The association between the subject public key in the target certificate and the corresponding private key is <b>not</b>
 * validated. This is part of the TLS authentication process and it is delegated to the proxy calling this API. The proxy is <b>trusted</b>
 * to have performed the TLS authentication, and this API translates that authentication into an Elasticsearch access token.
 */
public final class TransportDelegatePkiAuthenticationAction extends HandledTransportAction<
    DelegatePkiAuthenticationRequest,
    DelegatePkiAuthenticationResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDelegatePkiAuthenticationAction.class);

    private final ThreadPool threadPool;
    private final AuthenticationService authenticationService;
    private final TokenService tokenService;
    private final SecurityContext securityContext;

    @Inject
    public TransportDelegatePkiAuthenticationAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        AuthenticationService authenticationService,
        TokenService tokenService,
        SecurityContext securityContext
    ) {
        super(DelegatePkiAuthenticationAction.NAME, transportService, actionFilters, DelegatePkiAuthenticationRequest::new);
        this.threadPool = threadPool;
        this.authenticationService = authenticationService;
        this.tokenService = tokenService;
        this.securityContext = securityContext;
    }

    @Override
    protected void doExecute(
        Task task,
        DelegatePkiAuthenticationRequest request,
        ActionListener<DelegatePkiAuthenticationResponse> listener
    ) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        Authentication delegateeAuthentication = securityContext.getAuthentication();
        if (delegateeAuthentication == null) {
            listener.onFailure(new IllegalStateException("Delegatee authentication cannot be null"));
            return;
        }
        final X509AuthenticationToken x509DelegatedToken = X509AuthenticationToken.delegated(
            request.getCertificateChain().toArray(new X509Certificate[0]),
            delegateeAuthentication
        );
        logger.trace("Attempting to authenticate delegated x509Token [{}]", x509DelegatedToken);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            authenticationService.authenticate(
                DelegatePkiAuthenticationAction.NAME,
                request,
                x509DelegatedToken,
                ActionListener.wrap(authentication -> {
                    assert authentication != null : "authentication should never be null at this point";
                    tokenService.createOAuth2Tokens(
                        authentication,
                        delegateeAuthentication,
                        Map.of(),
                        false,
                        ActionListener.wrap(tokenResult -> {
                            final TimeValue expiresIn = tokenService.getExpirationDelay();
                            listener.onResponse(
                                new DelegatePkiAuthenticationResponse(tokenResult.getAccessToken(), expiresIn, authentication)
                            );
                        }, listener::onFailure)
                    );
                }, e -> {
                    logger.debug(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "Delegated x509Token [{}] could not be authenticated",
                            x509DelegatedToken
                        ),
                        e
                    );
                    listener.onFailure(e);
                })
            );
        }
    }
}
