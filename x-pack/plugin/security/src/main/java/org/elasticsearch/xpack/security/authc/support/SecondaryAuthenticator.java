/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.AuthenticationService;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Performs "secondary user authentication" (that is, a second user, _not_ second factor authentication).
 */
public class SecondaryAuthenticator {

    /**
     * The term "Authorization" in the header value is to mimic the standard HTTP "Authorization" header
     */
    public static final String SECONDARY_AUTH_HEADER_NAME = "es-secondary-authorization";

    private final Logger logger = LogManager.getLogger(SecondaryAuthenticator.class);
    private final SecurityContext securityContext;
    private final AuthenticationService authenticationService;
    private final AuditTrailService auditTrailService;

    public SecondaryAuthenticator(
        Settings settings,
        ThreadContext threadContext,
        AuthenticationService authenticationService,
        AuditTrailService auditTrailService
    ) {
        this(new SecurityContext(settings, threadContext), authenticationService, auditTrailService);
    }

    public SecondaryAuthenticator(
        SecurityContext securityContext,
        AuthenticationService authenticationService,
        AuditTrailService auditTrailService
    ) {
        this.securityContext = securityContext;
        this.authenticationService = authenticationService;
        this.auditTrailService = auditTrailService;
    }

    /**
     * @param listener Handler for the {@link SecondaryAuthentication} object.
     *                 If the secondary authentication credentials do not exist the thread context, the
     *                 {@link ActionListener#onResponse(Object)} method is called with a {@code null} authentication value.
     *                 If the secondary authentication credentials are found in the thread context, but fail to be authenticated, then
     *                 the failure is returned through {@link ActionListener#onFailure(Exception)}.
     */
    public void authenticate(String action, TransportRequest request, ActionListener<SecondaryAuthentication> listener) {
        // We never want the secondary authentication to fallback to anonymous.
        // Use cases for secondary authentication are far more likely to want to fall back to the primary authentication if no secondary
        // auth is provided, so in that case we do no want to set anything in the context
        authenticate(authListener -> authenticationService.authenticate(action, request, false, authListener), listener);
    }

    /**
     * @param listener Handler for the {@link SecondaryAuthentication} object.
     *                 If the secondary authentication credentials do not exist the thread context, the
     *                 {@link ActionListener#onResponse(Object)} method is called with a {@code null} authentication value.
     *                 If the secondary authentication credentials are found in the thread context, but fail to be authenticated, then
     *                 the failure is returned through {@link ActionListener#onFailure(Exception)}.
     */
    public void authenticateAndAttachToContext(RestRequest request, ActionListener<SecondaryAuthentication> listener) {
        final ThreadContext threadContext = securityContext.getThreadContext();
        // We never want the secondary authentication to fallback to anonymous.
        // Use cases for secondary authentication are far more likely to want to fall back to the primary authentication if no secondary
        // auth is provided, so in that case we do no want to set anything in the context
        authenticate(
            authListener -> authenticationService.authenticate(
                request.getHttpRequest(),
                false,
                authListener.delegateFailure((l, authentication) -> {
                    auditTrailService.get().authenticationSuccess(request);
                    l.onResponse(authentication);
                })
            ),
            ActionListener.wrap(secondaryAuthentication -> {
                if (secondaryAuthentication != null) {
                    secondaryAuthentication.writeToContext(threadContext);
                }
                listener.onResponse(secondaryAuthentication);
            }, listener::onFailure)
        );
    }

    private void authenticate(Consumer<ActionListener<Authentication>> authenticate, ActionListener<SecondaryAuthentication> listener) {
        final ThreadContext threadContext = securityContext.getThreadContext();
        final String header = threadContext.getHeader(SECONDARY_AUTH_HEADER_NAME);
        if (Strings.isNullOrEmpty(header)) {
            logger.trace("no secondary authentication credentials found (the [{}] header is [{}])", SECONDARY_AUTH_HEADER_NAME, header);
            listener.onResponse(null);
            return;
        }

        final Supplier<ThreadContext.StoredContext> originalContext = threadContext.newRestorableContext(false);
        final ActionListener<Authentication> authenticationListener = new ContextPreservingActionListener<>(
            originalContext,
            ActionListener.wrap(authentication -> {
                if (authentication == null) {
                    logger.debug("secondary authentication failed - authentication service returned a null authentication object");
                    listener.onFailure(new ElasticsearchSecurityException("Failed to authenticate secondary user"));
                } else {
                    logger.debug("secondary authentication succeeded [{}]", authentication);
                    listener.onResponse(new SecondaryAuthentication(securityContext, authentication));
                }
            }, e -> {
                logger.debug("secondary authentication failed - authentication service responded with failure", e);
                listener.onFailure(new ElasticsearchSecurityException("Failed to authenticate secondary user", e));
            })
        );

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            logger.trace(
                "found secondary authentication credentials, placing them in the internal [{}] header for authentication",
                UsernamePasswordToken.BASIC_AUTH_HEADER
            );
            threadContext.putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, header);
            authenticate.accept(authenticationListener);
        }
    }
}
