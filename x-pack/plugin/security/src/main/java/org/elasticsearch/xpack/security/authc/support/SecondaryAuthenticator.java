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

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication.SECONDARY_AUTHC_PRESERVED_TRANSIENT_PREFIX;

/**
 * Performs "secondary user authentication" (that is, a second user, _not_ second factor authentication).
 */
public class SecondaryAuthenticator {

    /**
     * The term "Authorization" in the header value is to mimic the standard HTTP "Authorization" header
     */
    public static final String SECONDARY_AUTH_HEADER_NAME = "es-secondary-authorization";

    /**
     * Header name for secondary client authentication credentials.
     * Used by authenticators that require additional headers beyond the Authorization header,
     * such as X-Client-Authentication.
     */
    public static final String SECONDARY_X_CLIENT_AUTH_HEADER_NAME = "es-secondary-x-client-authentication";
    private static final String X_CLIENT_AUTHENTICATION_HEADER = "X-Client-Authentication";

    private static final Logger logger = LogManager.getLogger(SecondaryAuthenticator.class);
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
            authListener -> authenticationService.authenticate(request.getHttpRequest(), false, authListener.map(authentication -> {
                auditTrailService.get().authenticationSuccess(request);
                return authentication;
            })),
            listener.delegateFailureAndWrap((l, secondaryAuthentication) -> {
                if (secondaryAuthentication != null) {
                    secondaryAuthentication.writeToContext(threadContext);
                }
                l.onResponse(secondaryAuthentication);
            })
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

        final Map<String, String> additionalSecondaryAuthHeaders = extractAdditionalSecondaryHeaders(threadContext);

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            logger.trace(
                "found secondary authentication credentials, placing them in the internal [{}] header for authentication",
                UsernamePasswordToken.BASIC_AUTH_HEADER
            );
            threadContext.putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, header);
            additionalSecondaryAuthHeaders.forEach(threadContext::putHeader);

            final ActionListener<Authentication> wrappedListener = authenticationListener.delegateFailureAndWrap((l, auth) -> {
                if (auth != null) {
                    var capturedTransientHeaders = capturePreservableAuthcTransients(threadContext);
                    storePreservedTransientsInOriginalContext(originalContext, capturedTransientHeaders);
                }
                l.onResponse(auth);
            });

            authenticate.accept(wrappedListener);
        }
    }

    private Map<String, String> extractAdditionalSecondaryHeaders(ThreadContext threadContext) {
        final String secondaryClientAuth = threadContext.getHeader(SECONDARY_X_CLIENT_AUTH_HEADER_NAME);
        if (Strings.hasText(secondaryClientAuth)) {
            logger.trace(
                "found secondary client authentication credentials in [{}], will place them in the [{}] header",
                SECONDARY_X_CLIENT_AUTH_HEADER_NAME,
                X_CLIENT_AUTHENTICATION_HEADER
            );
            return Map.of(X_CLIENT_AUTHENTICATION_HEADER, secondaryClientAuth);
        }
        return Map.of();
    }

    private Map<String, Object> capturePreservableAuthcTransients(ThreadContext threadContext) {
        final Map<String, Object> transientHeaders = threadContext.getTransientHeaders();
        final Map<String, Object> captured = new java.util.HashMap<>();
        for (Map.Entry<String, Object> entry : transientHeaders.entrySet()) {
            final String key = entry.getKey();
            if (key.startsWith(SecondaryAuthentication.PRESERVABLE_AUTHC_TRANSIENT_PREFIX)) {
                captured.put(key, entry.getValue());
            }
        }
        if (logger.isTraceEnabled() && captured.isEmpty() == false) {
            logger.trace("captured preservable authc transient headers: {}", captured.keySet());
        }
        return captured.isEmpty() ? Map.of() : Map.copyOf(captured);
    }

    private void storePreservedTransientsInOriginalContext(
        Supplier<ThreadContext.StoredContext> originalContext,
        Map<String, Object> capturedTransientHeaders
    ) {
        // restoring original context temporary to add additional transient headers
        try (var ignored = originalContext.get()) {
            final ThreadContext originalThreadContext = securityContext.getThreadContext();
            for (var entry : capturedTransientHeaders.entrySet()) {
                originalThreadContext.putTransient(SECONDARY_AUTHC_PRESERVED_TRANSIENT_PREFIX + entry.getKey(), entry.getValue());
            }
        }
    }
}
