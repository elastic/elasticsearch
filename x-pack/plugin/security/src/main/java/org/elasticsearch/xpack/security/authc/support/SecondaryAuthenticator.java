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
import org.elasticsearch.common.util.concurrent.ThreadContextTransient;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.AuthenticationService;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
     * Used by authenticators that require additional [@code X-Client-Authentication} header along with the Authorization header.
     */
    public static final String SECONDARY_X_CLIENT_AUTH_HEADER_NAME = "es-secondary-x-client-authentication";

    private static final String X_CLIENT_AUTHENTICATION = "X-Client-Authentication";

    private static final ThreadContextTransient<String> AUDIT_REQUEST_ID = ThreadContextTransient.transientValue(
        "_security_secondary_authc_audit_request_id",
        String.class
    );
    private static final ThreadContextTransient<Exception> FAILURE = ThreadContextTransient.transientValue(
        "_security_secondary_authc_failure",
        Exception.class
    );

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
     * Authenticates in the HTTP header validator because the REST interceptor must complete on the network thread.
     * Auditing success needs the request body, and a failure must not suppress the primary success audit.
     * Both are therefore deferred to {@link #completeAuthentication(RestRequest)}.
     */
    public void authenticateAndAttachResultToContext(HttpPreRequest request, ActionListener<Void> listener) {
        authenticateHttpRequest(request, listener.delegateResponse((l, e) -> {
            FAILURE.set(securityContext.getThreadContext(), e);
            l.onResponse(null);
        }).safeMap(ignored -> null));
    }

    /**
     * Throws the failure attached by {@link #authenticateAndAttachResultToContext(HttpPreRequest, ActionListener)}, if any,
     * otherwise audits the secondary authentication success, if any.
     */
    public void completeAuthentication(RestRequest request) throws Exception {
        final ThreadContext threadContext = securityContext.getThreadContext();
        final Exception failure = FAILURE.get(threadContext);
        if (failure != null) {
            throw failure;
        }
        final SecondaryAuthentication secondaryAuthentication = SecondaryAuthentication.readFromContext(securityContext);
        if (secondaryAuthentication == null) {
            return;
        }
        logger.trace("found secondary authentication {} in REST request [{}]", secondaryAuthentication, request.uri());
        final String auditRequestId = AUDIT_REQUEST_ID.require(threadContext);
        secondaryAuthentication.execute(ignore -> {
            threadContext.putHeader(AuditUtil.AUDIT_REQUEST_ID, auditRequestId);
            auditTrailService.get().authenticationSuccess(request);
            return null;
        });
    }

    /**
     * @param listener Handler for the {@link SecondaryAuthentication} object.
     *                 If the secondary authentication credentials do not exist the thread context, the
     *                 {@link ActionListener#onResponse(Object)} method is called with a {@code null} authentication value.
     *                 If the secondary authentication credentials are found in the thread context, but fail to be authenticated, then
     *                 the failure is returned through {@link ActionListener#onFailure(Exception)}.
     */
    void authenticateHttpRequest(HttpPreRequest request, ActionListener<SecondaryAuthentication> listener) {
        final ThreadContext threadContext = securityContext.getThreadContext();
        final AtomicReference<String> auditRequestId = new AtomicReference<>();
        // We never want the secondary authentication to fallback to anonymous.
        // Use cases for secondary authentication are far more likely to want to fall back to the primary authentication if no secondary
        // auth is provided, so in that case we do no want to set anything in the context
        authenticate(authListener -> authenticationService.authenticate(request, false, authListener.map(authentication -> {
            auditRequestId.set(AuditUtil.extractRequestId(threadContext));
            return authentication;
        })), listener.delegateFailureAndWrap((l, secondaryAuthentication) -> {
            if (secondaryAuthentication != null) {
                secondaryAuthentication.writeToContext(threadContext);
                AUDIT_REQUEST_ID.set(threadContext, auditRequestId.get());
            }
            l.onResponse(secondaryAuthentication);
        }));
    }

    private void authenticate(Consumer<ActionListener<Authentication>> authenticate, ActionListener<SecondaryAuthentication> listener) {
        final ThreadContext threadContext = securityContext.getThreadContext();
        final String header = threadContext.getHeader(SECONDARY_AUTH_HEADER_NAME);
        if (Strings.isNullOrEmpty(header)) {
            logger.trace("no secondary authentication credentials found (the [{}] header is [{}])", SECONDARY_AUTH_HEADER_NAME, header);
            listener.onResponse(null);
            return;
        }

        final Map<String, String> additionalHeaders = mapAdditionalSecondaryAuthHeaders(threadContext);

        final Supplier<ThreadContext.StoredContext> originalContext = threadContext.newRestorableContext(false);

        final ActionListener<SecondaryAuthentication> contextRestoringListener = new ContextPreservingActionListener<>(
            originalContext,
            listener
        );

        final ActionListener<Authentication> authenticationListener = ActionListener.wrap(authentication -> {
            if (authentication == null) {
                logger.debug("secondary authentication failed - authentication service returned a null authentication object");
                contextRestoringListener.onFailure(new ElasticsearchSecurityException("Failed to authenticate secondary user"));
            } else {
                Map<String, Object> transients = threadContext.getTransientHeaders()
                    .entrySet()
                    .stream()
                    .filter(e -> e.getValue() instanceof AuthenticationToken)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                logger.debug("secondary authentication succeeded [{}]", authentication);
                contextRestoringListener.onResponse(new SecondaryAuthentication(securityContext, authentication, transients));
            }
        }, e -> {
            logger.debug("secondary authentication failed - authentication service responded with failure", e);
            contextRestoringListener.onFailure(new ElasticsearchSecurityException("Failed to authenticate secondary user", e));
        });

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            logger.trace(
                "found secondary authentication credentials, placing them in the internal [{}] header for authentication",
                UsernamePasswordToken.BASIC_AUTH_HEADER
            );
            threadContext.putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, header);

            if (false == additionalHeaders.isEmpty()) {
                threadContext.putHeader(additionalHeaders);
            }

            authenticate.accept(authenticationListener);
        }
    }

    /**
     * Extracts additional secondary authentication headers from the thread context.
     * These headers are mapped from their secondary header names to the actual header names
     * expected by authenticators.
     *
     * @param threadContext the thread context to extract headers from
     * @return a map of header names to values for additional secondary auth headers, empty if none found
     */
    private Map<String, String> mapAdditionalSecondaryAuthHeaders(ThreadContext threadContext) {
        final String secondaryXClientAuthHeader = threadContext.getHeader(SECONDARY_X_CLIENT_AUTH_HEADER_NAME);
        if (Strings.hasText(secondaryXClientAuthHeader)) {
            logger.trace(
                "found additional secondary [{}] header, placing it in the [{}] header",
                SECONDARY_X_CLIENT_AUTH_HEADER_NAME,
                X_CLIENT_AUTHENTICATION
            );
            return Map.of(X_CLIENT_AUTHENTICATION, secondaryXClientAuthHeader);
        }

        return Map.of();
    }
}
