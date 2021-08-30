/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.XPackField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.core.security.support.Exceptions.internalServerError;

/**
 * The default implementation of a {@link AuthenticationFailureHandler}. This
 * handler will return an exception with a RestStatus of 401 and default failure
 * response headers like 'WWW-Authenticate'
 */
public class DefaultAuthenticationFailureHandler implements AuthenticationFailureHandler {
    private volatile Map<String, List<String>> defaultFailureResponseHeaders;

    /**
     * Constructs default authentication failure handler with provided default
     * response headers.
     *
     * @param failureResponseHeaders Map of header key and list of header values to
     *            be sent as failure response.
     * @see Realm#getAuthenticationFailureHeaders()
     */
    public DefaultAuthenticationFailureHandler(final Map<String, List<String>> failureResponseHeaders) {
        if (failureResponseHeaders == null || failureResponseHeaders.isEmpty()) {
            this.defaultFailureResponseHeaders = Collections.singletonMap("WWW-Authenticate",
                    Collections.singletonList("Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\""));
        } else {
            this.defaultFailureResponseHeaders = Collections.unmodifiableMap(failureResponseHeaders.entrySet().stream().collect(Collectors
                    .toMap(entry -> entry.getKey(), entry -> {
                        if (entry.getKey().equalsIgnoreCase("WWW-Authenticate")) {
                            List<String> values = new ArrayList<>(entry.getValue());
                            values.sort(Comparator.comparing(DefaultAuthenticationFailureHandler::authSchemePriority));
                            return Collections.unmodifiableList(values);
                        } else {
                            return Collections.unmodifiableList(entry.getValue());
                        }
                    })));
        }
    }

    /**
     * This method is called when failureResponseHeaders need to be set (at startup) or updated (if license state changes)
     *
     * @param failureResponseHeaders the Map of failure response headers to be set
     */
    public void setHeaders(Map<String, List<String>> failureResponseHeaders){
        defaultFailureResponseHeaders = failureResponseHeaders;
    }

    /**
     * For given 'WWW-Authenticate' header value returns the priority based on
     * the auth-scheme. Lower number denotes more secure and preferred
     * auth-scheme than the higher number.
     *
     * @param headerValue string starting with auth-scheme name
     * @return integer value denoting priority for given auth scheme.
     */
    private static Integer authSchemePriority(final String headerValue) {
        if (headerValue.regionMatches(true, 0, "negotiate", 0, "negotiate".length())) {
            return 0;
        } else if (headerValue.regionMatches(true, 0, "bearer", 0, "bearer".length())) {
            return 1;
        } else if (headerValue.regionMatches(true, 0, "apikey", 0, "apikey".length())) {
            return 2;
        } else if (headerValue.regionMatches(true, 0, "basic", 0, "basic".length())) {
            return 3;
        } else {
            return 4;
        }
    }

    @Override
    public ElasticsearchSecurityException failedAuthentication(RestRequest request, AuthenticationToken token, ThreadContext context) {
        return createAuthenticationError("unable to authenticate user [{}] for REST request [{}]", null, token.principal(), request.uri());
    }

    @Override
    public ElasticsearchSecurityException failedAuthentication(TransportMessage message, AuthenticationToken token, String action,
            ThreadContext context) {
        return createAuthenticationError("unable to authenticate user [{}] for action [{}]", null, token.principal(), action);
    }

    @Override
    public ElasticsearchSecurityException exceptionProcessingRequest(RestRequest request, Exception e, ThreadContext context) {
        return createAuthenticationError("error attempting to authenticate request", e, (Object[]) null);
    }

    @Override
    public ElasticsearchSecurityException exceptionProcessingRequest(TransportMessage message, String action, Exception e,
            ThreadContext context) {
        return createAuthenticationError("error attempting to authenticate request", e, (Object[]) null);
    }

    @Override
    public ElasticsearchSecurityException missingToken(RestRequest request, ThreadContext context) {
        return createAuthenticationError("missing authentication credentials for REST request [{}]", null, request.uri());
    }

    @Override
    public ElasticsearchSecurityException missingToken(TransportMessage message, String action, ThreadContext context) {
        return createAuthenticationError("missing authentication credentials for action [{}]", null, action);
    }

    @Override
    public ElasticsearchSecurityException authenticationRequired(String action, ThreadContext context) {
        return createAuthenticationError("action [{}] requires authentication", null, action);
    }

    /**
     * Creates an instance of {@link ElasticsearchSecurityException} with
     * {@link RestStatus#UNAUTHORIZED} status.
     * <p>
     * Also adds default failure response headers as configured for this
     * {@link DefaultAuthenticationFailureHandler}
     * <p>
     * It may replace existing response headers if the cause is an instance of
     * {@link ElasticsearchSecurityException}
     *
     * @param message error message
     * @param t cause, if it is an instance of
     *            {@link ElasticsearchSecurityException} asserts status is
     *            RestStatus.UNAUTHORIZED and adds headers to it, else it will
     *            create a new instance of {@link ElasticsearchSecurityException}
     * @param args error message args
     * @return instance of {@link ElasticsearchSecurityException}
     */
    private ElasticsearchSecurityException createAuthenticationError(final String message, final Throwable t, final Object... args) {
        final ElasticsearchSecurityException ese;
        final boolean containsNegotiateWithToken;
        if (t instanceof ElasticsearchSecurityException) {
            assert ((ElasticsearchSecurityException) t).status() == RestStatus.UNAUTHORIZED;
            ese = (ElasticsearchSecurityException) t;
            if (ese.getHeader("WWW-Authenticate") != null && ese.getHeader("WWW-Authenticate").isEmpty() == false) {
                /**
                 * If 'WWW-Authenticate' header is present with 'Negotiate ' then do not
                 * replace. In case of kerberos spnego mechanism, we use
                 * 'WWW-Authenticate' header value to communicate outToken to peer.
                 */
                containsNegotiateWithToken =
                        ese.getHeader("WWW-Authenticate").stream()
                                .anyMatch(s -> s != null && s.regionMatches(true, 0, "Negotiate ", 0, "Negotiate ".length()));
            } else {
                containsNegotiateWithToken = false;
            }
        } else if (t instanceof ElasticsearchStatusException && ((ElasticsearchStatusException) t).status() == INTERNAL_SERVER_ERROR) {
            ese = internalServerError(message, t, args);
            containsNegotiateWithToken = false;
        } else {
            ese = authenticationError(message, t, args);
            containsNegotiateWithToken = false;
        }
        defaultFailureResponseHeaders.entrySet().stream().forEach((e) -> {
            if (containsNegotiateWithToken && e.getKey().equalsIgnoreCase("WWW-Authenticate")) {
                return;
            }
            // If it is already present then it will replace the existing header.
            ese.addHeader(e.getKey(), e.getValue());
        });
        return ese;
    }
}
