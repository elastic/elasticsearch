/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.XPackField;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;

/**
 * The default implementation of a {@link AuthenticationFailureHandler}. This
 * handler will return an exception with a RestStatus of 401 and default failure
 * response headers like 'WWW-Authenticate'
 */
public class DefaultAuthenticationFailureHandler implements AuthenticationFailureHandler {
    private final Map<String, List<String>> defaultFailureResponseHeaders;

    /**
     * Constructs default authentication failure handler
     *
     * @deprecated replaced by {@link #DefaultAuthenticationFailureHandler(Map)}
     */
    @Deprecated
    public DefaultAuthenticationFailureHandler() {
        this(null);
    }

    /**
     * Constructs default authentication failure handler with provided default
     * response headers.
     *
     * @param failureResponseHeaders Map of header key and list of header values to
     *            be sent as failure response.
     * @see Realm#getAuthenticationFailureHeaders()
     */
    public DefaultAuthenticationFailureHandler(Map<String, List<String>> failureResponseHeaders) {
        if (failureResponseHeaders == null || failureResponseHeaders.isEmpty()) {
            failureResponseHeaders = Collections.singletonMap("WWW-Authenticate",
                    Collections.singletonList("Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\""));
        }
        this.defaultFailureResponseHeaders = Collections.unmodifiableMap(failureResponseHeaders);
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
        return createAuthenticationError("missing authentication token for REST request [{}]", null, request.uri());
    }

    @Override
    public ElasticsearchSecurityException missingToken(TransportMessage message, String action, ThreadContext context) {
        return createAuthenticationError("missing authentication token for action [{}]", null, action);
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
