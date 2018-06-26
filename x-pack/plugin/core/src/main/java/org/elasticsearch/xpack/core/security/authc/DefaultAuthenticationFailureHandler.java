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

import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;

/**
 * The default implementation of a {@link AuthenticationFailureHandler}. This
 * handler will return an exception with a RestStatus of 401 and the
 * WWW-Authenticate header with values as list of configured and enabled auth
 * schemes as challenge.
 */
public class DefaultAuthenticationFailureHandler implements AuthenticationFailureHandler {
    private final List<String> supportedWWWAuthenticateResponseHeaderValues;

    /**
     * Constructs default authentication failure handler. By default it only
     * supports 'Basic' auth scheme and uses it for 'WWW-Authenticate' header value
     *
     * @deprecated @see {@link #DefaultAuthenticationFailureHandler(List)}
     */
    @Deprecated
    public DefaultAuthenticationFailureHandler() {
        this(Collections.singletonList("Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\""));
    }

    /**
     * Constructs default authentication failure handler with provided list of
     * supported auth schemes to be presented as challenge
     *
     * @param supportedWWWAuthenticateResponseHeaderValues List of supported auth
     *            schemes to be returned as response header 'WWW-Authenticate'
     * @see {@link Realm#getWWWAuthenticateHeaderValue()} which is used by an realm
     *      to specify auth scheme it wants to use
     */
    public DefaultAuthenticationFailureHandler(final List<String> supportedWWWAuthenticateResponseHeaderValues) {
        if (supportedWWWAuthenticateResponseHeaderValues == null || supportedWWWAuthenticateResponseHeaderValues.isEmpty()) {
            throw new IllegalArgumentException("supported WWW-Authenticate response header values is null or empty");
        }
        this.supportedWWWAuthenticateResponseHeaderValues = Collections.unmodifiableList(supportedWWWAuthenticateResponseHeaderValues);
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
        if (e instanceof ElasticsearchSecurityException) {
            assert ((ElasticsearchSecurityException) e).status() == RestStatus.UNAUTHORIZED;
            assert ((ElasticsearchSecurityException) e).getHeader("WWW-Authenticate").size() > 0;
            return (ElasticsearchSecurityException) e;
        }
        return createAuthenticationError("error attempting to authenticate request", e, (Object[]) null);
    }

    @Override
    public ElasticsearchSecurityException exceptionProcessingRequest(TransportMessage message, String action, Exception e,
            ThreadContext context) {
        if (e instanceof ElasticsearchSecurityException) {
            assert ((ElasticsearchSecurityException) e).status() == RestStatus.UNAUTHORIZED;
            assert ((ElasticsearchSecurityException) e).getHeader("WWW-Authenticate").size() > 0;
            return (ElasticsearchSecurityException) e;
        }
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
     * Also adds response header 'WWW-Authenticate' with values as list of
     * configured and enabled auth schemes as challenge.
     *
     * @param message error message
     * @param t root cause
     * @param args error message args
     * @return instance of {@link ElasticsearchSecurityException}
     */
    private ElasticsearchSecurityException createAuthenticationError(final String message, final Throwable t, final Object... args) {
        ElasticsearchSecurityException ese = authenticationError(message, t, args);
        // If it is already present then it will replace the existing header.
        ese.addHeader("WWW-Authenticate", supportedWWWAuthenticateResponseHeaderValues);
        return ese;
    }
}
