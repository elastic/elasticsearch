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

import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;

/**
 * The default implementation of a {@link AuthenticationFailureHandler}. This
 * handler will return an exception with a RestStatus of 401 and the
 * WWW-Authenticate header with configured auth scheme else by default adds Basic challenge.
 */
public class DefaultAuthenticationFailureHandler implements AuthenticationFailureHandler {
    private final String defaultWWWAuthenticateResponseHeader;

    public DefaultAuthenticationFailureHandler() {
        this("Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\"");
    }

    public DefaultAuthenticationFailureHandler(final String defaultWWWAuthenticateHeaderValue) {
        defaultWWWAuthenticateResponseHeader = defaultWWWAuthenticateHeaderValue;
    }

    @Override
    public ElasticsearchSecurityException failedAuthentication(RestRequest request, AuthenticationToken token, ThreadContext context) {
        return addHeader(
                authenticationError("unable to authenticate user [{}] for REST request [{}]", token.principal(), request.uri()));
    }

    @Override
    public ElasticsearchSecurityException failedAuthentication(TransportMessage message, AuthenticationToken token, String action,
            ThreadContext context) {
        return addHeader(authenticationError("unable to authenticate user [{}] for action [{}]", token.principal(), action));
    }

    @Override
    public ElasticsearchSecurityException exceptionProcessingRequest(RestRequest request, Exception e, ThreadContext context) {
        if (e instanceof ElasticsearchSecurityException) {
            assert ((ElasticsearchSecurityException) e).status() == RestStatus.UNAUTHORIZED;
            assert ((ElasticsearchSecurityException) e).getHeader("WWW-Authenticate").size() == 1;
            return (ElasticsearchSecurityException) e;
        }
        return addHeader(authenticationError("error attempting to authenticate request", e));
    }

    @Override
    public ElasticsearchSecurityException exceptionProcessingRequest(TransportMessage message, String action, Exception e,
            ThreadContext context) {
        if (e instanceof ElasticsearchSecurityException) {
            assert ((ElasticsearchSecurityException) e).status() == RestStatus.UNAUTHORIZED;
            assert ((ElasticsearchSecurityException) e).getHeader("WWW-Authenticate").size() == 1;
            return (ElasticsearchSecurityException) e;
        }
        return addHeader(authenticationError("error attempting to authenticate request", e));
    }

    @Override
    public ElasticsearchSecurityException missingToken(RestRequest request, ThreadContext context) {
        return addHeader(authenticationError("missing authentication token for REST request [{}]", request.uri()));
    }

    @Override
    public ElasticsearchSecurityException missingToken(TransportMessage message, String action, ThreadContext context) {
        return addHeader(authenticationError("missing authentication token for action [{}]", action));
    }

    @Override
    public ElasticsearchSecurityException authenticationRequired(String action, ThreadContext context) {
        return addHeader(authenticationError("action [{}] requires authentication", action));
    }

    /**
     * This method replaces existing 'WWW-Authenticate' header if any
     *
     * @param ese instance of {@link ElasticsearchSecurityException}
     * @return same instance of {@link ElasticsearchSecurityException} with header
     *         'WWW-Authenticate'
     */
    private ElasticsearchSecurityException addHeader(ElasticsearchSecurityException ese) {
        ese.addHeader("WWW-Authenticate", defaultWWWAuthenticateResponseHeader);
        return ese;
    }
}
