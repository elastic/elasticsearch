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

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;

/**
 * The default implementation of a {@link AuthenticationFailureHandler}. This
 * handler will return an exception with a RestStatus of 401 and the
 * WWW-Authenticate header with a auth scheme as returned by last realm based on
 * the order.
 */
public class DefaultAuthenticationFailureHandler implements AuthenticationFailureHandler {
    private final Map<String, String[]> defaultResponseHeaders = new HashMap<>();

    public DefaultAuthenticationFailureHandler() {
        defaultResponseHeaders.putAll(Realm.WWW_AUTH_RESPONSE_HEADER_BASIC_SCHEME);
    }

    public DefaultAuthenticationFailureHandler(final Map<String, String[]> defaultResponseHeaders) {
        this.defaultResponseHeaders.putAll(defaultResponseHeaders);
    }

    @Override
    public ElasticsearchSecurityException failedAuthentication(RestRequest request, AuthenticationToken token, ThreadContext context) {
        return authenticationError(defaultResponseHeaders, "unable to authenticate user [{}] for REST request [{}]", null,
                token.principal(), request.uri());
    }

    @Override
    public ElasticsearchSecurityException failedAuthentication(TransportMessage message, AuthenticationToken token, String action,
            ThreadContext context) {
        return authenticationError(defaultResponseHeaders, "unable to authenticate user [{}] for action [{}]", null, token.principal(),
                action);
    }

    @Override
    public ElasticsearchSecurityException exceptionProcessingRequest(RestRequest request, Exception e, ThreadContext context) {
        if (e instanceof ElasticsearchSecurityException) {
            assert ((ElasticsearchSecurityException) e).status() == RestStatus.UNAUTHORIZED;
            assert ((ElasticsearchSecurityException) e).getHeader(Realm.WWW_AUTHN_HEADER) != null
                    && ((ElasticsearchSecurityException) e).getHeader(Realm.WWW_AUTHN_HEADER).size() == 1;
            return (ElasticsearchSecurityException) e;
        }
        return authenticationError(defaultResponseHeaders, "error attempting to authenticate request", e);
    }

    @Override
    public ElasticsearchSecurityException exceptionProcessingRequest(TransportMessage message, String action, Exception e,
            ThreadContext context) {
        if (e instanceof ElasticsearchSecurityException) {
            assert ((ElasticsearchSecurityException) e).status() == RestStatus.UNAUTHORIZED;
            assert ((ElasticsearchSecurityException) e).getHeader(Realm.WWW_AUTHN_HEADER) != null
                    && ((ElasticsearchSecurityException) e).getHeader(Realm.WWW_AUTHN_HEADER).size() == 1;
            return (ElasticsearchSecurityException) e;
        }
        return authenticationError(defaultResponseHeaders, "error attempting to authenticate request", e);
    }

    @Override
    public ElasticsearchSecurityException missingToken(RestRequest request, ThreadContext context) {
        return authenticationError(defaultResponseHeaders, "missing authentication token for REST request [{}]", null, request.uri());
    }

    @Override
    public ElasticsearchSecurityException missingToken(TransportMessage message, String action, ThreadContext context) {
        return authenticationError(defaultResponseHeaders, "missing authentication token for action [{}]", null, action);
    }

    @Override
    public ElasticsearchSecurityException authenticationRequired(String action, ThreadContext context) {
        return authenticationError(defaultResponseHeaders, "action [{}] requires authentication", null, action);
    }
}
