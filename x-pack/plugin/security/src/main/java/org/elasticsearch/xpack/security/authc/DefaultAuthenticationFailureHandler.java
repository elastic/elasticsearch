/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;

/**
 * The default implementation of a {@link AuthenticationFailureHandler}. This
 * handler will return an exception with a RestStatus of 401 and the
 * WWW-Authenticate header with a auth scheme as returned by last realm based on the order.
 */
public class DefaultAuthenticationFailureHandler implements AuthenticationFailureHandler {
    public static final String HTTP_AUTH_HEADER = "WWW-Authenticate";
    private final Map<String, String[]> defaultResponseHeaders = new HashMap<>();

    public DefaultAuthenticationFailureHandler(final Realms realms) {
        final List<Realm> orderedListOfRealms = realms.asList();
        String defaultAuthenticateHeaderValue = Realm.WWW_AUTHN_HEADER_DEFAULT_VALUE;
        if (orderedListOfRealms.isEmpty() == false) {
            // Last realm from ordered list determines the value for header `WWW-Authenticate:`
            // Ideally the realm failing the authn should determine the value
            // ex. Error in parsing Basic authn token must use 'Basic' scheme, for spnego if
            // token parsing errors out it should be 'Negotiate' etc.
            defaultAuthenticateHeaderValue = orderedListOfRealms.get(orderedListOfRealms.size() - 1).getWWWAuthenticateHeaderValue();
        }
        defaultResponseHeaders.put(HTTP_AUTH_HEADER, new String[] { defaultAuthenticateHeaderValue });
    }

    @Override
    public ElasticsearchSecurityException failedAuthentication(RestRequest request, AuthenticationToken token, ThreadContext context) {
        return authenticationError(defaultResponseHeaders, "unable to authenticate user [{}] for REST request [{}]",
                null, token.principal(), request.uri());
    }

    @Override
    public ElasticsearchSecurityException failedAuthentication(TransportMessage message, AuthenticationToken token, String action,
            ThreadContext context) {
        return authenticationError(defaultResponseHeaders, "unable to authenticate user [{}] for action [{}]", null,
                token.principal(), action);
    }

    @Override
    public ElasticsearchSecurityException exceptionProcessingRequest(RestRequest request, Exception e, ThreadContext context) {
        if (e instanceof ElasticsearchSecurityException) {
            assert ((ElasticsearchSecurityException) e).status() == RestStatus.UNAUTHORIZED;
            assert ((ElasticsearchSecurityException) e).getHeader(HTTP_AUTH_HEADER).size() == 1;
            return (ElasticsearchSecurityException) e;
        }
        return authenticationError(defaultResponseHeaders, "error attempting to authenticate request", e);
    }

    @Override
    public ElasticsearchSecurityException exceptionProcessingRequest(TransportMessage message, String action, Exception e,
            ThreadContext context) {
        if (e instanceof ElasticsearchSecurityException) {
            assert ((ElasticsearchSecurityException) e).status() == RestStatus.UNAUTHORIZED;
            assert ((ElasticsearchSecurityException) e).getHeader(HTTP_AUTH_HEADER).size() == 1;
            return (ElasticsearchSecurityException) e;
        }
        return authenticationError(defaultResponseHeaders, "error attempting to authenticate request", e);
    }

    @Override
    public ElasticsearchSecurityException missingToken(RestRequest request, ThreadContext context) {
        return authenticationError(defaultResponseHeaders, "missing authentication token for REST request [{}]", null,
                request.uri());
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
