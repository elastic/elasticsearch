/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportMessage;

import static org.elasticsearch.shield.support.Exceptions.authenticationError;

/**
 * The default implementation of a {@link AuthenticationFailureHandler}. This handler will return an exception with a
 * RestStatus of 401 and the WWW-Authenticate header with a Basic challenge.
 */
public class DefaultAuthenticationFailureHandler implements AuthenticationFailureHandler {

    @Override
    public ElasticsearchSecurityException unsuccessfulAuthentication(RestRequest request, AuthenticationToken token) {
        return authenticationError("unable to authenticate user [{}] for REST request [{}]", token.principal(), request.uri());
    }

    @Override
    public ElasticsearchSecurityException unsuccessfulAuthentication(TransportMessage message, AuthenticationToken token, String action) {
        return authenticationError("unable to authenticate user [{}] for action [{}]", token.principal(), action);
    }

    @Override
    public ElasticsearchSecurityException exceptionProcessingRequest(RestRequest request, Exception e) {
        if (e instanceof ElasticsearchSecurityException) {
            assert ((ElasticsearchSecurityException) e).status() == RestStatus.UNAUTHORIZED;
            assert ((ElasticsearchSecurityException) e).getHeader("WWW-Authenticate").size() == 1;
            return (ElasticsearchSecurityException) e;
        }
        return authenticationError("error attempting to authenticate request", e);
    }

    @Override
    public ElasticsearchSecurityException exceptionProcessingRequest(TransportMessage message, Exception e) {
        if (e instanceof ElasticsearchSecurityException) {
            assert ((ElasticsearchSecurityException) e).status() == RestStatus.UNAUTHORIZED;
            assert ((ElasticsearchSecurityException) e).getHeader("WWW-Authenticate").size() == 1;
            return (ElasticsearchSecurityException) e;
        }
        return authenticationError("error attempting to authenticate request", e);
    }

    @Override
    public ElasticsearchSecurityException missingToken(RestRequest request) {
        return authenticationError("missing authentication token for REST request [{}]", request.uri());
    }

    @Override
    public ElasticsearchSecurityException missingToken(TransportMessage message, String action) {
        return authenticationError("missing authentication token for action [{}]", action);
    }

    @Override
    public ElasticsearchSecurityException authenticationRequired(String action) {
        return authenticationError("action [{}] requires authentication", action);
    }
}
