/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.transport.TransportMessage;

public class CustomAuthenticationFailureHandler extends DefaultAuthenticationFailureHandler {

    @Override
    public ElasticsearchSecurityException unsuccessfulAuthentication(RestRequest request, AuthenticationToken token) {
        ElasticsearchSecurityException e = super.unsuccessfulAuthentication(request, token);
        // set a custom header
        e.addHeader("WWW-Authenticate", "custom-challenge");
        return e;
    }

    @Override
    public ElasticsearchSecurityException unsuccessfulAuthentication(TransportMessage message, AuthenticationToken token, String action) {
        ElasticsearchSecurityException e = super.unsuccessfulAuthentication(message, token, action);
        // set a custom header
        e.addHeader("WWW-Authenticate", "custom-challenge");
        return e;
    }

    @Override
    public ElasticsearchSecurityException missingToken(RestRequest request) {
        ElasticsearchSecurityException e = super.missingToken(request);
        // set a custom header
        e.addHeader("WWW-Authenticate", "custom-challenge");
        return e;
    }

    @Override
    public ElasticsearchSecurityException missingToken(TransportMessage message, String action) {
        ElasticsearchSecurityException e = super.missingToken(message, action);
        // set a custom header
        e.addHeader("WWW-Authenticate", "custom-challenge");
        return e;
    }
}
