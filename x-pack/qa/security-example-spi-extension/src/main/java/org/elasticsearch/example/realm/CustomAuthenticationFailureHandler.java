/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.DefaultAuthenticationFailureHandler;

import java.util.Collections;

public class CustomAuthenticationFailureHandler extends DefaultAuthenticationFailureHandler {

    public CustomAuthenticationFailureHandler() {
        super(Collections.emptyMap());
    }

    @Override
    public ElasticsearchSecurityException failedAuthentication(HttpPreRequest request, AuthenticationToken token, ThreadContext context) {
        ElasticsearchSecurityException e = super.failedAuthentication(request, token, context);
        // set a custom header
        e.addHeader("WWW-Authenticate", "custom-challenge");
        return e;
    }

    @Override
    public ElasticsearchSecurityException failedAuthentication(
        TransportRequest message,
        AuthenticationToken token,
        String action,
        ThreadContext context
    ) {
        ElasticsearchSecurityException e = super.failedAuthentication(message, token, action, context);
        // set a custom header
        e.addHeader("WWW-Authenticate", "custom-challenge");
        return e;
    }

    @Override
    public ElasticsearchSecurityException missingToken(HttpPreRequest request, ThreadContext context) {
        ElasticsearchSecurityException e = super.missingToken(request, context);
        // set a custom header
        e.addHeader("WWW-Authenticate", "custom-challenge");
        return e;
    }

    @Override
    public ElasticsearchSecurityException missingToken(TransportRequest message, String action, ThreadContext context) {
        ElasticsearchSecurityException e = super.missingToken(message, action, context);
        // set a custom header
        e.addHeader("WWW-Authenticate", "custom-challenge");
        return e;
    }
}
