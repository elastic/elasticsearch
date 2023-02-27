/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;

@SuppressForbidden(reason = "Tests that all requests come via a proxy")
class ForwardedViaProxyHandler implements HttpHandler {

    private final HttpHandler delegateHandler;

    ForwardedViaProxyHandler(HttpHandler delegateHandler) {
        this.delegateHandler = delegateHandler;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        assert "test-web-proxy-server".equals(exchange.getRequestHeaders().getFirst("X-Via"));
        delegateHandler.handle(exchange);
    }
}
