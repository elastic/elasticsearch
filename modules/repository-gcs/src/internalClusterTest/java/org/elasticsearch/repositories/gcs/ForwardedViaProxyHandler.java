package org.elasticsearch.repositories.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;

class ForwardedViaProxyHandler implements HttpHandler {

    private final HttpHandler delegateHandler;

    public ForwardedViaProxyHandler(HttpHandler delegateHandler) {
        this.delegateHandler = delegateHandler;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        assert "test-forward-proxy".equals(exchange.getRequestHeaders().getFirst("X-Via"));
        delegateHandler.handle(exchange);
    }
}
