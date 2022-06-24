/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.fixtures.http

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import org.apache.commons.io.IOUtils
import org.elasticsearch.gradle.internal.util.ports.AvailablePortAllocator
import org.junit.rules.ExternalResource

import java.util.function.Consumer

class HttpServerRule extends ExternalResource {

    private HttpServer server
    private List<String> contexts = new ArrayList<>()
    private int port

    @Override
    protected void before() throws Throwable {
        // TODO revisit port range allocation
        def socket = new ServerSocket(0)
        port = socket.getLocalPort()
        socket.close()
        server = HttpServer.create(new InetSocketAddress(port), 0) // localhost:6991
        server.setExecutor(null) // creates a default executor
        server.start()
    }

    @Override
    protected void after() {
        if (server != null) {
            server.stop(0) // doesn't wait all current exchange handlers complete
        }
    }

    String getUriFor(String path) {
        if (path.startsWith("/") == false) {
            path = "/" + path
        }
        String host = "http://localhost:" + port
        return host + path
    }

    void registerHandler(String uriToHandle, Closure<?> configuration) {
        registerHandler(uriToHandle, new Consumer<SimpleHttpHandler>() {
            @Override
            void accept(SimpleHttpHandler httpHandler) {
                configuration.call(httpHandler)
            }
        })
    }

    void registerHandler(String uriToHandle, Consumer<SimpleHttpHandler> configuration) {
        if(contexts.contains(uriToHandle)) {
            server.removeContext(uriToHandle)
        }

        def handler = new SimpleHttpHandler()
        configuration.accept(handler)
        server.createContext(uriToHandle, handler)
        contexts.add(uriToHandle)
    }

    private static class SimpleHttpHandler implements HttpHandler {
        private String responseBody = "";
        private String contentType = "text/plain";
        private int expectedHttpResponseCode

        @Override
        void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Content-Type", contentType)
            exchange.sendResponseHeaders(expectedHttpResponseCode, responseBody.length())
            IOUtils.write(responseBody, exchange.getResponseBody())
            exchange.close()
        }

        void setResponseBody(String responseBody) {
            this.responseBody = responseBody
        }

        void setExpectedHttpResponseCode(int expectedHttpResponseCode) {
            this.expectedHttpResponseCode = expectedHttpResponseCode
        }
    }
}