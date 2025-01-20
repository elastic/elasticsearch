/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.testclusters;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * This is a server which just accepts lines of JSON code and if the JSON
 * is valid and the root node is "transaction", then adds that JSON object
 * to a transaction list which is accessible externally to the class.
 * <p>
 * The Elastic agent sends lines of JSON code, and so this mock server
 * can be used as a basic APM server for testing.
 * <p>
 * The HTTP server used is the JDK embedded com.sun.net.httpserver
 */
public class MockApmServer {
    private static final Logger logger = Logging.getLogger(MockApmServer.class);
    private int port;

    public MockApmServer(int port) {
        this.port = port;
    }

    /**
     * Simple main that starts a mock APM server and prints the port it is
     * running on. This is not needed
     * for testing, it is just a convenient template for trying things out
     * if you want play around.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        MockApmServer server = new MockApmServer(9999);
        server.start();
    }

    private static volatile HttpServer instance;

    /**
     * Start the Mock APM server. Just returns empty JSON structures for every incoming message
     *
     * @return - the port the Mock APM server started on
     * @throws IOException
     */
    public synchronized int start() throws IOException {
        if (instance != null) {
            String hostname = instance.getAddress().getHostName();
            int port = instance.getAddress().getPort();
            logger.lifecycle("MockApmServer is already running. Reusing on address:port " + hostname + ":" + port);
            return port;
        }
        InetSocketAddress addr = new InetSocketAddress("0.0.0.0", port);
        HttpServer server = HttpServer.create(addr, 10);
        server.createContext("/exit", new ExitHandler());
        server.createContext("/", new RootHandler());

        server.start();
        instance = server;
        logger.lifecycle("MockApmServer started on port " + server.getAddress().getPort());
        return server.getAddress().getPort();
    }

    public int getPort() {
        return port;
    }

    /**
     * Stop the server gracefully if possible
     */
    public synchronized void stop() {
        logger.lifecycle("stopping apm server");
        instance.stop(1);
        instance = null;
    }

    class RootHandler implements HttpHandler {
        public void handle(HttpExchange t) {
            try {
                InputStream body = t.getRequestBody();
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                byte[] buffer = new byte[8 * 1024];
                int lengthRead;
                while ((lengthRead = body.read(buffer)) > 0) {
                    bytes.write(buffer, 0, lengthRead);
                }
                logger.lifecycle(("MockApmServer reading JSON objects: " + bytes.toString()));

                String response = "{}";
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class ExitHandler implements HttpHandler {
        private static final int STOP_TIME = 3;

        public void handle(HttpExchange t) {
            try {
                InputStream body = t.getRequestBody();
                String response = "{}";
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
                instance.stop(STOP_TIME);
                instance = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
