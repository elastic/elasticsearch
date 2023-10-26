/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.apmintegration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.spi.XContentProvider;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@SuppressForbidden(reason = "Uses an HTTP server for testing")
public class RecordingApmServer extends ExternalResource {
    private static final Logger logger = LogManager.getLogger(RecordingApmServer.class);

    private static final XContentProvider.FormatProvider XCONTENT = XContentProvider.provider().getJsonXContent();

    final List<String> received = new CopyOnWriteArrayList<>();

    private static HttpServer server;

    @Override
    protected void before() throws Throwable {
        server = HttpServer.create();
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    @Override
    protected void after() {
        server.stop(1);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                try (InputStream requestBody = exchange.getRequestBody()) {
                    if (requestBody != null) {
                        var read = readJsonMessages(requestBody);
                        read.forEach(s -> logger.debug(s));
                        received.addAll(read);
                    }
                }

            } catch (RuntimeException e) {
                logger.warn("failed to parse request", e);
            }
            exchange.sendResponseHeaders(201, 0);
        }
    }

    private List<String> readJsonMessages(InputStream input) throws IOException {
        // XContentParser parser = XCONTENT.XContent().createParser(XContentParserConfiguration.EMPTY, input);
        // if (parser.currentToken() == null) {
        // parser.nextToken();
        // }
        // return XContentParserUtils.parseList(parser, XContentParser::textOrNull);
        return Arrays.stream(new String(input.readAllBytes(), StandardCharsets.UTF_8).split(System.lineSeparator())).toList();
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public List<String> getMessages() {
        return received;
    }
}
