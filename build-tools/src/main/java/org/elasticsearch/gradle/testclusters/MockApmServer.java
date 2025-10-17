/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.testclusters;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.JsonGeneratorDelegate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

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
@NotThreadSafe
public class MockApmServer {
    private static final Logger logger = Logging.getLogger(MockApmServer.class);
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MockApmServer.class);
    private final Pattern metricFilter;
    private final Pattern transactionFilter;
    private final Pattern transactionExcludesFilter;

    private HttpServer instance;

    public MockApmServer(String metricFilter, String transactionFilter, String transactionExcludesFilter) {
        this.metricFilter = createWildcardPattern(metricFilter);
        this.transactionFilter = createWildcardPattern(transactionFilter);
        this.transactionExcludesFilter = createWildcardPattern(transactionExcludesFilter);
    }

    private Pattern createWildcardPattern(String filter) {
        if (filter == null || filter.isEmpty()) {
            return null;
        }
        var pattern = Arrays.stream(filter.split(",\\s*"))
            .map(Pattern::quote)
            .map(s -> s.replace("*", "\\E.*\\Q"))
            .collect(Collectors.joining(")|(", "(", ")"));
        return Pattern.compile(pattern);
    }

    /**
     * Start the Mock APM server. Just returns empty JSON structures for every incoming message
     *
     * @throws IOException
     */
    public void start() throws IOException {
        if (instance != null) {
            throw new IllegalStateException("MockApmServer already started");
        }
        InetSocketAddress addr = new InetSocketAddress("0.0.0.0", 0);
        HttpServer server = HttpServer.create(addr, 10);
        server.createContext("/", new RootHandler());
        server.start();
        instance = server;
        logger.lifecycle("MockApmServer started on port " + server.getAddress().getPort());
    }

    public int getPort() {
        if (instance == null) {
            throw new IllegalStateException("MockApmServer not started");
        }
        return instance.getAddress().getPort();
    }

    /**
     * Stop the server gracefully if possible
     */
    public void stop() {
        if (instance != null) {
            logger.lifecycle("stopping apm server");
            instance.stop(1);
            instance = null;
        }
    }

    class RootHandler implements HttpHandler {
        private static final String ANSI_RESET = "\u001B[0m";
        private static final String ANSI_BLUE = "\u001B[34m";
        private static final String ANSI_GREEN = "\u001B[32m";
        private static final String ANSI_YELLOW = "\u001B[33m";
        private static final String ANSI_PURPLE = "\u001B[35m";
        private static final String ANSI_CYAN = "\u001B[36m";

        private final ObjectMapper mapper = new ObjectMapper();
        private final ObjectWriter prettyWriter = mapper.writerWithDefaultPrettyPrinter();
        private final JsonFactory jsonFactory = mapper.getFactory();

        /**
         * A custom JsonGenerator that delegates all calls to a base generator
         * but wraps the output of value-writing methods in ANSI color codes.
         */
        private static class HighlightingJsonGenerator extends JsonGeneratorDelegate {

            HighlightingJsonGenerator(JsonGenerator d) {
                // Pass 'false' to prevent base generator from being auto-closed by this delegate
                super(d, false);
            }

            @Override
            public void writeFieldName(String name) throws IOException {
                delegate.writeRaw(ANSI_BLUE);
                delegate.writeFieldName(name);
                delegate.writeRaw(ANSI_RESET);
            }

            @Override
            public void writeString(String text) throws IOException {
                delegate.writeRaw(ANSI_GREEN);
                delegate.writeString(text);
                delegate.writeRaw(ANSI_RESET);
            }

            @Override
            public void writeNumber(int v) throws IOException {
                delegate.writeRaw(ANSI_YELLOW);
                delegate.writeNumber(v);
                delegate.writeRaw(ANSI_RESET);
            }

            @Override
            public void writeNumber(long v) throws IOException {
                delegate.writeRaw(ANSI_YELLOW);
                delegate.writeNumber(v);
                delegate.writeRaw(ANSI_RESET);
            }

            @Override
            public void writeNumber(BigInteger v) throws IOException {
                delegate.writeRaw(ANSI_YELLOW);
                delegate.writeNumber(v);
                delegate.writeRaw(ANSI_RESET);
            }

            @Override
            public void writeNumber(double v) throws IOException {
                delegate.writeRaw(ANSI_YELLOW);
                delegate.writeNumber(v);
                delegate.writeRaw(ANSI_RESET);
            }

            @Override
            public void writeNumber(float v) throws IOException {
                delegate.writeRaw(ANSI_YELLOW);
                delegate.writeNumber(v);
                delegate.writeRaw(ANSI_RESET);
            }

            @Override
            public void writeNumber(BigDecimal v) throws IOException {
                delegate.writeRaw(ANSI_YELLOW);
                delegate.writeNumber(v);
                delegate.writeRaw(ANSI_RESET);
            }

            @Override
            public void writeNumber(String encodedValue) throws IOException, UnsupportedOperationException {
                delegate.writeRaw(ANSI_YELLOW);
                delegate.writeNumber(encodedValue);
                delegate.writeRaw(ANSI_RESET);
            }

            @Override
            public void writeBoolean(boolean state) throws IOException {
                delegate.writeRaw(ANSI_PURPLE);
                delegate.writeBoolean(state);
                delegate.writeRaw(ANSI_RESET);
            }

            @Override
            public void writeNull() throws IOException {
                delegate.writeRaw(ANSI_CYAN);
                delegate.writeNull();
                delegate.writeRaw(ANSI_RESET);
            }
        }

        /**
         * Serializes a JsonNode to a string using Jackson's DefaultPrettyPrinter
         * and our custom HighlightingJsonGenerator for syntax highlighting.
         */
        private String colorize(JsonNode node) {
            try (StringWriter stringWriter = new StringWriter()) {
                try (JsonGenerator baseGen = jsonFactory.createGenerator(stringWriter)) {
                    HighlightingJsonGenerator highlightingGen = new HighlightingJsonGenerator(baseGen);
                    prettyWriter.writeValue(highlightingGen, node);
                }
                return stringWriter.toString();
            } catch (IOException e) {
                log.warn("Failed to colorize JSON, falling back to basic pretty print", e);
                try {
                    return prettyWriter.writeValueAsString(node);
                } catch (IOException ex) {
                    return node.toString();
                }
            }
        }

        public void handle(HttpExchange t) {
            try {
                InputStream body = t.getRequestBody();
                if (metricFilter == null && transactionFilter == null) {
                    logRequestBody(body);
                } else {
                    logFiltered(body);
                }

                String response = "{}";
                t.sendResponseHeaders(200, response.length());
                try (OutputStream os = t.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void logRequestBody(InputStream body) throws IOException {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(body))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) {
                        continue;
                    }
                    // Use the class's 'mapper' field
                    var jsonNode = mapper.readTree(line);
                    logger.lifecycle("MockApmServer reading JSON object:\n{}", colorize(jsonNode));
                }
            }
        }

        private void logFiltered(InputStream body) throws IOException {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(body))) {
                String line;
                String tier = null;
                String node = null;

                while ((line = reader.readLine()) != null) {
                    // Use the class's 'mapper' field
                    var jsonNode = mapper.readTree(line);

                    if (jsonNode.has("metadata")) {
                        node = jsonNode.path("metadata").path("service").path("node").path("configured_name").asText(null);
                        tier = jsonNode.path("metadata").path("labels").path("node_tier").asText(null);
                    } else if (transactionFilter != null && jsonNode.has("transaction")) {
                        var transaction = jsonNode.get("transaction");
                        var name = transaction.get("name").asText();
                        if (transactionFilter.matcher(name).matches()
                            && (transactionExcludesFilter == null || transactionExcludesFilter.matcher(name).matches() == false)) {
                            logger.lifecycle("Transaction [{}/{}]:\n{}", node, tier, colorize(transaction));
                        }
                    } else if (metricFilter != null && jsonNode.has("metricset")) {
                        var metricset = jsonNode.get("metricset");
                        var samples = (ObjectNode) metricset.get("samples");
                        java.util.List<String> fieldNames = new java.util.ArrayList<>();
                        samples.fieldNames().forEachRemaining(fieldNames::add);
                        for (String name : fieldNames) {
                            if (metricFilter.matcher(name).matches() == false) {
                                samples.remove(name);
                            }
                        }
                        if (samples.isEmpty() == false) {
                            logger.lifecycle("Metricset [{}/{}]:\n{}", node, tier, colorize(metricset));
                        }
                    }
                }
            }
        }
    }
}
