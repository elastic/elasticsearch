/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.testclusters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.LRUMap;
import com.fasterxml.jackson.databind.util.LookupCache;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.stream.Streams;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    private static final LookupCache<String, String> transactionCache = new LRUMap(16, 16);

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
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            IOUtils.copy(body, bytes);
            logger.lifecycle(("MockApmServer reading JSON objects: " + bytes.toString()));
        }

        private void logFiltered(InputStream body) throws IOException {
            ObjectMapper mapper = new ObjectMapper();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(body))) {
                String line;
                String nodeMetadata = null;

                List<JsonNode> spans = new ArrayList<>();

                while ((line = reader.readLine()) != null) {
                    var jsonNode = mapper.readTree(line);

                    if (jsonNode.has("metadata")) {
                        nodeMetadata = jsonNode.path("metadata").path("service").path("node").path("configured_name").asText(null);
                        var tier = jsonNode.path("metadata").path("labels").path("node_tier").asText(null);
                        nodeMetadata += tier != null ? "/" + tier : "";

                    } else if (transactionFilter != null && jsonNode.has("transaction")) {
                        var transaction = jsonNode.get("transaction");
                        var name = transaction.get("name").asText();
                        if (transactionFilter.matcher(name).matches()
                            && (transactionExcludesFilter == null || transactionExcludesFilter.matcher(name).matches() == false)) {
                            transactionCache.put(transaction.get("id").asText(), name);
                            logger.lifecycle("Transaction {} [{}]: {}", name, nodeMetadata, transaction);
                        }
                    } else if (jsonNode.has("span")) {
                        spans.add(jsonNode.get("span")); // make sure to record all transactions first
                    } else if (metricFilter != null && jsonNode.has("metricset")) {
                        var metricset = jsonNode.get("metricset");
                        var samples = (ObjectNode) metricset.get("samples");
                        for (var name : Streams.of(samples.fieldNames()).toList()) {
                            if (metricFilter.matcher(name).matches() == false) {
                                samples.remove(name);
                            }
                        }
                        if (samples.isEmpty() == false) {
                            logger.lifecycle("Metricset [{}]: {}", nodeMetadata, metricset);
                        }
                    }
                }

                // emit only spans for previously matched transactions using the transaction cache
                for (var span : spans) {
                    var name = span.get("name").asText();
                    var transactionId = span.get("transaction_id").asText();
                    var transactionName = transactionCache.get(transactionId);
                    if (transactionName != null) {
                        logger.lifecycle("Span {} of {} [{}]: {}", name, transactionName, nodeMetadata, span);
                    }
                }
            }
        }
    }
}
