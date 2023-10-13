/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.testclusters.apmserver;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * This is a server which just accepts lines of JSON code and if the JSON
 * is valid and the root node is "transaction", then adds that JSON object
 * to a transaction list which is accessible externally to the class.
 *
 * The Elastic agent sends lines of JSON code, and so this mock server
 * can be used as a basic APM server for testing.
 *
 * The HTTP server used is the JDK embedded com.sun.net.httpserver
 */
public class MockApmServer {
    /**
     * Simple main that starts a mock APM server, prints the port it is
     * running on, and exits after 2_000 seconds. This is not needed
     * for testing, it is just a convenient template for trying things out
     * if you want play around.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        MockApmServer server = new MockApmServer();
        System.out.println(server.start());
        server.blockUntilReady();
        Thread.sleep(2_000_000L);
        server.stop();
        server.blockUntilStopped();
    }

    private static volatile HttpServer TheServerInstance;

    private final List<JsonNode> transactions = new ArrayList<>();
    private final List<JsonNode> metricsets = new ArrayList<>();

    /**
     * A count of the number of transactions received and not yet removed
     * @return the number of transactions received and not yet removed
     */
    public int getTransactionCount() {
        synchronized (transactions) {
            return transactions.size();
        }
    }

    /**
     * Gets the transaction at index i if it exists within the timeout
     * specified, and removes it from the transaction list.
     * If it doesn't exist within the timeout period, an
     * IllegalArgumentException is thrown
     * @param i - the index to retrieve a transaction from
     * @param timeOutInMillis - millisecond timeout to wait for the
     *                        transaction at index i to exist
     * @return - the transaction information as a JSON object
     * @throws TimeoutException - thrown if no transaction
     *                         exists at index i by timeout
     */
    public JsonNode getAndRemoveTransaction(int i, long timeOutInMillis) throws TimeoutException {
        //because the agent writes to the server asynchronously,
        //any transaction created in a client is not here immediately
        long start = System.currentTimeMillis();
        long elapsedTime = 0;
        while (elapsedTime < timeOutInMillis) {
            synchronized (transactions) {
                if (transactions.size() > i) {
                    break;
                }
                if (timeOutInMillis-elapsedTime > 0) {
                    try {
                        transactions.wait(timeOutInMillis - elapsedTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                elapsedTime = System.currentTimeMillis() - start;
            }
        }
        synchronized (transactions) {
            if (transactions.size() <= i) {
                throw new TimeoutException("The apm server does not have a transaction at index " + i);
            }
        }
        synchronized (transactions) {
            return transactions.remove(i);
        }
    }

    public JsonNode popMetricset(long timeOutInMillis) throws TimeoutException {
        //because the agent writes to the server asynchronously,
        //any metricset created in a client is not here immediately
        long start = System.currentTimeMillis();
        long elapsedTime = 0;
        while (elapsedTime < timeOutInMillis) {
            synchronized (metricsets) {
                if (metricsets.size() > 0) {
                    break;
                }
                if (timeOutInMillis-elapsedTime > 0) {
                    try {
                        metricsets.wait(timeOutInMillis - elapsedTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                elapsedTime = System.currentTimeMillis() - start;
            }
        }
        if (timeOutInMillis-elapsedTime <= 0) {
            return null;
        }
        synchronized (metricsets) {
            return metricsets.remove(0);
        }
    }

    /**
     * Start the Mock APM server. Just returns empty JSON structures for every incoming message
     * @return - the port the Mock APM server started on
     * @throws IOException
     */
    public synchronized int start() throws IOException {
        if (TheServerInstance != null) {
            throw new IOException("MockApmServer: Ooops, you can't start this instance more than once");
        }
        InetSocketAddress addr = new InetSocketAddress("0.0.0.0", 9999);
        HttpServer server = HttpServer.create(addr, 10);
        server.createContext("/exit", new ExitHandler());
        server.createContext("/", new RootHandler());

        server.start();
        TheServerInstance = server;
        System.out.println("MockApmServer started on port "+server.getAddress().getPort());
        return server.getAddress().getPort();
    }

    public int getPort(){
        return TheServerInstance.getAddress().getPort();
    }

    /**
     * Stop the server gracefully if possible
     */
    public synchronized void stop() {
        System.out.println("stopping!");
        TheServerInstance.stop(1);
        TheServerInstance = null;
    }

    class RootHandler implements HttpHandler {
        public void handle(HttpExchange t) {
            try {
                InputStream body = t.getRequestBody();
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                byte[] buffer = new byte[8*1024];
                int lengthRead;
                while((lengthRead = body.read(buffer)) > 0) {
                    bytes.write(buffer, 0, lengthRead);
                }
                reportTransactionsAndMetrics(bytes.toString());
                String response = "{}";
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void reportTransactionsAndMetrics(String json) {
            String[] lines = json.split("[\r\n]");
            for (String line: lines) {
                reportTransactionOrMetric(line);
            }
        }
        private void reportTransactionOrMetric(String line) {
            System.out.println("MockApmServer reading JSON objects: "+ line);
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode messageRootNode = null;
            try {
                messageRootNode = objectMapper.readTree(line);
                JsonNode transactionNode = messageRootNode.get("transaction");
                if (transactionNode != null) {
                    synchronized (transactions) {
                        transactions.add(transactionNode);
                        transactions.notify();
                    }
                }
                JsonNode metricsetNode = messageRootNode.get("metricset");
                if (metricsetNode != null) {
                    synchronized (metricsets) {
                        metricsets.add(metricsetNode);
                        metricsets.notify();
                    }
                }
            } catch (JsonProcessingException e) {
                System.out.println("Not JSON: "+line);
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
                TheServerInstance.stop(STOP_TIME);
                TheServerInstance = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Wait until the server is ready to accept messages
     */
    public void blockUntilReady() {
        while (TheServerInstance == null) {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
                // do nothing, just enter the next sleep
            }
        }
    }

    /**
     * Wait until the server is terminated
     */
    public void blockUntilStopped() {
        while (TheServerInstance != null) {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
                // do nothing, just enter the next sleep
            }
        }
    }
}
