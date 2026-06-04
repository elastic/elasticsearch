/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link TikaServerExtractionBackend} using an in-process mock HTTP server.
 */
public class TikaServerExtractionBackendTests extends ESTestCase {

    private static HttpServer mockServer;
    /** Mutable holder so individual tests can install their own handler. */
    private static volatile HttpHandlerRef currentHandler;

    @BeforeClass
    public static void startMockServer() throws IOException {
        mockServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        mockServer.createContext("/tika/json", exchange -> {
            HttpHandlerRef h = currentHandler;
            if (h != null) {
                h.handle(exchange);
            } else {
                exchange.sendResponseHeaders(500, 0);
                exchange.close();
            }
        });
        mockServer.start();
    }

    @AfterClass
    public static void stopMockServer() {
        if (mockServer != null) {
            mockServer.stop(0);
        }
    }

    public void testSuccessfulExtraction() throws Exception {
        String json = """
            {
              "X-TIKA:content": "Hello World\\n",
              "Content-Type": "text/plain",
              "dc:creator": "Test Author",
              "dcterms:created": "2024-01-01T00:00:00Z"
            }
            """;
        installJsonHandler(200, json);

        ExtractionResult result = extractSync("test content".getBytes(StandardCharsets.UTF_8), "test.txt", -1);

        // content should be stored as-is (trimming happens in applyResult)
        assertThat(result.content(), is("Hello World\n"));
        assertThat(result.metadata().get("Content-Type"), is("text/plain"));
        assertThat(result.metadata().get("dc:creator"), is("Test Author"));
        assertThat(result.metadata().get("dcterms:created"), is("2024-01-01T00:00:00Z"));
        // X-TIKA:content must not appear in the metadata map
        assertThat(result.metadata().get(TikaServerExtractionBackend.TIKA_CONTENT_KEY), nullValue());
    }

    public void testMultiValuedMetadataFieldTakesFirst() throws Exception {
        String json = """
            {
              "X-TIKA:content": "",
              "dc:creator": ["First Author", "Second Author"]
            }
            """;
        installJsonHandler(200, json);

        ExtractionResult result = extractSync(new byte[1], null, -1);
        assertThat(result.metadata().get("dc:creator"), is("First Author"));
    }

    public void testClientSideTruncation() throws Exception {
        String longContent = "A".repeat(200);
        String json = "{\"X-TIKA:content\": \"" + longContent + "\"}";
        installJsonHandler(200, json);

        ExtractionResult result = extractSync(new byte[1], null, 100);
        assertThat(result.content().length(), is(100));
    }

    public void testNon200ResponseBecomesFailure() throws Exception {
        installFixedStatusHandler(422);

        Exception ex = extractFailure(new byte[1], null, -1);
        assertThat(ex, instanceOf(IOException.class));
        assertThat(ex.getMessage(), containsString("422"));
    }

    public void testServerErrorResponseBecomesFailure() throws Exception {
        installFixedStatusHandler(503);

        Exception ex = extractFailure(new byte[1], null, -1);
        assertThat(ex, instanceOf(IOException.class));
        assertThat(ex.getMessage(), containsString("503"));
    }

    public void testEmptyJsonObjectReturnsEmptyResult() throws Exception {
        installJsonHandler(200, "{}");

        ExtractionResult result = extractSync(new byte[1], null, -1);
        assertThat(result.content(), is(""));
        assertThat(result.metadata().isEmpty(), is(true));
    }

    // ---------- helpers ----------

    private URI serverUri() {
        InetSocketAddress addr = mockServer.getAddress();
        return URI.create("http://" + addr.getHostString() + ":" + addr.getPort());
    }

    private TikaServerExtractionBackend newBackend() {
        return new TikaServerExtractionBackend(serverUri(), Duration.ofSeconds(30), Duration.ofSeconds(5));
    }

    private void installJsonHandler(int statusCode, String json) {
        currentHandler = exchange -> {
            byte[] response = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        };
    }

    private void installFixedStatusHandler(int statusCode) {
        currentHandler = exchange -> {
            exchange.sendResponseHeaders(statusCode, 0);
            exchange.close();
        };
    }

    private ExtractionResult extractSync(byte[] content, String resourceName, int maxChars) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ExtractionResult> resultRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        try (TikaServerExtractionBackend backend = newBackend()) {
            backend.extract(content, resourceName, maxChars, ActionListener.wrap(r -> {
                resultRef.set(r);
                latch.countDown();
            }, e -> {
                exceptionRef.set(e);
                latch.countDown();
            }));
            assertTrue("tika-server call timed out", latch.await(10, TimeUnit.SECONDS));
        }

        assertThat("unexpected extraction failure: " + exceptionRef.get(), exceptionRef.get(), nullValue());
        assertThat(resultRef.get(), notNullValue());
        return resultRef.get();
    }

    private Exception extractFailure(byte[] content, String resourceName, int maxChars) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        try (TikaServerExtractionBackend backend = newBackend()) {
            backend.extract(content, resourceName, maxChars, ActionListener.wrap(r -> latch.countDown(), e -> {
                exceptionRef.set(e);
                latch.countDown();
            }));
            assertTrue("tika-server call timed out", latch.await(10, TimeUnit.SECONDS));
        }

        assertThat("expected an extraction failure", exceptionRef.get(), notNullValue());
        return exceptionRef.get();
    }

    @FunctionalInterface
    private interface HttpHandlerRef {
        void handle(com.sun.net.httpserver.HttpExchange exchange) throws IOException;
    }
}
