/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.FaultInjectingS3HttpHandler;
import org.elasticsearch.xpack.esql.datasources.FaultInjectingS3HttpHandler.FaultType;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Tests {@link FaultInjectingS3HttpHandler} using a real HTTP server to verify
 * fault injection produces correct HTTP responses.
 */
@SuppressForbidden(reason = "uses HttpServer to test fault injection handler")
public class FaultInjectingS3HttpHandlerIT extends ESTestCase {

    public void testNoFaultReturnsNormalResponse() throws Exception {
        S3HttpHandler s3Handler = new S3HttpHandler("bucket", S3ConsistencyModel.STRONG_MPUS);
        s3Handler.blobs().put("/bucket/data.parquet", new BytesArray("test-content".getBytes(StandardCharsets.UTF_8)));
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(s3Handler);

        try (TestHttpServer server = new TestHttpServer(handler)) {
            HttpURLConnection conn = openConnection(server, "/bucket/data.parquet");
            assertEquals(200, conn.getResponseCode());
        }
    }

    public void testHttp503FaultReturnsServiceUnavailable() throws Exception {
        S3HttpHandler s3Handler = new S3HttpHandler("bucket", S3ConsistencyModel.STRONG_MPUS);
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(s3Handler);
        handler.setFault(FaultType.HTTP_503, 1);

        try (TestHttpServer server = new TestHttpServer(handler)) {
            HttpURLConnection conn = openConnection(server, "/bucket/data.parquet");
            assertEquals(503, conn.getResponseCode());
            assertEquals(0, handler.remainingFaults());

            // Next request should succeed (fault exhausted)
            s3Handler.blobs().put("/bucket/data.parquet", new BytesArray("ok".getBytes(StandardCharsets.UTF_8)));
            HttpURLConnection conn2 = openConnection(server, "/bucket/data.parquet");
            assertEquals(200, conn2.getResponseCode());
        }
    }

    public void testHttp500FaultReturnsInternalError() throws Exception {
        S3HttpHandler s3Handler = new S3HttpHandler("bucket", S3ConsistencyModel.STRONG_MPUS);
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(s3Handler);
        handler.setFault(FaultType.HTTP_500, 1);

        try (TestHttpServer server = new TestHttpServer(handler)) {
            HttpURLConnection conn = openConnection(server, "/bucket/data.parquet");
            assertEquals(500, conn.getResponseCode());
        }
    }

    public void testFaultCountdownDecrementsCorrectly() throws Exception {
        S3HttpHandler s3Handler = new S3HttpHandler("bucket", S3ConsistencyModel.STRONG_MPUS);
        s3Handler.blobs().put("/bucket/data.parquet", new BytesArray("ok".getBytes(StandardCharsets.UTF_8)));
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(s3Handler);
        handler.setFault(FaultType.HTTP_503, 3);

        try (TestHttpServer server = new TestHttpServer(handler)) {
            for (int i = 0; i < 3; i++) {
                HttpURLConnection conn = openConnection(server, "/bucket/data.parquet");
                assertEquals("Request " + i + " should fail", 503, conn.getResponseCode());
                assertEquals(2 - i, handler.remainingFaults());
            }
            HttpURLConnection conn = openConnection(server, "/bucket/data.parquet");
            assertEquals("After faults exhausted, should succeed", 200, conn.getResponseCode());
        }
    }

    public void testPathFilterOnlyAffectsMatchingPaths() throws Exception {
        S3HttpHandler s3Handler = new S3HttpHandler("bucket", S3ConsistencyModel.STRONG_MPUS);
        s3Handler.blobs().put("/bucket/metadata.json", new BytesArray("meta".getBytes(StandardCharsets.UTF_8)));
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(s3Handler);
        handler.setFault(FaultType.HTTP_503, 10, path -> path.endsWith(".parquet"));

        try (TestHttpServer server = new TestHttpServer(handler)) {
            HttpURLConnection conn = openConnection(server, "/bucket/metadata.json");
            assertEquals("Non-matching path should succeed", 200, conn.getResponseCode());
            assertEquals("Fault count unchanged for non-matching path", 10, handler.remainingFaults());
        }
    }

    public void testClearFaultStopsInjection() throws Exception {
        S3HttpHandler s3Handler = new S3HttpHandler("bucket", S3ConsistencyModel.STRONG_MPUS);
        s3Handler.blobs().put("/bucket/data.parquet", new BytesArray("ok".getBytes(StandardCharsets.UTF_8)));
        FaultInjectingS3HttpHandler handler = new FaultInjectingS3HttpHandler(s3Handler);
        handler.setFault(FaultType.HTTP_503, 100);

        try (TestHttpServer server = new TestHttpServer(handler)) {
            HttpURLConnection conn1 = openConnection(server, "/bucket/data.parquet");
            assertEquals(503, conn1.getResponseCode());

            handler.clearFault();

            HttpURLConnection conn2 = openConnection(server, "/bucket/data.parquet");
            assertEquals(200, conn2.getResponseCode());
        }
    }

    @SuppressForbidden(reason = "test HTTP server for fault injection")
    private static HttpURLConnection openConnection(TestHttpServer server, String path) throws IOException {
        URI uri = URI.create("http://localhost:" + server.port() + path);
        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        conn.connect();
        return conn;
    }

    @SuppressForbidden(reason = "test HTTP server for fault injection")
    private static class TestHttpServer implements AutoCloseable {
        private final HttpServer server;

        TestHttpServer(FaultInjectingS3HttpHandler handler) throws IOException {
            server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/", handler);
            server.start();
        }

        int port() {
            return server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
