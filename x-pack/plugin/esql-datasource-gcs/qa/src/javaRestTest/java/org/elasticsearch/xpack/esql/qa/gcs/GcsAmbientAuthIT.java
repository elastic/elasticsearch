/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.gcs;

import fixture.gcs.GoogleCloudStorageHttpHandler;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.gcs.GcsConfiguration;
import org.elasticsearch.xpack.esql.datasource.gcs.GcsStorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end regression guard for {@code auth=ambient} on GCS.
 *
 * <p>Injects a known bearer token via {@code tests.gcs.ambient_access_token} (the
 * test-only system property that {@link GcsStorageProvider#credentials} checks), then
 * reads a blob through the provider and asserts the GCS mock received a request bearing
 * that exact token.
 *
 * <p>The mock GCS server validates the {@code Authorization} header: requests without
 * {@code Bearer <AMBIENT_TOKEN>} return HTTP 401. A successful read therefore proves the
 * ambient credential was used, not an anonymous or fallback identity.
 */
@SuppressForbidden(reason = "uses HttpServer for local GCS mock and System.setProperty for ambient token injection")
public class GcsAmbientAuthIT extends ESTestCase {

    static final String AMBIENT_TOKEN = "gcs-ambient-test-token";
    static final String BUCKET = "test-ambient-bucket";
    static final String OBJECT_KEY = "data/rows.ndjson";

    static final byte[] CONTENT = "{\"id\": 1, \"city\": \"Tokyo\"}\n{\"id\": 2, \"city\": \"Seoul\"}\n".getBytes(StandardCharsets.UTF_8);

    /** Authorization header captured from the most recent GCS request. */
    static final AtomicReference<String> lastAuthorizationHeader = new AtomicReference<>();

    private static HttpServer gcsServer;
    private static int gcsPort;
    private static GcsStorageProvider provider;

    @BeforeClass
    public static void startServer() throws Exception {
        GoogleCloudStorageHttpHandler blobHandler = new GoogleCloudStorageHttpHandler(BUCKET);
        blobHandler.putBlob(OBJECT_KEY, new BytesArray(CONTENT));

        gcsServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        gcsServer.createContext("/", exchange -> {
            String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
            lastAuthorizationHeader.set(authHeader);

            // Reject requests that don't carry the expected ambient token.
            if (authHeader == null || authHeader.contains("Bearer " + AMBIENT_TOKEN) == false) {
                exchange.sendResponseHeaders(401, 0);
                exchange.close();
                return;
            }
            blobHandler.handle(exchange);
        });
        gcsServer.start();
        gcsPort = gcsServer.getAddress().getPort();
    }

    @AfterClass
    public static void stopServer() {
        if (gcsServer != null) {
            gcsServer.stop(0);
            gcsServer = null;
        }
    }

    @Before
    public void seedAmbientToken() throws Exception {
        System.setProperty("tests.gcs.ambient_access_token", AMBIENT_TOKEN);
        GcsConfiguration config = GcsConfiguration.fromMap(
            Map.of("auth", "ambient", "endpoint", "http://localhost:" + gcsPort, "project_id", "test-project")
        );
        provider = new GcsStorageProvider(config);
    }

    @After
    public void clearAmbientToken() throws Exception {
        System.clearProperty("tests.gcs.ambient_access_token");
        if (provider != null) {
            provider.close();
            provider = null;
        }
        lastAuthorizationHeader.set(null);
    }

    /**
     * Core guard: reads a blob through the ambient-auth provider and asserts both
     * the content and the bearer token in the Authorization header.
     */
    public void testAmbientAuthReadsBlob() throws IOException {
        StoragePath path = StoragePath.of("gs://" + BUCKET + "/" + OBJECT_KEY);
        var obj = provider.newObject(path);

        byte[] readBytes;
        try (InputStream stream = obj.newStream()) {
            readBytes = stream.readAllBytes();
        }

        assertArrayEquals("read bytes must match uploaded content", CONTENT, readBytes);

        String authHeader = lastAuthorizationHeader.get();
        assertThat("GCS request must carry an Authorization header", authHeader, notNullValue());
        assertThat(
            "Authorization header must contain the ambient token injected via system property",
            authHeader,
            containsString("Bearer " + AMBIENT_TOKEN)
        );
    }

    /**
     * Counter-proof: a provider built with a DIFFERENT token is rejected by the mock server
     * (HTTP 401), proving the auth gate is enforced end-to-end, not just assumed.
     */
    public void testWrongTokenIsRejected() throws Exception {
        System.setProperty("tests.gcs.ambient_access_token", "wrong-token-rejected-by-server");
        if (provider != null) {
            provider.close();
        }
        GcsConfiguration config = GcsConfiguration.fromMap(
            Map.of("auth", "ambient", "endpoint", "http://localhost:" + gcsPort, "project_id", "test-project")
        );
        provider = new GcsStorageProvider(config);

        StoragePath path = StoragePath.of("gs://" + BUCKET + "/" + OBJECT_KEY);
        Exception e = expectThrows(Exception.class, () -> {
            var obj = provider.newObject(path);
            try (InputStream stream = obj.newStream()) {
                stream.readAllBytes();
            }
        });
        // Any exception is acceptable — 401 from the server propagates as a storage error.
        assertNotNull(e);
    }
}
