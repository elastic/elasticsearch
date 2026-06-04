/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import fixture.gcs.GoogleCloudStorageHttpHandler;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
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
 * <p>The provider is constructed with an anonymous subclass that overrides
 * {@link GcsStorageProvider#buildAmbientCredentials()} to return a credential bearing a known
 * token. The mock GCS server then validates the {@code Authorization} header on every request:
 * a successful read proves the credential returned by the seam was actually used to sign the
 * GCS call, not silently replaced by an anonymous identity or a different default.
 *
 * <p>This is the same override pattern used by {@code GoogleCloudStorageAdcTests} in
 * {@code repository-gcs} to test ADC fallback against a mock metadata server, except here the
 * seam is {@code buildAmbientCredentials()} (the only network-touching point of the ambient
 * branch) rather than the broader {@code createStorageOptions}.
 *
 * <p>Production code unchanged: {@code buildAmbientCredentials()} is package-private with a
 * single default implementation that returns {@link com.google.auth.oauth2.ComputeEngineCredentials}.
 * Coverage of that default is in {@code GcsStorageProviderTests#testAmbientCredentialsReturnsComputeEngine}.
 */
@SuppressForbidden(reason = "uses HttpServer for local GCS mock")
public class GcsAmbientAuthTests extends ESTestCase {

    static final String AMBIENT_TOKEN = "gcs-ambient-test-token";
    static final String BUCKET = "test-ambient-bucket";
    static final String OBJECT_KEY = "data/rows.ndjson";

    static final byte[] CONTENT = "{\"id\": 1, \"city\": \"Tokyo\"}\n{\"id\": 2, \"city\": \"Seoul\"}\n".getBytes(StandardCharsets.UTF_8);

    /** Authorization header captured from the most recent GCS request. */
    static final AtomicReference<String> lastAuthorizationHeader = new AtomicReference<>();

    private static HttpServer gcsServer;
    private static int gcsPort;
    private GcsStorageProvider provider;

    @BeforeClass
    public static void startServer() throws Exception {
        GoogleCloudStorageHttpHandler blobHandler = new GoogleCloudStorageHttpHandler(BUCKET);
        blobHandler.putBlob(OBJECT_KEY, new BytesArray(CONTENT));

        gcsServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        gcsServer.createContext("/", exchange -> {
            String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
            lastAuthorizationHeader.set(authHeader);

            // Reject requests that don't carry the expected ambient token.
            if (authHeader == null || authHeader.equals("Bearer " + AMBIENT_TOKEN) == false) {
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
    public void resetAuthHeaderCapture() {
        lastAuthorizationHeader.set(null);
    }

    @After
    public void closeProvider() throws Exception {
        if (provider != null) {
            provider.close();
            provider = null;
        }
    }

    /**
     * Builds a provider whose ambient seam returns a credential bearing {@code token}, so a
     * successful GCS read proves the seam-supplied credential reached the wire.
     */
    private static GcsStorageProvider providerWithAmbientToken(String token) {
        GcsConfiguration config = GcsConfiguration.fromMap(
            Map.of("auth", "ambient", "endpoint", "http://localhost:" + gcsPort, "project_id", "test-project")
        );
        return new GcsStorageProvider(config) {
            @Override
            protected Credentials buildAmbientCredentials() {
                return GoogleCredentials.create(new AccessToken(token, null));
            }
        };
    }

    /**
     * Core guard: reads a blob through a provider whose ambient seam returns a known token, and
     * asserts both the content and the bearer token observed by the GCS mock.
     */
    public void testAmbientAuthReadsBlob() throws IOException {
        provider = providerWithAmbientToken(AMBIENT_TOKEN);

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
            "Authorization header must carry the token returned by buildAmbientCredentials()",
            authHeader,
            containsString("Bearer " + AMBIENT_TOKEN)
        );
    }

    /**
     * Counter-proof: a provider whose seam returns a different token is rejected by the mock
     * server (HTTP 401), proving the auth gate is enforced end-to-end and the seam token is
     * actually what reaches the wire.
     */
    public void testWrongTokenIsRejected() throws Exception {
        provider = providerWithAmbientToken("wrong-token-rejected-by-server");

        StoragePath path = StoragePath.of("gs://" + BUCKET + "/" + OBJECT_KEY);
        expectThrows(Exception.class, () -> {
            var obj = provider.newObject(path);
            try (InputStream stream = obj.newStream()) {
                stream.readAllBytes();
            }
        });
        // Confirm the request reached the fixture with the wrong token, not that it failed for
        // an unrelated reason (network, setup, etc.).
        String authHeader = lastAuthorizationHeader.get();
        assertThat("GCS request must have reached the fixture", authHeader, notNullValue());
        assertThat(authHeader, containsString("Bearer wrong-token-rejected-by-server"));
    }
}
