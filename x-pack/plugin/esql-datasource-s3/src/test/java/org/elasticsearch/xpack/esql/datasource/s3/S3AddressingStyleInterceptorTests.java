/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

import org.elasticsearch.test.ESTestCase;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * No-network tests for the addressing_style setting.
 *
 * <p>A {@link CapturingInterceptor} fires in {@code beforeTransmission} — after the outbound
 * URL is fully resolved and signed but before any socket is opened. It stores the URI and throws
 * to abort. This lets us assert bucket-in-host (virtual-hosted) vs bucket-in-path (path-style)
 * for every combination of endpoint and addressing_style without running a server.
 *
 * <p>Matrix:
 * <pre>
 *   endpoint absent  × auto (default)   → bucket in host  (SDK default: virtual-hosted)
 *   endpoint absent  × auto (explicit)  → bucket in host
 *   endpoint absent  × path             → bucket in path
 *   endpoint absent  × virtual_hosted   → bucket in host
 *   endpoint present × auto (default)   → bucket in path  (auto: path-style when endpoint is set)
 *   endpoint present × auto (explicit)  → bucket in path
 *   endpoint present × path             → bucket in path
 *   endpoint present × virtual_hosted   → bucket in host
 * </pre>
 *
 * <p>The endpoint used in the "present" cases is {@code http://s3.example.com} — a named host, not
 * a bare IP — so the SDK can actually apply virtual-hosted addressing (bare IPs always fall back
 * to path-style; that behaviour is covered by {@link S3AddressingStyleTests}).
 */
public class S3AddressingStyleInterceptorTests extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/test.parquet";
    /** Named-host endpoint used for the "endpoint present" cases; no real server runs there. */
    private static final String NAMED_ENDPOINT = "http://s3.example.com";

    // ── endpoint absent ───────────────────────────────────────────────────────

    public void testNoEndpointAutoDefaultIsVirtualHosted() {
        assertBucketInHost(captureUri(null, null), "absent (auto), no endpoint");
    }

    public void testNoEndpointExplicitAutoIsVirtualHosted() {
        assertBucketInHost(captureUri(null, "auto"), "auto, no endpoint");
    }

    public void testNoEndpointPathIsPathStyle() {
        assertBucketInPath(captureUri(null, "path"), "path, no endpoint");
    }

    public void testNoEndpointVirtualHostedIsVirtualHosted() {
        assertBucketInHost(captureUri(null, "virtual_hosted"), "virtual_hosted, no endpoint");
    }

    // ── endpoint present (named host) ─────────────────────────────────────────

    public void testWithEndpointAutoDefaultIsPathStyle() {
        assertBucketInPath(captureUri(NAMED_ENDPOINT, null), "absent (auto), endpoint set");
    }

    public void testWithEndpointExplicitAutoIsPathStyle() {
        assertBucketInPath(captureUri(NAMED_ENDPOINT, "auto"), "auto, endpoint set");
    }

    public void testWithEndpointPathIsPathStyle() {
        assertBucketInPath(captureUri(NAMED_ENDPOINT, "path"), "path, endpoint set");
    }

    public void testWithEndpointVirtualHostedIsVirtualHosted() {
        assertBucketInHost(captureUri(NAMED_ENDPOINT, "virtual_hosted"), "virtual_hosted, endpoint set");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private URI captureUri(String endpoint, String addressingStyle) {
        Map<String, Object> raw = new HashMap<>();
        raw.put("auth", "anonymous");
        if (endpoint != null) raw.put("endpoint", endpoint);
        if (addressingStyle != null) raw.put("addressing_style", addressingStyle);
        S3Configuration config = S3Configuration.fromMap(raw);

        CapturingInterceptor interceptor = new CapturingInterceptor();
        try (
            S3Client client = S3StorageProvider.configureCommon(
                S3Client.builder(),
                config,
                AnonymousCredentialsProvider.create(),
                List.of(interceptor)
            ).build()
        ) {
            try {
                client.headObject(HeadObjectRequest.builder().bucket(BUCKET).key(KEY).build());
                fail("expected interceptor to abort before any network I/O");
            } catch (Exception e) {
                // Walk the cause chain: the SDK wraps our AbortException in a SdkClientException.
                // Any exception that doesn't originate from AbortException is unexpected.
                Throwable t = e;
                while (t != null) {
                    if (t instanceof AbortException) break;
                    t = t.getCause();
                }
                if (t == null) {
                    throw new AssertionError("unexpected exception — interceptor may not have been called", e);
                }
            }
        }
        assertNotNull("Interceptor was not invoked for endpoint=" + endpoint + " style=" + addressingStyle, interceptor.capturedUri);
        return interceptor.capturedUri;
    }

    private static void assertBucketInHost(URI uri, String label) {
        assertTrue("Expected bucket in host (virtual-hosted) for [" + label + "], got URI: " + uri, uri.getHost().startsWith(BUCKET + "."));
    }

    private static void assertBucketInPath(URI uri, String label) {
        assertTrue(
            "Expected bucket in path (path-style) for [" + label + "], got URI: " + uri,
            uri.getPath().startsWith("/" + BUCKET + "/")
        );
    }

    private static final class CapturingInterceptor implements ExecutionInterceptor {
        URI capturedUri;

        @Override
        public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            capturedUri = context.httpRequest().getUri();
            throw new AbortException();
        }
    }

    /** Thrown by {@link CapturingInterceptor} to abort the request before any network I/O. */
    private static final class AbortException extends RuntimeException {
        AbortException() {
            super(null, null, true, false);
        }
    }
}
