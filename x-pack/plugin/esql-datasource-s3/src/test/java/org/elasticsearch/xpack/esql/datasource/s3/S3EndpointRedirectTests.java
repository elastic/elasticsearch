/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Whether a read follows an HTTP redirect away from the configured endpoint.
 *
 * <p>The endpoint constraint in {@link S3EndpointCheck} confines the host a data source may name. It is
 * worth only as much as the guarantee that the node then talks to that host and no other, so this pins
 * the answer: a permitted endpoint that answers with a redirect to an unrelated host must not cause the
 * node to contact that host.
 *
 * <p>Two servers. The first stands in for the configured endpoint and answers every request with a
 * redirect to the second; the second counts the requests it receives. A read is then driven through the
 * provider and the second server's count must stay at zero.
 */
@SuppressForbidden(reason = "an in-process HTTP server is the only way to return a redirect to the SDK's own client")
public class S3EndpointRedirectTests extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/test.parquet";

    public void testReadDoesNotFollowRedirectAwayFromTheEndpoint() throws Exception {
        for (int status : List.of(301, 302, 303, 307, 308)) {
            assertRedirectNotFollowed(status, false);
        }
    }

    public void testReadDoesNotFollowAnS3CrossRegionRedirect() throws Exception {
        // S3 answers a wrong-region request with 301 carrying x-amz-bucket-region, which the SDK's
        // cross-region decorator understands. The provider enables that decorator only when no endpoint
        // is configured, so with one set this must be inert too.
        assertRedirectNotFollowed(301, true);
    }

    private void assertRedirectNotFollowed(int status, boolean withBucketRegionHeader) throws Exception {
        AtomicInteger elsewhereHits = new AtomicInteger();
        HttpServer elsewhere = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        elsewhere.createContext("/", exchange -> {
            elsewhereHits.incrementAndGet();
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });
        elsewhere.start();

        AtomicInteger redirectorHits = new AtomicInteger();
        HttpServer redirector = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        String elsewhereUrl = "http://" + addressOf(elsewhere);
        redirector.createContext("/", exchange -> {
            redirectorHits.incrementAndGet();
            exchange.getResponseHeaders().add("Location", elsewhereUrl + exchange.getRequestURI().getPath());
            if (withBucketRegionHeader) {
                exchange.getResponseHeaders().add("x-amz-bucket-region", "eu-west-1");
            }
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        redirector.start();

        try {
            S3Configuration config = S3Configuration.fromMap(Map.of("auth", "anonymous", "endpoint", "http://" + addressOf(redirector)));
            try (S3StorageProvider provider = new S3StorageProvider(config)) {
                try {
                    provider.exists(StoragePath.of("s3://" + BUCKET + "/" + KEY));
                } catch (IOException | RuntimeException expected) {
                    // The redirect is not a usable S3 response; failing is the correct outcome. What is
                    // under test is where the node went, not whether the read succeeded.
                }
            }
            // Positive control. Without it a zero count at the second server would also be what a read
            // that never left the node looks like, and the assertion below would prove nothing.
            assertTrue("the read never reached the configured endpoint, so this says nothing about redirects", redirectorHits.get() > 0);
            assertEquals("a " + status + " redirect was followed to a host the endpoint setting never named", 0, elsewhereHits.get());
        } finally {
            redirector.stop(0);
            elsewhere.stop(0);
        }
    }

    private static String addressOf(HttpServer server) {
        return server.getAddress().getAddress().getHostAddress() + ":" + server.getAddress().getPort();
    }
}
