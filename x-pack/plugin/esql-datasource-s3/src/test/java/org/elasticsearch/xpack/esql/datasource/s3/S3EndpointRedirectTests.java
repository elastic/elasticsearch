/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.hasItem;

/**
 * Whether a read follows an HTTP redirect away from the configured endpoint.
 *
 * <p>The endpoint constraint in {@link S3EndpointCheck} confines the host a data source may name. It is
 * worth only as much as the guarantee that the node then talks to that host and no other, so this pins
 * the answer: a permitted endpoint that answers with a redirect to an unrelated host must not cause the
 * node to contact that host.
 *
 * <p>Two servers. The first stands in for the configured endpoint and answers every request with a
 * redirect to the second; the second counts the requests it receives. A request is then driven through
 * the provider and the second server's count must stay at zero.
 *
 * <p>Both HTTP stacks are covered, because the provider builds two clients and they are different
 * implementations: an Apache sync client for metadata calls such as {@code exists}, and a netty-nio async
 * client used exclusively for the range reads that carry object data. Redirect handling is a property of
 * each stack, so exercising one says nothing about the other, and the async one is the path that matters
 * for moving bytes to a host nobody named.
 */
@SuppressForbidden(reason = "an in-process HTTP server is the only way to return a redirect to the SDK's own client")
public class S3EndpointRedirectTests extends ESTestCase {

    private static final String BUCKET = "test-bucket";
    private static final String KEY = "data/test.parquet";

    public void testSyncClientDoesNotFollowRedirectAwayFromTheEndpoint() throws Exception {
        for (int status : List.of(301, 302, 303, 307, 308)) {
            assertRedirectNotFollowed(status, false, Driver.SYNC);
        }
    }

    public void testAsyncReadDoesNotFollowRedirectAwayFromTheEndpoint() throws Exception {
        // The range-read path: the one that carries object data.
        for (int status : List.of(301, 302, 303, 307, 308)) {
            assertRedirectNotFollowed(status, false, Driver.ASYNC_READ);
        }
    }

    public void testNeitherClientFollowsAnS3CrossRegionRedirect() throws Exception {
        // S3 answers a wrong-region request with 301 carrying x-amz-bucket-region, which the SDK's
        // cross-region decorator understands. The provider enables that decorator only when no endpoint
        // is configured, so with one set it must be inert on both stacks.
        assertRedirectNotFollowed(301, true, Driver.SYNC);
        assertRedirectNotFollowed(301, true, Driver.ASYNC_READ);
    }

    private enum Driver {
        SYNC,
        ASYNC_READ
    }

    private void assertRedirectNotFollowed(int status, boolean withBucketRegionHeader, Driver driver) throws Exception {
        AtomicInteger elsewhereHits = new AtomicInteger();
        HttpServer elsewhere = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        elsewhere.createContext("/", exchange -> {
            elsewhereHits.incrementAndGet();
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });
        elsewhere.start();

        List<String> redirectorRequests = Collections.synchronizedList(new ArrayList<>());
        HttpServer redirector = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        String elsewhereUrl = "http://" + addressOf(elsewhere);
        redirector.createContext("/", exchange -> {
            String range = exchange.getRequestHeaders().getFirst("Range");
            redirectorRequests.add(
                exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath() + (range == null ? "" : " " + range)
            );
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
            StoragePath path = StoragePath.of("s3://" + BUCKET + "/" + KEY);
            try (S3StorageProvider provider = new S3StorageProvider(config)) {
                try {
                    if (driver == Driver.SYNC) {
                        provider.exists(path);
                    } else {
                        readOneRange(provider, path);
                    }
                } catch (Exception expected) {
                    // The redirect is not a usable S3 response; failing is the correct outcome. What is
                    // under test is where the node went, not whether the read succeeded.
                }
            }
            // Positive control, and it has to name the request this driver issues rather than count
            // requests. The async path opens an object before it reads one, and opening it goes out
            // over the sync client, so a count alone stays satisfied by a range read that never ran.
            assertThat(
                driver + ": the request this driver issues never reached the configured endpoint, so this proves nothing",
                redirectorRequests,
                hasItem(driver == Driver.SYNC ? "HEAD /" + BUCKET + "/" + KEY : "GET /" + BUCKET + "/" + KEY + " bytes=0-15")
            );
            assertEquals(driver + ": a " + status + " redirect was followed to a host never named", 0, elsewhereHits.get());
        } finally {
            redirector.stop(0);
            elsewhere.stop(0);
        }
    }

    /** Drives one range read through the async client and waits for it to settle, however it settles. */
    private static void readOneRange(S3StorageProvider provider, StoragePath path) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        DirectBufferFactory factory = DirectBufferFactory.forBreaker(new NoopCircuitBreaker("test"));
        provider.newObject(path, 1024).readBytesAsync(0, 16, factory, Runnable::run, ActionListener.running(done::countDown));
        assertTrue("the async read never settled", done.await(30, TimeUnit.SECONDS));
    }

    private static String addressOf(HttpServer server) {
        return server.getAddress().getAddress().getHostAddress() + ":" + server.getAddress().getPort();
    }
}
