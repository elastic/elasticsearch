/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for AzureStorageObject.
 * Tests constructor validation.
 * Note: Tests that require mocking BlobClient are excluded as it is a final class
 * and cannot be mocked with standard Mockito.
 * <p>
 * Metrics-increment tests use a {@link MockHttpServer} fed to a real {@link BlobClient},
 * matching the real-Azure-SDK-stub style used in {@code AzureStorageObjectAsyncTests} and
 * {@code repository-azure}'s test suite. Reactor-Netty threads started by the Azure SDK
 * are filtered via {@link AzureReactorThreadFilter} plus a local filter for the
 * {@code parallel-N} scheduler threads (not covered by the shared filter).
 */
@SuppressForbidden(reason = "use a http server")
@ThreadLeakFilters(filters = { AzureReactorThreadFilter.class, AzureStorageObjectTests.ReactorParallelThreadFilter.class })
public class AzureStorageObjectTests extends ESTestCase {

    /**
     * Reactor's {@code Schedulers.parallel()} pool spawns {@code parallel-N} threads
     * that are not covered by the shared {@link AzureReactorThreadFilter}.
     */
    public static final class ReactorParallelThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("parallel-");
        }
    }

    public void testConstructorNullBlobClientThrows() {
        StoragePath path = StoragePath.of("wasbs://account.blob.core.windows.net/container/key");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new AzureStorageObject(null, "container", "key", path)
        );
        assertEquals("blobClient cannot be null", e.getMessage());
    }

    /**
     * newStream(pos, length) increments {@link StorageObjectMetrics} request counters and records
     * the requested byte count, mirroring the S3 contract.
     */
    public void testRangeNewStreamIncrementsMetrics() throws IOException {
        long rangeBytes = 1024L;
        byte[] payload = new byte[(int) rangeBytes];
        long fileSize = 100_000L;

        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            // Azure SDK GET with range -> respond 206 with the payload and a Content-Range header.
            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            exchange.getResponseHeaders().add("Content-Range", "bytes 0-" + (rangeBytes - 1) + "/" + fileSize);
            exchange.getResponseHeaders().add("ETag", "\"0x1\"");
            exchange.getResponseHeaders().add("x-ms-creation-time", "Wed, 01 Jan 2026 00:00:00 GMT");
            exchange.sendResponseHeaders(206, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        });
        server.start();
        try {
            BlobClient blobClient = newBlobClient(server, "container", "blob.parquet");
            StoragePath path = StoragePath.of("wasbs://devstoreaccount1.blob.core.windows.net/container/blob.parquet");
            AzureStorageObject obj = new AzureStorageObject(blobClient, "container", "blob.parquet", path, fileSize);

            assertEquals(0L, obj.metrics().requestCount());
            try (InputStream stream = obj.newStream(0, rangeBytes)) {
                // Drain to satisfy the SDK reactor pipeline; we only care that the call completes.
                stream.readAllBytes();
            }

            StorageObjectMetrics metrics = obj.metrics();
            assertEquals(1L, metrics.requestCount());
            assertEquals(rangeBytes, metrics.bytesRead());
            assertTrue("requestNanos should be > 0", metrics.requestNanos() > 0);
            assertEquals(0L, metrics.retryCount());
        } finally {
            server.stop(0);
        }
    }

    public void testReadBytesAsyncConstrainsOverAllocatedDestination() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(32, 512));
        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            exchange.getResponseHeaders().add("Content-Range", "bytes 0-" + (payload.length - 1) + "/" + payload.length);
            exchange.getResponseHeaders().add("ETag", "\"0x1\"");
            exchange.getResponseHeaders().add("x-ms-creation-time", "Wed, 01 Jan 2026 00:00:00 GMT");
            exchange.sendResponseHeaders(206, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        });
        server.start();
        try {
            BlobClient blobClient = newBlobClient(server, "container", "blob.parquet");
            BlobAsyncClient blobAsyncClient = newBlobAsyncClient(server, "container", "blob.parquet");
            StoragePath path = StoragePath.of("wasbs://devstoreaccount1.blob.core.windows.net/container/blob.parquet");
            AzureStorageObject obj = new AzureStorageObject(blobClient, blobAsyncClient, "container", "blob.parquet", path);

            AtomicInteger closeCalls = new AtomicInteger();
            DirectBufferFactory factory = length -> {
                ByteBuffer buffer = ByteBuffer.allocate(length + 17);
                buffer.limit(1);
                return new DirectReadBuffer(buffer, closeCalls::incrementAndGet);
            };
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<DirectReadBuffer> result = new AtomicReference<>();
            AtomicReference<Exception> error = new AtomicReference<>();

            obj.readBytesAsync(0, payload.length, factory, Runnable::run, ActionListener.wrap(buffer -> {
                result.set(buffer);
                latch.countDown();
            }, e -> {
                error.set(e);
                latch.countDown();
            }));

            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertNull(error.get());
            assertNotNull(result.get());
            try (DirectReadBuffer drb = result.get()) {
                assertEquals(payload.length + 17, drb.buffer().capacity());
                assertEquals(payload.length, drb.buffer().remaining());
                byte[] actual = new byte[payload.length];
                drb.buffer().get(actual);
                assertArrayEquals(payload, actual);
            }
            assertEquals(1, closeCalls.get());
        } finally {
            server.stop(0);
        }
    }

    /**
     * Metadata-probe paths (length(), exists(), lastModified()) are intentionally NOT counted in
     * metrics() — they are not data reads. Even when fetchMetadata fails, requestCount stays at 0.
     */
    public void testMetadataProbesDoNotCountAsRequests() throws IOException {
        // 404 the properties HEAD so fetchMetadata sets cachedExists=false fast (single try, no retries).
        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();
        try {
            BlobClient blobClient = newBlobClient(server, "container", "blob.parquet");
            StoragePath path = StoragePath.of("wasbs://devstoreaccount1.blob.core.windows.net/container/blob.parquet");
            AzureStorageObject obj = new AzureStorageObject(blobClient, "container", "blob.parquet", path);

            // length() throws IOException because the blob does not exist; that's fine — we only
            // care that no metrics counter incremented.
            expectThrows(IOException.class, obj::length);
            assertFalse(obj.exists());
            assertNull(obj.lastModified());

            assertEquals(0L, obj.metrics().requestCount());
            assertEquals(0L, obj.metrics().bytesRead());
            assertEquals(0L, obj.metrics().requestNanos());
            assertEquals(0L, obj.metrics().retryCount());
        } finally {
            server.stop(0);
        }
    }

    /**
     * Build a real {@link BlobClient} pointed at a local {@link HttpServer} with retries
     * disabled so unit tests stay fast and deterministic.
     */
    private static BlobClient newBlobClient(HttpServer server, String container, String blobName) {
        return newBlobServiceClientBuilder(server).buildClient().getBlobContainerClient(container).getBlobClient(blobName);
    }

    private static BlobAsyncClient newBlobAsyncClient(HttpServer server, String container, String blobName) {
        return newBlobServiceClientBuilder(server).buildAsyncClient().getBlobContainerAsyncClient(container).getBlobAsyncClient(blobName);
    }

    private static BlobServiceClientBuilder newBlobServiceClientBuilder(HttpServer server) {
        String endpoint = "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/devstoreaccount1";
        // Azurite's well-known dev-storage shared key — value comes from public Azure SDK docs.
        // Used here only to satisfy the SDK's auth-header-signing pipeline against our local server.
        String connectionString = "DefaultEndpointsProtocol=http;"
            + "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            + "BlobEndpoint="
            + endpoint;
        // maxTries=1 -> no retries, so a 404/transient failure surfaces immediately.
        RequestRetryOptions noRetries = new RequestRetryOptions(
            RetryPolicyType.FIXED,
            1,
            (int) Duration.ofSeconds(5).getSeconds(),
            1L,
            1L,
            null
        );
        return new BlobServiceClientBuilder().connectionString(connectionString).retryOptions(noRetries);
    }
}
