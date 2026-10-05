/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;
import fixture.azure.MockAzureBlobStore;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.ConcurrentMultipartHelper;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomFiniteRetryingPurpose;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerTests extends AbstractAzureServerTestCase {

    private AzureHttpHandler azureHttpHandler;

    @Before
    public void configureAzureHandler() {
        azureHttpHandler = new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE);
        httpServer.createContext("/", azureHttpHandler);
    }

    public void testCanConfigureReadTimeout() {
        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/account/container/read_blob_read_timeout", exchange -> {
            logger.info("Received request: {} {}", exchange.getRequestMethod(), exchange.getRequestURI());
            Streams.readFully(exchange.getRequestBody());
            if ("HEAD".equals(exchange.getRequestMethod())) {
                sendBlobHeaders(exchange, bytes);
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                exchange.close();
            } else if ("GET".equals(exchange.getRequestMethod())) {
                sendBlobHeaders(exchange, bytes);
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), bytes.length);
                // Send the response headers back then stop (this is required to trigger the read timeout)
                exchange.getResponseBody().flush();
            }
        });

        /*
         * The read timeout should be reflected in the timeout message
         */
        {
            final var tryTimeout = TimeValue.timeValueSeconds(60);
            final var readTimeoutMillis = randomLongBetween(100, 1000);
            final BlobContainer blobContainer = builder().withMaxRetries(0)
                .withTryTimeout(tryTimeout)
                .withReadTimeout(TimeValue.timeValueMillis(readTimeoutMillis))
                .build();
            final long startTimeMillis = System.currentTimeMillis();
            final RuntimeException readBlobException = assertThrows(RuntimeException.class, () -> {
                try (InputStream inputStream = blobContainer.readBlob(randomFiniteRetryingPurpose(), "read_blob_read_timeout")) {
                    assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));
                }
            });
            assertThat(
                readBlobException.getMessage(),
                containsString("Channel read timed out after " + readTimeoutMillis + " milliseconds")
            );
            final long elapsedTimeMillis = System.currentTimeMillis() - startTimeMillis;
            assertThat(elapsedTimeMillis, lessThan(tryTimeout.millis()));
        }
    }

    /**
     * Simulates a request body producer that stalls after the first upload buffer (e.g. slow reads of local files). The upload must be
     * aborted by the configured write timeout rather than the SDK default of 60s.
     */
    public void testCanConfigureWriteTimeout() throws Exception {
        final int uploadBufferSize = ByteSizeUnit.KB.toIntBytes(64);
        final byte[] bytes = randomByteArrayOfLength(uploadBufferSize + randomIntBetween(1, uploadBufferSize));
        httpServer.createContext("/account/container/write_blob_write_timeout", exchange -> {
            logger.info("Received request: {} {}", exchange.getRequestMethod(), exchange.getRequestURI());
            try {
                // blocks until the client gives up and closes the connection
                Streams.readFully(exchange.getRequestBody());
            } catch (IOException e) {
                // expected once the client closes the connection
            } finally {
                exchange.close();
            }
        });

        final var tryTimeout = TimeValue.timeValueSeconds(60);
        final var writeTimeoutMillis = randomLongBetween(100, 1000);
        final BlobContainer blobContainer = builder().withMaxRetries(0)
            .withTryTimeout(tryTimeout)
            .withWriteTimeout(TimeValue.timeValueMillis(writeTimeoutMillis))
            .build();

        final CountDownLatch stalled = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        try {
            final long startTimeMillis = System.currentTimeMillis();
            final IOException writeBlobException = expectThrows(
                IOException.class,
                () -> blobContainer.writeBlobAtomic(
                    randomPurpose(),
                    "write_blob_write_timeout",
                    bytes.length,
                    (offset, length) -> new StallingInputStream(bytes, uploadBufferSize, stalled, release),
                    false,
                    Runnable::run
                )
            );
            assertTrue("the request body producer should have stalled", stalled.await(0, TimeUnit.SECONDS));
            assertThat(
                ExceptionsHelper.stackTrace(writeBlobException),
                containsString("Channel write operation timed out after " + writeTimeoutMillis + " milliseconds")
            );
            final long elapsedTimeMillis = System.currentTimeMillis() - startTimeMillis;
            assertThat(elapsedTimeMillis, lessThan(tryTimeout.millis()));
        } finally {
            release.countDown();
        }
    }

    /**
     * Serves the first {@code stallAfter} bytes, then blocks until released.
     */
    private static class StallingInputStream extends ByteArrayInputStream {
        private final int stallAfter;
        private final CountDownLatch stalled;
        private final CountDownLatch release;

        StallingInputStream(byte[] bytes, int stallAfter, CountDownLatch stalled, CountDownLatch release) {
            super(bytes);
            this.stallAfter = stallAfter;
            this.stalled = stalled;
            this.release = release;
        }

        @Override
        public synchronized int read(byte[] b, int off, int len) {
            if (pos >= stallAfter) {
                stalled.countDown();
                safeAwait(release, TimeValue.timeValueSeconds(30));
            }
            return super.read(b, off, Math.min(len, pos < stallAfter ? stallAfter - pos : len));
        }
    }

    public void testConcurrentMultipartCopySingleThread() throws Exception {
        testConcurrentMultipartCopy(true);
    }

    public void testConcurrentMultipartCopyMultipleThreads() throws Exception {
        testConcurrentMultipartCopy(false);
    }

    private void testConcurrentMultipartCopy(boolean singleThread) throws Exception {
        final AzureBlobContainer blobContainer = asInstanceOf(AzureBlobContainer.class, createBlobContainer(between(1, 3)));
        final AzureBlobStore blobStore = blobContainer.getBlobStore();
        final long partSize = blobStore.getUploadBlockSize();
        final int nbParts = randomIntBetween(2, 5);
        final long blobSize = randomLongBetween((nbParts - 1) * partSize + 1, nbParts * partSize);
        assertThat(ConcurrentMultipartHelper.numberOfParts(blobSize, partSize), equalTo(nbParts));

        final String sourceBlobName = randomIdentifier();
        final String destBlobName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(Math.toIntExact(blobSize));
        blobStore.writeBlob(OperationPurpose.CLUSTER_STATE, sourceBlobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(data)), false);

        final AtomicInteger stageBlockFromUrlCalls = new AtomicInteger();
        // Wrap the default handler so we can count Put Block From URL requests
        final HttpHandler previousHandler = azureHttpHandler;
        httpServer.removeContext("/");
        httpServer.createContext("/", exchange -> {
            final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI();
            if (request.contains("blockid=") && exchange.getRequestHeaders().getFirst("x-ms-copy-source") != null) {
                stageBlockFromUrlCalls.incrementAndGet();
            }
            previousHandler.handle(exchange);
        });

        final int numThreads = singleThread ? 1 : nbParts;
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        try {
            blobContainer.copyBlob(randomPurpose(), blobContainer, sourceBlobName, destBlobName, blobSize, executorService);
        } finally {
            ESTestCase.terminate(executorService);
        }

        assertThat(stageBlockFromUrlCalls.get(), equalTo(nbParts));
        assertArrayEquals(data, BytesReference.toBytes(Streams.readFully(blobContainer.readBlob(randomPurpose(), destBlobName))));
    }

    public void testConcurrentMultipartCopyMissingSource() {
        final AzureBlobContainer blobContainer = asInstanceOf(AzureBlobContainer.class, createBlobContainer(between(1, 3)));
        final long blobSize = blobContainer.getBlobStore().getUploadBlockSize() + 1;
        expectThrows(
            NoSuchFileException.class,
            () -> blobContainer.copyBlob(
                randomPurpose(),
                blobContainer,
                "missing-" + randomIdentifier(),
                randomIdentifier(),
                blobSize,
                Runnable::run
            )
        );
    }

    public void testSmallCopyWithExecutorUsesBeginCopy() throws IOException {
        final AzureBlobContainer blobContainer = asInstanceOf(AzureBlobContainer.class, createBlobContainer(between(1, 3)));
        final AzureBlobStore blobStore = blobContainer.getBlobStore();
        // Below the upload block size, even with an executor we use async Copy Blob (not multipart)
        final byte[] data = randomByteArrayOfLength(between(1, Math.toIntExact(blobStore.getUploadBlockSize())));
        final String sourceBlobName = randomIdentifier();
        final String destBlobName = randomIdentifier();
        blobStore.writeBlob(OperationPurpose.CLUSTER_STATE, sourceBlobName, BytesReference.fromByteBuffer(ByteBuffer.wrap(data)), false);

        blobContainer.copyBlob(randomPurpose(), blobContainer, sourceBlobName, destBlobName, data.length, Runnable::run);
        assertArrayEquals(data, BytesReference.toBytes(Streams.readFully(blobContainer.readBlob(randomPurpose(), destBlobName))));
        assertThat(blobStore.stats().get(AzureBlobStore.Operation.COPY_BLOB.getKey()).operations(), greaterThan(0L));
    }

    protected void sendBlobHeaders(HttpExchange exchange, byte[] blobContents) {
        exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(blobContents.length));
        exchange.getResponseHeaders().add("Content-Length", String.valueOf(blobContents.length));
        exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
        exchange.getResponseHeaders().add("ETag", eTagForContents(blobContents));
    }

    private static String eTagForContents(byte[] blobContents) {
        return Base64.getEncoder().encodeToString(MessageDigests.digest(new BytesArray(blobContents), MessageDigests.md5()));
    }
}
