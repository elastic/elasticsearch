/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase.randomBytes;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * This class tests how a {@link AzureBlobContainer} and its underlying SDK client are retrying requests when reading or writing blobs.
 */
@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerRetriesTests extends AbstractAzureServerTestCase {

    public void testReadNonexistentBlobThrowsNoSuchFileException() {
        final BlobContainer blobContainer = createBlobContainer(between(1, 5));
        final Exception exception = expectThrows(NoSuchFileException.class, () -> {
            if (randomBoolean()) {
                blobContainer.readBlob("read_nonexistent_blob");
            } else {
                final long position = randomLongBetween(0, MAX_RANGE_VAL - 1L);
                final long length = randomLongBetween(1, MAX_RANGE_VAL - position);
                blobContainer.readBlob("read_nonexistent_blob", position, length);
            }
        });
        assertThat(exception.toString(), exception.getMessage().toLowerCase(Locale.ROOT), containsString("not found"));
    }

    public void testReadBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(1, 5);
        final CountDown countDownHead = new CountDown(maxRetries);
        final CountDown countDownGet = new CountDown(maxRetries);
        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/account/container/read_blob_max_retries", exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    if (countDownHead.countDown()) {
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(bytes.length));
                        exchange.getResponseHeaders().add("Content-Length", String.valueOf(bytes.length));
                        exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                        return;
                    }
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    if (countDownGet.countDown()) {
                        final int rangeStart = getRangeStart(exchange);
                        assertThat(rangeStart, lessThan(bytes.length));
                        final int length = bytes.length - rangeStart;
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(length));
                        exchange.getResponseHeaders().add("Content-Length", String.valueOf(length));
                        exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                        exchange.getResponseHeaders().add("ETag", UUIDs.base64UUID());
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), length);
                        exchange.getResponseBody().write(bytes, rangeStart, length);
                        return;
                    }
                }
                if (randomBoolean()) {
                    AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
                }
            } finally {
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        try (InputStream inputStream = blobContainer.readBlob("read_blob_max_retries")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));
            assertThat(countDownHead.isCountedDown(), is(true));
            assertThat(countDownGet.isCountedDown(), is(true));
        }
    }

    public void testReadRangeBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(1, 5);
        final CountDown countDownGet = new CountDown(maxRetries);
        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/account/container/read_range_blob_max_retries", exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    throw new AssertionError("Should not HEAD blob for ranged reads");
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    if (countDownGet.countDown()) {
                        final int rangeStart = getRangeStart(exchange);
                        assertThat(rangeStart, lessThan(bytes.length));
                        final Optional<Integer> rangeEnd = getRangeEnd(exchange);
                        assertThat(rangeEnd.isPresent(), is(true));
                        assertThat(rangeEnd.get(), greaterThanOrEqualTo(rangeStart));
                        final int length = (rangeEnd.get() - rangeStart) + 1;
                        assertThat(length, lessThanOrEqualTo(bytes.length - rangeStart));
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.getResponseHeaders()
                            .add("Content-Range", "bytes " + rangeStart + "-" + (rangeStart + rangeEnd.get() + 1) + "/" + bytes.length);
                        exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(length));
                        exchange.getResponseHeaders().add("Content-Length", String.valueOf(length));
                        exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                        exchange.getResponseHeaders().add("ETag", UUIDs.base64UUID());
                        exchange.sendResponseHeaders(RestStatus.PARTIAL_CONTENT.getStatus(), length);
                        exchange.getResponseBody().write(bytes, rangeStart, length);
                        return;
                    }
                }
                if (randomBoolean()) {
                    AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
                }
            } finally {
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        final int position = randomIntBetween(0, bytes.length - 1);
        final int length = randomIntBetween(1, bytes.length - position);
        try (InputStream inputStream = blobContainer.readBlob("read_range_blob_max_retries", position, length)) {
            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(inputStream));
            assertArrayEquals(Arrays.copyOfRange(bytes, position, Math.min(bytes.length, position + length)), bytesRead);
            assertThat(countDownGet.isCountedDown(), is(true));
        }
    }

    public void testWriteBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(1, 5);
        final CountDown countDown = new CountDown(maxRetries);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/account/container/write_blob_max_retries", exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                if (countDown.countDown()) {
                    final BytesReference body = Streams.readFully(exchange.getRequestBody());
                    if (Objects.deepEquals(bytes, BytesReference.toBytes(body))) {
                        exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                        exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
                    } else {
                        AzureHttpHandler.sendError(exchange, RestStatus.BAD_REQUEST);
                    }
                    exchange.close();
                    return;
                }

                if (randomBoolean()) {
                    if (randomBoolean()) {
                        Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, Math.max(1, bytes.length - 1))]);
                    } else {
                        Streams.readFully(exchange.getRequestBody());
                        AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
                    }
                }
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
            blobContainer.writeBlob("write_blob_max_retries", stream, bytes.length, false);
        }
        assertThat(countDown.isCountedDown(), is(true));
    }

    public void testWriteLargeBlob() throws Exception {
        final int maxRetries = randomIntBetween(2, 5);

        final byte[] data = randomBytes(ByteSizeUnit.MB.toIntBytes(10) + randomIntBetween(0, ByteSizeUnit.MB.toIntBytes(1)));
        int nbBlocks = data.length / ByteSizeUnit.MB.toIntBytes(1);
        if (data.length % ByteSizeUnit.MB.toIntBytes(1) != 0) {
            nbBlocks += 1;
        }

        final int nbErrors = 2; // we want all requests to fail at least once
        final AtomicInteger countDownUploads = new AtomicInteger(nbErrors * nbBlocks);
        final CountDown countDownComplete = new CountDown(nbErrors);

        final Map<String, BytesReference> blocks = new ConcurrentHashMap<>();
        httpServer.createContext("/account/container/write_large_blob", exchange -> {

            if ("PUT".equals(exchange.getRequestMethod())) {
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getRawQuery(), 0, params);

                final String blockId = params.get("blockid");
                assert Strings.hasText(blockId) == false || AzureFixtureHelper.assertValidBlockId(blockId);

                if (Strings.hasText(blockId) && (countDownUploads.decrementAndGet() % 2 == 0)) {
                    blocks.put(blockId, Streams.readFully(exchange.getRequestBody()));
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
                    exchange.close();
                    return;
                }

                final String complete = params.get("comp");
                if ("blocklist".equals(complete) && (countDownComplete.countDown())) {
                    final String blockList = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), UTF_8));
                    final List<String> blockUids = Arrays.stream(blockList.split("<Latest>"))
                        .filter(line -> line.contains("</Latest>"))
                        .map(line -> line.substring(0, line.indexOf("</Latest>")))
                        .collect(Collectors.toList());

                    final ByteArrayOutputStream blob = new ByteArrayOutputStream();
                    for (String blockUid : blockUids) {
                        BytesReference block = blocks.remove(blockUid);
                        assert block != null;
                        block.writeTo(blob);
                    }
                    assertArrayEquals(data, blob.toByteArray());
                    exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
                    exchange.close();
                    return;
                }
            }

            if (randomBoolean()) {
                Streams.readFully(exchange.getRequestBody());
                AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
            } else {
                long contentLength = Long.parseLong(exchange.getRequestHeaders().getFirst("Content-Length"));
                readFromInputStream(exchange.getRequestBody(), randomLongBetween(0, contentLength));
            }
            exchange.close();
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries);

        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", data), data.length)) {
            blobContainer.writeBlob("write_large_blob", stream, data.length, false);
        }

        assertThat(countDownUploads.get(), equalTo(0));
        assertThat(countDownComplete.isCountedDown(), is(true));
        assertThat(blocks.isEmpty(), is(true));
    }

    public void testWriteLargeBlobStreaming() throws Exception {
        final int maxRetries = randomIntBetween(2, 5);

        final int blobSize = (int) ByteSizeUnit.MB.toBytes(10);
        final byte[] data = randomBytes(blobSize);

        final int nbErrors = 2; // we want all requests to fail at least once
        final AtomicInteger counterUploads = new AtomicInteger(0);
        final AtomicLong bytesReceived = new AtomicLong(0L);
        final CountDown countDownComplete = new CountDown(nbErrors);

        final Map<String, BytesReference> blocks = new ConcurrentHashMap<>();
        httpServer.createContext("/account/container/write_large_blob_streaming", exchange -> {

            if ("PUT".equals(exchange.getRequestMethod())) {
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getRawQuery(), 0, params);

                final String blockId = params.get("blockid");
                assert Strings.hasText(blockId) == false || AzureFixtureHelper.assertValidBlockId(blockId);

                if (Strings.hasText(blockId) && (counterUploads.incrementAndGet() % 2 == 0)) {
                    final BytesReference blockData = Streams.readFully(exchange.getRequestBody());
                    blocks.put(blockId, blockData);
                    bytesReceived.addAndGet(blockData.length());
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
                    exchange.close();
                    return;
                }

                final String complete = params.get("comp");
                if ("blocklist".equals(complete) && (countDownComplete.countDown())) {
                    final String blockList = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), UTF_8));
                    final List<String> blockUids = Arrays.stream(blockList.split("<Latest>"))
                        .filter(line -> line.contains("</Latest>"))
                        .map(line -> line.substring(0, line.indexOf("</Latest>")))
                        .collect(Collectors.toList());

                    final ByteArrayOutputStream blob = new ByteArrayOutputStream();
                    for (String blockUid : blockUids) {
                        BytesReference block = blocks.remove(blockUid);
                        assert block != null;
                        block.writeTo(blob);
                    }
                    assertArrayEquals(data, blob.toByteArray());
                    exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
                    exchange.close();
                    return;
                }
            }

            if (randomBoolean()) {
                Streams.readFully(exchange.getRequestBody());
                AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
            }
            exchange.close();
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        blobContainer.writeBlob("write_large_blob_streaming", false, randomBoolean(), out -> {
            int outstanding = data.length;
            while (outstanding > 0) {
                if (randomBoolean()) {
                    int toWrite = Math.toIntExact(Math.min(randomIntBetween(64, data.length), outstanding));
                    out.write(data, data.length - outstanding, toWrite);
                    outstanding -= toWrite;
                } else {
                    out.write(data[data.length - outstanding]);
                    outstanding--;
                }
            }
        });
        assertEquals(blobSize, bytesReceived.get());
    }

    public void testRetryUntilFail() throws Exception {
        final int maxRetries = randomIntBetween(2, 5);
        final AtomicInteger requestsReceived = new AtomicInteger(0);
        httpServer.createContext("/account/container/write_blob_max_retries", exchange -> {
            try {
                requestsReceived.incrementAndGet();
                if (Streams.readFully(exchange.getRequestBody()).length() > 0) {
                    throw new AssertionError("Should not receive any data");
                }
            } catch (IOException e) {
                // Suppress the exception since it's expected that the
                // connection is closed before anything can be read
            } finally {
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        try (InputStream stream = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("foo");
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public void reset() {}
        }) {
            final IOException ioe = expectThrows(
                IOException.class,
                () -> blobContainer.writeBlob("write_blob_max_retries", stream, randomIntBetween(1, 128), randomBoolean())
            );
            assertThat(ioe.getMessage(), is("Unable to write blob write_blob_max_retries"));
            // The mock http server uses 1 thread to process the requests, it's possible that the
            // call to writeBlob throws before all the requests have been processed in the http server,
            // as the http server thread might get de-scheduled and the sdk keeps sending requests
            // as it fails to read the InputStream to write.
            assertBusy(() -> assertThat(requestsReceived.get(), equalTo(maxRetries + 1)));
        }
    }

    public void testRetryFromSecondaryLocationPolicies() throws Exception {
        final int maxRetries = randomIntBetween(1, 5);
        final AtomicInteger failedHeadCalls = new AtomicInteger();
        final AtomicInteger failedGetCalls = new AtomicInteger();
        final byte[] bytes = randomBlobContent();

        HttpHandler failingHandler = exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    failedHeadCalls.incrementAndGet();
                    AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    failedGetCalls.incrementAndGet();
                    AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
                }
            } finally {
                exchange.close();
            }
        };

        HttpHandler workingHandler = exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(bytes.length));
                    exchange.getResponseHeaders().add("Content-Length", String.valueOf(bytes.length));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    final int rangeStart = getRangeStart(exchange);
                    assertThat(rangeStart, lessThan(bytes.length));
                    final int length = bytes.length - rangeStart;
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(length));
                    exchange.getResponseHeaders().add("Content-Length", String.valueOf(length));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.getResponseHeaders().add("ETag", UUIDs.base64UUID());
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), length);
                    exchange.getResponseBody().write(bytes, rangeStart, length);
                }
            } finally {
                exchange.close();
            }
        };
        LocationMode locationMode = randomFrom(LocationMode.PRIMARY_THEN_SECONDARY, LocationMode.SECONDARY_THEN_PRIMARY);

        String secondaryHost = null;
        String blobPath = "/account/container/read_blob_from_secondary";
        if (locationMode == LocationMode.PRIMARY_THEN_SECONDARY) {
            httpServer.createContext(blobPath, failingHandler);
            secondaryHttpServer.createContext(blobPath, workingHandler);
            // The SDK doesn't work well with secondary host endpoints that contain
            // a path, that's the reason why we sould provide just the host + port;
            secondaryHost = getEndpointForServer(secondaryHttpServer, "account");
        } else if (locationMode == LocationMode.SECONDARY_THEN_PRIMARY) {
            secondaryHttpServer.createContext(blobPath, failingHandler);
            httpServer.createContext(blobPath, workingHandler);
            secondaryHost = getEndpointForServer(httpServer, "account");
        }

        final BlobContainer blobContainer = createBlobContainer(maxRetries, secondaryHost, locationMode);
        try (InputStream inputStream = blobContainer.readBlob("read_blob_from_secondary")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));

            // It does round robin, first tries on the primary, then on the secondary
            assertThat(failedHeadCalls.get(), equalTo(1));
            assertThat(failedGetCalls.get(), equalTo(1));
        }
    }
}
