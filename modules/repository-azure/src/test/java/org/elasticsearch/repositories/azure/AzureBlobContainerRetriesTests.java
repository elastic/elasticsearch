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

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.blobstore.AbstractBlobContainerRetriesTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.fixture.HttpHeaderParser;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.azure.AbstractAzureServerTestCase.getRanges;
import static org.elasticsearch.repositories.azure.AbstractAzureServerTestCase.readFromInputStream;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.CONTAINER_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.LOCATION_MODE_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.MAX_SINGLE_PART_UPLOAD_SIZE_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ENDPOINT_SUFFIX_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.KEY_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.READ_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.TIMEOUT_SETTING;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomRetryingPurpose;
import static org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase.randomBytes;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * This class tests how a {@link AzureBlobContainer} and its underlying SDK client are retrying requests when reading or writing blobs.
 */
@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerRetriesTests extends AbstractBlobContainerRetriesTestCase {

    private static final String ACCOUNT = "account";
    private static final String CONTAINER = "container";

    private AzureClientProvider clientProvider;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private HttpServer secondaryHttpServer;

    @Before
    public void setUp() throws Exception {
        threadPool = new TestThreadPool(
            getTestClass().getName(),
            AzureRepositoryPlugin.executorBuilder(Settings.EMPTY),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
        secondaryHttpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        secondaryHttpServer.start();
        clientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY);
        clientProvider.start();
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        clientProvider.close();
        super.tearDown();
        secondaryHttpServer.stop(0);
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    public void testReadNonexistentBlobThrowsNoSuchFileException() {
        final BlobContainer blobContainer = createBlobContainer(between(1, 5));
        final Exception exception = expectThrows(NoSuchFileException.class, () -> {
            if (randomBoolean()) {
                blobContainer.readBlob(randomPurpose(), "read_nonexistent_blob");
            } else {
                final long position = randomLongBetween(0, MAX_RANGE_VAL - 1L);
                final long length = randomLongBetween(1, MAX_RANGE_VAL - position);
                blobContainer.readBlob(randomPurpose(), "read_nonexistent_blob", position, length);
            }
        });
        assertThat(exception.toString(), exception.getMessage().toLowerCase(Locale.ROOT), containsString("not found"));
    }

    public void testReadBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(1, 5);
        final CountDown countDownHead = new CountDown(maxRetries);
        final CountDown countDownGet = new CountDown(maxRetries);
        final byte[] bytes = randomBlobContent();
        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_blob_max_retries"), exchange -> {
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

        try (InputStream inputStream = blobContainer.readBlob(randomRetryingPurpose(), "read_blob_max_retries")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));
            assertThat(countDownHead.isCountedDown(), is(true));
            assertThat(countDownGet.isCountedDown(), is(true));
        }
    }

    public void testReadBlobWithFailuresMidDownload() throws IOException {
        final int responsesToSend = randomIntBetween(3, 5);
        final AtomicInteger responseCounter = new AtomicInteger(responsesToSend);
        final byte[] blobContents = randomBlobContent();
        final String eTag = UUIDs.base64UUID();
        final BlobContainer blobContainer = createBlobContainer(responsesToSend * 2);
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_blob_fail_mid_stream"), exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(blobContents.length));
                    exchange.getResponseHeaders().add("Content-Length", String.valueOf(blobContents.length));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    if (responseCounter.decrementAndGet() > 0) {
                        switch (randomIntBetween(1, 3)) {
                            case 1 -> {
                                final Integer rCode = randomFrom(
                                    RestStatus.INTERNAL_SERVER_ERROR.getStatus(),
                                    RestStatus.SERVICE_UNAVAILABLE.getStatus(),
                                    RestStatus.TOO_MANY_REQUESTS.getStatus()
                                );
                                logger.info("---> sending error: {}", rCode);
                                exchange.sendResponseHeaders(rCode, -1);
                            }
                            case 2 -> logger.info("---> sending no response");
                            case 3 -> sendResponse(eTag, blobContents, exchange, true);
                        }
                    } else {
                        sendResponse(eTag, blobContents, exchange, false);
                    }
                }
            } finally {
                exchange.close();
            }
        });

        try (InputStream inputStream = blobContainer.readBlob(randomRetryingPurpose(), "read_blob_fail_mid_stream")) {
            assertArrayEquals(blobContents, BytesReference.toBytes(Streams.readFully(inputStream)));
        }
    }

    private void sendResponse(String eTag, byte[] blobContents, HttpExchange exchange, boolean partial) throws IOException {
        final var ranges = getRanges(exchange);
        final int start = Math.toIntExact(ranges.start());
        final int end = partial ? randomIntBetween(start, Math.toIntExact(ranges.end())) : Math.toIntExact(ranges.end());
        final var contents = Arrays.copyOfRange(blobContents, start, end + 1);

        logger.info("---> responding to: {} -> {} (sending chunk of size {})", ranges.start(), ranges.end(), contents.length);
        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
        exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(blobContents.length));
        exchange.getResponseHeaders().add("Content-Length", String.valueOf(blobContents.length));
        exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
        exchange.getResponseHeaders().add("ETag", eTag);
        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), blobContents.length - ranges.start());
        exchange.getResponseBody().write(contents, 0, contents.length);
    }

    public void testReadRangeBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(1, 5);
        final CountDown countDownGet = new CountDown(maxRetries);
        final byte[] bytes = randomBlobContent();
        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_range_blob_max_retries"), exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    throw new AssertionError("Should not HEAD blob for ranged reads");
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    if (countDownGet.countDown()) {
                        final int rangeStart = getRangeStart(exchange);
                        assertThat(rangeStart, lessThan(bytes.length));
                        final OptionalInt rangeEnd = getRangeEnd(exchange);
                        assertTrue(rangeEnd.isPresent());
                        assertThat(rangeEnd.getAsInt(), greaterThanOrEqualTo(rangeStart));
                        final int length = (rangeEnd.getAsInt() - rangeStart) + 1;
                        assertThat(length, lessThanOrEqualTo(bytes.length - rangeStart));
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.getResponseHeaders()
                            .add(
                                "Content-Range",
                                "bytes " + rangeStart + "-" + (rangeStart + rangeEnd.getAsInt() + 1) + "/" + bytes.length
                            );
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

        final int position = randomIntBetween(0, bytes.length - 1);
        final int length = randomIntBetween(1, bytes.length - position);
        try (InputStream inputStream = blobContainer.readBlob(randomPurpose(), "read_range_blob_max_retries", position, length)) {
            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(inputStream));
            assertArrayEquals(Arrays.copyOfRange(bytes, position, Math.min(bytes.length, position + length)), bytesRead);
            assertThat(countDownGet.isCountedDown(), is(true));
        }
    }

    public void testWriteBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(1, 5);
        final CountDown countDown = new CountDown(maxRetries);

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        final byte[] bytes = randomBlobContent();
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "write_blob_max_retries"), exchange -> {
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
                        org.elasticsearch.core.Streams.readFully(
                            exchange.getRequestBody(),
                            new byte[randomIntBetween(1, Math.max(1, bytes.length - 1))]
                        );
                    } else {
                        Streams.readFully(exchange.getRequestBody());
                        AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
                    }
                }
                exchange.close();
            }
        });

        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
            blobContainer.writeBlob(randomPurpose(), "write_blob_max_retries", stream, bytes.length, false);
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

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        final Map<String, BytesReference> blocks = new ConcurrentHashMap<>();
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "write_large_blob"), exchange -> {

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

        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", data), data.length)) {
            blobContainer.writeBlob(randomPurpose(), "write_large_blob", stream, data.length, false);
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
        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "write_large_blob_streaming"), exchange -> {

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

        blobContainer.writeMetadataBlob(randomPurpose(), "write_large_blob_streaming", false, randomBoolean(), out -> {
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
        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "write_blob_max_retries"), exchange -> {
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
                () -> blobContainer.writeBlob(randomPurpose(), "write_blob_max_retries", stream, randomIntBetween(1, 128), randomBoolean())
            );
            assertThat(ioe.getMessage(), is("Unable to write blob " + blobContainer.path().buildAsString() + "write_blob_max_retries"));
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
            // a path, that's the reason why we should provide just the host + port;
            secondaryHost = getEndpointForServer(secondaryHttpServer, "account");
        } else if (locationMode == LocationMode.SECONDARY_THEN_PRIMARY) {
            secondaryHttpServer.createContext(blobPath, failingHandler);
            httpServer.createContext(blobPath, workingHandler);
            secondaryHost = getEndpointForServer(httpServer, "account");
        }

        final BlobContainer blobContainer = createBlobContainer(maxRetries, secondaryHost, locationMode);
        try (InputStream inputStream = blobContainer.readBlob(randomRetryingPurpose(), "read_blob_from_secondary")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));

            // It does round robin, first tries on the primary, then on the secondary
            assertThat(failedHeadCalls.get(), equalTo(1));
            assertThat(failedGetCalls.get(), equalTo(1));
        }
    }

    public void testRetriesAreTerminatedWhenClientProviderIsClosed() {
        final BlobContainer blobContainer = createBlobContainer(randomIntBetween(1000, 2000));
        final byte[] blobContents = randomByteArrayOfLength(1024);
        final int incompleteLength = 10;
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_blob_while_store_closes"), exchange -> {
            boolean closeAfterHandling = false;
            try {
                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    handleHeadRequest(exchange, blobContents);
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    final int rangeStart = getRangeStart(exchange);
                    assertThat(rangeStart, lessThan(blobContents.length));
                    final OptionalInt rangeEnd = getRangeEnd(exchange);
                    assertTrue(rangeEnd.isPresent());
                    assertThat(rangeEnd.getAsInt(), greaterThanOrEqualTo(rangeStart));
                    final int requestedLength = (rangeEnd.getAsInt() - rangeStart) + 1;
                    assertThat(requestedLength, lessThanOrEqualTo(blobContents.length - rangeStart));
                    assertThat(requestedLength, greaterThan(incompleteLength));
                    addSuccessfulDownloadHeaders(exchange, blobContents, requestedLength);
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), requestedLength);
                    exchange.getResponseBody().write(blobContents, rangeStart, incompleteLength);
                    closeAfterHandling = true;
                } else {
                    ExceptionsHelper.maybeDieOnAnotherThread(
                        new AssertionError("Unexpected request method: " + exchange.getRequestMethod())
                    );
                }
            } finally {
                exchange.close();
                if (closeAfterHandling) {
                    // Close the client provider after we've sent the response
                    clientProvider.close();
                }
            }
        });

        assertThrows(
            AlreadyClosedException.class,
            () -> Streams.readFully(blobContainer.readBlob(randomRetryingPurpose(), "read_blob_while_store_closes"))
        );
    }

    private BlobContainer createBlobContainer(int maxRetries, String secondaryHost, LocationMode locationMode) {
        return createBlobContainer(maxRetries, null, null, null, null, null, BlobPath.EMPTY, secondaryHost, locationMode);
    }

    private BlobContainer createBlobContainer(int maxRetries) {
        return createBlobContainer(maxRetries, null, null, null, null, null, null);
    }

    @Override
    protected String downloadStorageEndpoint(BlobContainer container, String blob) {
        return "/account/container/" + container.path().buildAsString() + blob;
    }

    @Override
    protected String bytesContentType() {
        return "application/octet-stream";
    }

    @Override
    protected Class<? extends Exception> unresponsiveExceptionType() {
        // Actually a reactor.core.Exceptions.ReactiveException which is not visible, but extends RuntimeException
        return RuntimeException.class;
    }

    @Override
    protected BlobContainer createBlobContainer(
        @Nullable Integer maxRetries,
        @Nullable TimeValue readTimeout,
        @Nullable Boolean disableChunkedEncoding,
        @Nullable Integer maxConnections,
        @Nullable ByteSizeValue bufferSize,
        @Nullable Integer maxBulkDeletes,
        @Nullable BlobPath blobContainerPath
    ) {
        return createBlobContainer(
            maxRetries,
            readTimeout,
            disableChunkedEncoding,
            maxConnections,
            bufferSize,
            maxBulkDeletes,
            blobContainerPath,
            null,
            LocationMode.PRIMARY_ONLY
        );
    }

    private BlobContainer createBlobContainer(
        @Nullable Integer maxRetries,
        @Nullable TimeValue readTimeout,
        @Nullable Boolean disableChunkedEncoding,
        @Nullable Integer maxConnections,
        @Nullable ByteSizeValue bufferSize,
        @Nullable Integer maxBulkDeletes,
        @Nullable BlobPath blobContainerPath,
        @Nullable String secondaryHost,
        LocationMode locationMode
    ) {
        warnIfUnsupportedSettingSet("disableChunkedEncoding", disableChunkedEncoding);
        warnIfUnsupportedSettingSet("maxConnections", maxConnections);
        warnIfUnsupportedSettingSet("bufferSize", bufferSize);
        warnIfUnsupportedSettingSet("maxBulkDeletes", maxBulkDeletes);

        final Settings.Builder clientSettings = Settings.builder();
        final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + getEndpointForServer(httpServer, ACCOUNT);
        if (secondaryHost != null) {
            endpoint += ";BlobSecondaryEndpoint=" + getEndpointForServer(secondaryHttpServer, ACCOUNT);
        }
        clientSettings.put(ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint);
        if (maxRetries != null) {
            clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxRetries);
        }
        clientSettings.put(TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), TimeValue.timeValueSeconds(1));
        if (readTimeout != null) {
            clientSettings.put(READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), readTimeout);
        }

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), ACCOUNT);
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(14).getBytes(UTF_8));
        secureSettings.setString(KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), key);
        clientSettings.setSecureSettings(secureSettings);

        final AzureStorageService service = new AzureStorageService(
            clientSettings.build(),
            clientProvider,
            clusterService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        ) {
            @Override
            RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
                RequestRetryOptions retryOptions = super.getRetryOptions(locationMode, azureStorageSettings);
                return new RequestRetryOptions(
                    RetryPolicyType.EXPONENTIAL,
                    retryOptions.getMaxTries(),
                    retryOptions.getTryTimeoutDuration(),
                    Duration.ofMillis(50),
                    Duration.ofMillis(100),
                    // The SDK doesn't work well with ip endpoints. Secondary host endpoints that contain
                    // a path causes the sdk to rewrite the endpoint with an invalid path, that's the reason why we provide just the host +
                    // port.
                    secondaryHost != null ? secondaryHost.replaceFirst("/" + ACCOUNT, "") : null
                );
            }

            @Override
            long getUploadBlockSize() {
                return ByteSizeUnit.MB.toBytes(1);
            }
        };

        final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
            "repository",
            AzureRepository.TYPE,
            Settings.builder()
                .put(CONTAINER_SETTING.getKey(), CONTAINER)
                .put(ACCOUNT_SETTING.getKey(), clientName)
                .put(LOCATION_MODE_SETTING.getKey(), locationMode)
                .put(MAX_SINGLE_PART_UPLOAD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.MB))
                .build()
        );

        return new AzureBlobContainer(
            Objects.requireNonNullElse(blobContainerPath, randomBoolean() ? BlobPath.EMPTY : BlobPath.EMPTY.add(randomIdentifier())),
            new AzureBlobStore(ProjectId.DEFAULT, repositoryMetadata, service, BigArrays.NON_RECYCLING_INSTANCE, RepositoriesMetrics.NOOP)
        );
    }

    private void warnIfUnsupportedSettingSet(String settingName, Object value) {
        if (value != null) {
            logger.warn("Setting [{}] is not supported for Azure repository. Ignoring value [{}]", settingName, value);
        }
    }

    private String getEndpointForServer(HttpServer server, String accountName) {
        InetSocketAddress address = server.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort() + "/" + accountName;
    }

    @Override
    protected void addSuccessfulDownloadHeaders(HttpExchange exchange, byte[] blobContents, int contentLength) {
        exchange.getResponseHeaders().add("Content-Length", String.valueOf(contentLength));
        exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
        exchange.getResponseHeaders().add("ETag", eTagForContents(blobContents));
    }

    @Override
    protected HttpHeaderParser.Range getRange(HttpExchange exchange) {
        return getRanges(exchange);
    }

    @Override
    protected HttpHandler interceptGetBlobRequest(HttpHandler handler, byte[] blobContents) {
        return exchange -> {
            if (exchange.getRequestMethod().equals("HEAD")) {
                try (exchange) {
                    handleHeadRequest(exchange, blobContents);
                }
            } else {
                handler.handle(exchange);
            }
        };
    }

    protected void handleHeadRequest(HttpExchange exchange, byte[] blobContents) throws IOException {
        if (exchange.getRequestHeaders().containsKey("X-ms-range")) {
            ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError("Shouldn't send a HEAD request for a range"));
        }
        addSuccessfulDownloadHeaders(exchange, blobContents, blobContents.length);
        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
    }

    private static String eTagForContents(byte[] blobContents) {
        return Base64.getEncoder().encodeToString(MessageDigests.digest(new BytesArray(blobContents), MessageDigests.md5()));
    }
}
