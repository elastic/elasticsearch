/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.bytes.BytesReference;
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
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESTestCase;
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
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.CONTAINER_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.LOCATION_MODE_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.MAX_SINGLE_PART_UPLOAD_SIZE_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ENDPOINT_SUFFIX_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.KEY_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.TIMEOUT_SETTING;
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
public class AzureBlobContainerRetriesTests extends ESTestCase {

    private static final long MAX_RANGE_VAL = Long.MAX_VALUE - 1L;

    private HttpServer httpServer;
    private HttpServer secondaryHttpServer;
    private ThreadPool threadPool;
    private AzureClientProvider clientProvider;

    @Before
    public void setUp() throws Exception {
        threadPool = new TestThreadPool(
            getTestClass().getName(),
            AzureRepositoryPlugin.executorBuilder(),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        secondaryHttpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        secondaryHttpServer.start();
        clientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY);
        clientProvider.start();
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        clientProvider.close();
        httpServer.stop(0);
        secondaryHttpServer.stop(0);
        super.tearDown();
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    private BlobContainer createBlobContainer(final int maxRetries) {
        return createBlobContainer(maxRetries, null, LocationMode.PRIMARY_ONLY);
    }

    private BlobContainer createBlobContainer(final int maxRetries, String secondaryHost, final LocationMode locationMode) {
        final Settings.Builder clientSettings = Settings.builder();
        final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + getEndpointForServer(httpServer, "account");
        if (secondaryHost != null) {
            endpoint += ";BlobSecondaryEndpoint=" + getEndpointForServer(secondaryHttpServer, "account");
        }
        clientSettings.put(ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint);
        clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxRetries);
        clientSettings.put(TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), TimeValue.timeValueMillis(500));

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "account");
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(14).getBytes(UTF_8));
        secureSettings.setString(KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), key);
        clientSettings.setSecureSettings(secureSettings);

        final AzureStorageService service = new AzureStorageService(clientSettings.build(), clientProvider) {
            @Override
            RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
                return new RequestRetryOptions(
                    RetryPolicyType.EXPONENTIAL,
                    maxRetries + 1,
                    60,
                    50L,
                    100L,
                    // The SDK doesn't work well with ip endponts. Secondary host endpoints that contain
                    // a path causes the sdk to rewrite the endpoint with an invalid path, that's the reason why we provide just the host +
                    // port.
                    secondaryHost != null ? secondaryHost.replaceFirst("/account", "") : null
                );
            }

            @Override
            long getUploadBlockSize() {
                return ByteSizeUnit.MB.toBytes(1);
            }

            @Override
            int getMaxReadRetries(String clientName) {
                return maxRetries;
            }
        };

        final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
            "repository",
            AzureRepository.TYPE,
            Settings.builder()
                .put(CONTAINER_SETTING.getKey(), "container")
                .put(ACCOUNT_SETTING.getKey(), clientName)
                .put(LOCATION_MODE_SETTING.getKey(), locationMode)
                .put(MAX_SINGLE_PART_UPLOAD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.MB))
                .build()
        );

        return new AzureBlobContainer(BlobPath.EMPTY, new AzureBlobStore(repositoryMetadata, service, BigArrays.NON_RECYCLING_INSTANCE));
    }

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

    private static byte[] randomBlobContent() {
        return randomByteArrayOfLength(randomIntBetween(1, 1 << 20)); // rarely up to 1mb
    }

    private static final Pattern RANGE_PATTERN = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$");

    private static Tuple<Long, Long> getRanges(HttpExchange exchange) {
        final String rangeHeader = exchange.getRequestHeaders().getFirst("X-ms-range");
        if (rangeHeader == null) {
            return Tuple.tuple(0L, MAX_RANGE_VAL);
        }

        final Matcher matcher = RANGE_PATTERN.matcher(rangeHeader);
        assertTrue(rangeHeader + " matches expected pattern", matcher.matches());
        final long rangeStart = Long.parseLong(matcher.group(1));
        final long rangeEnd = Long.parseLong(matcher.group(2));
        assertThat(rangeStart, lessThanOrEqualTo(rangeEnd));
        return Tuple.tuple(rangeStart, rangeEnd);
    }

    private static int getRangeStart(HttpExchange exchange) {
        return Math.toIntExact(getRanges(exchange).v1());
    }

    private static Optional<Integer> getRangeEnd(HttpExchange exchange) {
        final long rangeEnd = getRanges(exchange).v2();
        if (rangeEnd == MAX_RANGE_VAL) {
            return Optional.empty();
        }
        return Optional.of(Math.toIntExact(rangeEnd));
    }

    private String getEndpointForServer(HttpServer server, String accountName) {
        InetSocketAddress address = server.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort() + "/" + accountName;
    }

    private void readFromInputStream(InputStream inputStream, long bytesToRead) {
        try {
            long totalBytesRead = 0;
            while (inputStream.read() != -1 && totalBytesRead < bytesToRead) {
                totalBytesRead += 1;
            }
            assertThat(totalBytesRead, equalTo(bytesToRead));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
