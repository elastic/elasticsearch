/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import fixture.gcs.FakeOAuth2HttpHandler;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageRetryStrategy;
import com.sun.net.httpserver.HttpHandler;

import org.apache.http.HttpStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.repositories.blobstore.AbstractBlobContainerRetriesTestCase;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.fixture.HttpHeaderParser;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static fixture.gcs.GoogleCloudStorageHttpHandler.getContentRangeEnd;
import static fixture.gcs.GoogleCloudStorageHttpHandler.getContentRangeLimit;
import static fixture.gcs.GoogleCloudStorageHttpHandler.getContentRangeStart;
import static fixture.gcs.GoogleCloudStorageHttpHandler.parseMultipartRequestBody;
import static fixture.gcs.TestUtils.createServiceAccount;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase.randomBytes;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageBlobStore.MAX_DELETES_PER_BATCH;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

@SuppressForbidden(reason = "use a http server")
public class GoogleCloudStorageBlobContainerRetriesTests extends AbstractBlobContainerRetriesTestCase {

    private String httpServerUrl() {
        assertThat(httpServer, notNullValue());
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    @Override
    protected String downloadStorageEndpoint(BlobContainer container, String blob) {
        return "/download/storage/v1/b/bucket/o/" + container.path().buildAsString() + blob;
    }

    @Override
    protected String bytesContentType() {
        return "application/octet-stream";
    }

    @Override
    protected Class<? extends Exception> unresponsiveExceptionType() {
        return StorageException.class;
    }

    @Override
    protected BlobContainer createBlobContainer(
        final @Nullable Integer maxRetries,
        final @Nullable TimeValue readTimeout,
        final @Nullable Boolean disableChunkedEncoding,
        final @Nullable ByteSizeValue bufferSize
    ) {
        final Settings.Builder clientSettings = Settings.builder();
        final String client = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        clientSettings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace(client).getKey(), httpServerUrl());
        clientSettings.put(TOKEN_URI_SETTING.getConcreteSettingForNamespace(client).getKey(), httpServerUrl() + "/token");
        if (readTimeout != null) {
            clientSettings.put(READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(client).getKey(), readTimeout);
        }

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile(CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(client).getKey(), createServiceAccount(random()));
        clientSettings.setSecureSettings(secureSettings);

        final GoogleCloudStorageService service = new GoogleCloudStorageService() {
            @Override
            StorageOptions createStorageOptions(
                final GoogleCloudStorageClientSettings gcsClientSettings,
                final HttpTransportOptions httpTransportOptions
            ) {
                StorageOptions options = super.createStorageOptions(gcsClientSettings, httpTransportOptions);
                RetrySettings.Builder retrySettingsBuilder = RetrySettings.newBuilder()
                    .setTotalTimeout(options.getRetrySettings().getTotalTimeout())
                    .setInitialRetryDelay(Duration.ofMillis(10L))
                    .setRetryDelayMultiplier(1.0d)
                    .setMaxRetryDelay(Duration.ofSeconds(1L))
                    .setJittered(false)
                    .setInitialRpcTimeout(Duration.ofSeconds(1))
                    .setRpcTimeoutMultiplier(options.getRetrySettings().getRpcTimeoutMultiplier())
                    .setMaxRpcTimeout(Duration.ofSeconds(1));
                if (maxRetries != null) {
                    retrySettingsBuilder.setMaxAttempts(maxRetries + 1);
                }
                return options.toBuilder()
                    .setStorageRetryStrategy(StorageRetryStrategy.getLegacyStorageRetryStrategy())
                    .setHost(options.getHost())
                    .setCredentials(options.getCredentials())
                    .setRetrySettings(retrySettingsBuilder.build())
                    .build();
            }
        };
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(clientSettings.build()));

        httpServer.createContext("/token", new FakeOAuth2HttpHandler());
        final GoogleCloudStorageBlobStore blobStore = new GoogleCloudStorageBlobStore(
            "bucket",
            client,
            "repo",
            service,
            BigArrays.NON_RECYCLING_INSTANCE,
            randomIntBetween(1, 8) * 1024
        );

        return new GoogleCloudStorageBlobContainer(randomBoolean() ? BlobPath.EMPTY : BlobPath.EMPTY.add("foo"), blobStore);
    }

    public void testReadLargeBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(2, 10);
        final AtomicInteger countDown = new AtomicInteger(maxRetries);

        final BlobContainer blobContainer = createBlobContainer(maxRetries, null, null, null);

        // SDK reads in 2 MB chunks so we use twice that to simulate 2 chunks
        final byte[] bytes = randomBytes(1 << 22);
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "large_blob_retries"), exchange -> {
            Streams.readFully(exchange.getRequestBody());
            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            final HttpHeaderParser.Range range = getRange(exchange);
            final int offset = Math.toIntExact(range.start());
            final byte[] chunk = Arrays.copyOfRange(bytes, offset, Math.toIntExact(Math.min(range.end() + 1, bytes.length)));
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), chunk.length);
            if (randomBoolean() && countDown.decrementAndGet() >= 0) {
                exchange.getResponseBody().write(chunk, 0, chunk.length - 1);
                exchange.close();
                return;
            }
            exchange.getResponseBody().write(chunk);
            exchange.close();
        });

        try (InputStream inputStream = blobContainer.readBlob(randomPurpose(), "large_blob_retries")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));
        }
    }

    public void testWriteBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(2, 10);
        final CountDown countDown = new CountDown(maxRetries);

        final BlobContainer blobContainer = createBlobContainer(maxRetries, null, null, null);
        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/upload/storage/v1/b/bucket/o", safeHandler(exchange -> {
            assertThat(exchange.getRequestURI().getQuery(), containsString("uploadType=multipart"));
            if (countDown.countDown()) {
                Optional<Tuple<String, BytesReference>> content = parseMultipartRequestBody(exchange.getRequestBody());
                assertThat(content, isPresent());
                assertThat(content.get().v1(), equalTo(blobContainer.path().buildAsString() + "write_blob_max_retries"));
                if (Objects.deepEquals(bytes, BytesReference.toBytes(content.get().v2()))) {
                    byte[] response = Strings.format("""
                        {"bucket":"bucket","name":"%s"}
                        """, content.get().v1()).getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                } else {
                    exchange.sendResponseHeaders(HttpStatus.SC_BAD_REQUEST, -1);
                }
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
                    exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
                }
            }
        }));

        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
            blobContainer.writeBlob(randomPurpose(), "write_blob_max_retries", stream, bytes.length, false);
        }
        assertThat(countDown.isCountedDown(), is(true));
    }

    public void testWriteBlobWithReadTimeouts() {
        final byte[] bytes = randomByteArrayOfLength(randomIntBetween(10, 128));
        final TimeValue readTimeout = TimeValue.timeValueMillis(randomIntBetween(100, 500));
        final BlobContainer blobContainer = createBlobContainer(1, readTimeout, null, null);

        // HTTP server does not send a response
        httpServer.createContext("/upload/storage/v1/b/bucket/o", exchange -> {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    org.elasticsearch.core.Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, bytes.length - 1)]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                }
            }
        });

        Exception exception = expectThrows(StorageException.class, () -> {
            try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
                blobContainer.writeBlob(randomPurpose(), "write_blob_timeout", stream, bytes.length, false);
            }
        });
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));

        assertThat(exception.getCause(), instanceOf(SocketTimeoutException.class));
        assertThat(exception.getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
    }

    public void testWriteLargeBlob() throws IOException {
        // See {@link BaseWriteChannel#DEFAULT_CHUNK_SIZE}
        final int defaultChunkSize = 60 * 256 * 1024;
        final int nbChunks = randomIntBetween(3, 5);
        final int lastChunkSize = randomIntBetween(1, defaultChunkSize - 1);
        final int totalChunks = nbChunks + 1;
        final byte[] data = randomBytes(defaultChunkSize * nbChunks + lastChunkSize);
        assertThat(data.length, greaterThan(GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE));

        logger.debug(
            "resumable upload is composed of [{}] total chunks ([{}] chunks of length [{}] and last chunk of length [{}]",
            totalChunks,
            nbChunks,
            defaultChunkSize,
            lastChunkSize
        );

        final int nbErrors = 2; // we want all requests to fail at least once
        final AtomicInteger countInits = new AtomicInteger(nbErrors);
        final AtomicInteger countUploads = new AtomicInteger(nbErrors * totalChunks);
        final AtomicBoolean allow410Gone = new AtomicBoolean(randomBoolean());
        final AtomicBoolean allowReadTimeout = new AtomicBoolean(rarely());
        final AtomicInteger bytesReceived = new AtomicInteger();
        final int wrongChunk = randomIntBetween(1, totalChunks);

        final AtomicReference<String> sessionUploadId = new AtomicReference<>(UUIDs.randomBase64UUID());
        logger.debug("starting with resumable upload id [{}]", sessionUploadId.get());

        final TimeValue readTimeout = allowReadTimeout.get() ? TimeValue.timeValueSeconds(3) : null;
        final BlobContainer blobContainer = createBlobContainer(nbErrors + 1, readTimeout, null, null);

        httpServer.createContext("/upload/storage/v1/b/bucket/o", safeHandler(exchange -> {
            final BytesReference requestBody = Streams.readFully(exchange.getRequestBody());

            final Map<String, String> params = new HashMap<>();
            RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
            assertThat(params.get("uploadType"), equalTo("resumable"));

            if ("POST".equals(exchange.getRequestMethod())) {
                assertThat(params.get("name"), equalTo(blobContainer.path().buildAsString() + "write_large_blob"));
                if (countInits.decrementAndGet() <= 0) {
                    byte[] response = requestBody.utf8ToString().getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.getResponseHeaders()
                        .add(
                            "Location",
                            httpServerUrl() + "/upload/storage/v1/b/bucket/o?uploadType=resumable&upload_id=" + sessionUploadId.get()
                        );
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                    return;
                }
                if (allowReadTimeout.get()) {
                    assertThat(wrongChunk, greaterThan(0));
                    return;
                }

            } else if ("PUT".equals(exchange.getRequestMethod())) {
                final String uploadId = params.get("upload_id");
                if (uploadId.equals(sessionUploadId.get()) == false) {
                    logger.debug("session id [{}] is gone", uploadId);
                    assertThat(wrongChunk, greaterThan(0));
                    exchange.sendResponseHeaders(HttpStatus.SC_GONE, -1);
                    return;
                }

                if (countUploads.get() == (wrongChunk * nbErrors)) {
                    if (allowReadTimeout.compareAndSet(true, false)) {
                        assertThat(wrongChunk, greaterThan(0));
                        return;
                    }
                    if (allow410Gone.compareAndSet(true, false)) {
                        final String newUploadId = UUIDs.randomBase64UUID(random());
                        logger.debug("chunk [{}] gone, updating session ids [{} -> {}]", wrongChunk, sessionUploadId.get(), newUploadId);
                        sessionUploadId.set(newUploadId);

                        // we must reset the counters because the whole object upload will be retried
                        countInits.set(nbErrors);
                        countUploads.set(nbErrors * totalChunks);
                        bytesReceived.set(0);

                        exchange.sendResponseHeaders(HttpStatus.SC_GONE, -1);
                        return;
                    }
                }

                final String range = exchange.getRequestHeaders().getFirst("Content-Range");
                assertTrue(Strings.hasLength(range));

                if (range.equals("bytes */*")) {
                    final int receivedSoFar = bytesReceived.get();
                    if (receivedSoFar > 0) {
                        exchange.getResponseHeaders().add("Range", Strings.format("bytes=0-%d", receivedSoFar));
                    }
                    exchange.getResponseHeaders().add("Content-Length", "0");
                    exchange.sendResponseHeaders(308 /* Resume Incomplete */, -1);
                    return;
                }

                if (countUploads.decrementAndGet() % 2 == 0) {

                    assertThat(Math.toIntExact(requestBody.length()), anyOf(equalTo(defaultChunkSize), equalTo(lastChunkSize)));

                    final int rangeStart = getContentRangeStart(range);
                    final int rangeEnd = getContentRangeEnd(range);
                    assertThat(rangeEnd + 1 - rangeStart, equalTo(Math.toIntExact(requestBody.length())));
                    assertThat(new BytesArray(data, rangeStart, rangeEnd - rangeStart + 1), is(requestBody));
                    bytesReceived.updateAndGet(existing -> Math.max(existing, rangeEnd));

                    final Integer limit = getContentRangeLimit(range);
                    if (limit != null) {
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                        return;
                    } else {
                        exchange.getResponseHeaders().add("Range", Strings.format("bytes=%d/%d", rangeStart, rangeEnd));
                        exchange.getResponseHeaders().add("Content-Length", "0");
                        exchange.sendResponseHeaders(308 /* Resume Incomplete */, -1);
                        return;
                    }
                }
            }

            if (randomBoolean()) {
                exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
            }
        }));

        if (randomBoolean()) {
            try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", data), data.length)) {
                blobContainer.writeBlob(randomPurpose(), "write_large_blob", stream, data.length, false);
            }
        } else {
            blobContainer.writeMetadataBlob(randomPurpose(), "write_large_blob", false, randomBoolean(), out -> out.write(data));
        }

        assertThat(countInits.get(), equalTo(0));
        assertThat(countUploads.get(), equalTo(0));
        assertThat(allow410Gone.get(), is(false));
    }

    public void testDeleteBatchesAreSentIncrementally() throws Exception {
        // See com.google.cloud.storage.spi.v1.HttpStorageRpc.DefaultRpcBatch.MAX_BATCH_SIZE
        final int sdkMaxBatchSize = 100;
        final AtomicInteger receivedBatchRequests = new AtomicInteger();

        final int totalDeletes = randomIntBetween(MAX_DELETES_PER_BATCH - 1, MAX_DELETES_PER_BATCH * 2);
        final AtomicInteger pendingDeletes = new AtomicInteger();
        final Iterator<String> blobNamesIterator = new Iterator<>() {
            int totalDeletesSent = 0;

            @Override
            public boolean hasNext() {
                return totalDeletesSent < totalDeletes;
            }

            @Override
            public String next() {
                if (pendingDeletes.get() == MAX_DELETES_PER_BATCH) {
                    // Check that once MAX_DELETES_PER_BATCH deletes are enqueued the pending batch requests are sent
                    assertThat(receivedBatchRequests.get(), is(greaterThan(0)));
                    assertThat(receivedBatchRequests.get(), is(lessThanOrEqualTo(MAX_DELETES_PER_BATCH / sdkMaxBatchSize)));
                    receivedBatchRequests.set(0);
                    pendingDeletes.set(0);
                }

                pendingDeletes.incrementAndGet();
                return Integer.toString(totalDeletesSent++);
            }
        };
        final BlobContainer blobContainer = createBlobContainer(1, null, null, null);
        httpServer.createContext("/batch/storage/v1", safeHandler(exchange -> {
            assert pendingDeletes.get() <= MAX_DELETES_PER_BATCH;

            receivedBatchRequests.incrementAndGet();
            final StringBuilder batch = new StringBuilder();
            for (String line : Streams.readAllLines(exchange.getRequestBody())) {
                if (line.length() == 0 || line.startsWith("--") || line.toLowerCase(Locale.ROOT).startsWith("content")) {
                    batch.append(line).append("\r\n");
                } else if (line.startsWith("DELETE")) {
                    batch.append("HTTP/1.1 204 NO_CONTENT").append("\r\n");
                    batch.append("\r\n");
                }
            }
            byte[] response = batch.toString().getBytes(UTF_8);
            exchange.getResponseHeaders().add("Content-Type", exchange.getRequestHeaders().getFirst("Content-Type"));
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
            exchange.getResponseBody().write(response);
        }));

        blobContainer.deleteBlobsIgnoringIfNotExists(randomPurpose(), blobNamesIterator);

        // Ensure that the remaining deletes are sent in the last batch
        if (pendingDeletes.get() > 0) {
            assertThat(receivedBatchRequests.get(), is(greaterThan(0)));
            assertThat(receivedBatchRequests.get(), is(lessThanOrEqualTo(MAX_DELETES_PER_BATCH / sdkMaxBatchSize)));

            assertThat(pendingDeletes.get(), is(lessThanOrEqualTo(MAX_DELETES_PER_BATCH)));
        }
    }

    private HttpHandler safeHandler(HttpHandler handler) {
        final HttpHandler loggingHandler = ESMockAPIBasedRepositoryIntegTestCase.wrap(handler, logger);
        return exchange -> {
            try {
                loggingHandler.handle(exchange);
            } finally {
                exchange.close();
            }
        };
    }
}
