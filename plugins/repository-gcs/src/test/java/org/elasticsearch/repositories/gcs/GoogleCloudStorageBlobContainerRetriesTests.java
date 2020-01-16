/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import fixture.gcs.FakeOAuth2HttpHandler;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.threeten.bp.Duration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase.randomBytes;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;
import static org.elasticsearch.repositories.gcs.TestUtils.createServiceAccount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@SuppressForbidden(reason = "use a http server")
public class GoogleCloudStorageBlobContainerRetriesTests extends ESTestCase {

    private HttpServer httpServer;

    private String httpServerUrl() {
        assertThat(httpServer, notNullValue());
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    @Before
    public void setUp() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        httpServer.stop(0);
        super.tearDown();
    }

    private BlobContainer createBlobContainer(final int maxRetries, final @Nullable TimeValue readTimeout) {
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
            StorageOptions createStorageOptions(final GoogleCloudStorageClientSettings clientSettings,
                                                final HttpTransportOptions httpTransportOptions) {
                StorageOptions options = super.createStorageOptions(clientSettings, httpTransportOptions);
                return options.toBuilder()
                    .setRetrySettings(RetrySettings.newBuilder()
                        .setTotalTimeout(options.getRetrySettings().getTotalTimeout())
                        .setInitialRetryDelay(Duration.ofMillis(10L))
                        .setRetryDelayMultiplier(options.getRetrySettings().getRetryDelayMultiplier())
                        .setMaxRetryDelay(Duration.ofSeconds(1L))
                        .setMaxAttempts(maxRetries)
                        .setJittered(false)
                        .setInitialRpcTimeout(options.getRetrySettings().getInitialRpcTimeout())
                        .setRpcTimeoutMultiplier(options.getRetrySettings().getRpcTimeoutMultiplier())
                        .setMaxRpcTimeout(options.getRetrySettings().getMaxRpcTimeout())
                        .build())
                    .build();
            }
        };
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(clientSettings.build()));

        final List<HttpContext> httpContexts = Arrays.asList(
            // Auth
            httpServer.createContext("/token", new FakeOAuth2HttpHandler()),
            // Does bucket exists?
            httpServer.createContext("/storage/v1/b/bucket", safeHandler(exchange -> {
                byte[] response = ("{\"kind\":\"storage#bucket\",\"name\":\"bucket\",\"id\":\"0\"}").getBytes(UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
                exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
                exchange.getResponseBody().write(response);
            }))
        );

        final GoogleCloudStorageBlobStore blobStore = new GoogleCloudStorageBlobStore("bucket", client, service);
        httpContexts.forEach(httpContext -> httpServer.removeContext(httpContext));

        return new GoogleCloudStorageBlobContainer(BlobPath.cleanPath(), blobStore);
    }

    public void testReadNonexistentBlobThrowsNoSuchFileException() {
        final BlobContainer blobContainer = createBlobContainer(between(1, 5), null);
        final Exception exception = expectThrows(NoSuchFileException.class,
            () -> Streams.readFully(blobContainer.readBlob("read_nonexistent_blob")));
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("blob [read_nonexistent_blob] does not exist"));
    }

    public void testReadBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(2, 10);
        final CountDown countDown = new CountDown(maxRetries);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/download/storage/v1/b/bucket/o/read_blob_max_retries", exchange -> {
            Streams.readFully(exchange.getRequestBody());
            if (countDown.countDown()) {
                exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
                return;
            }
            exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
            if (randomBoolean()) {
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries, TimeValue.timeValueMillis(between(100, 500)));
        try (InputStream inputStream = blobContainer.readBlob("read_blob_max_retries")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));
            assertThat(countDown.isCountedDown(), is(true));
        }
    }

    public void testReadBlobWithReadTimeouts() {
        final int maxRetries = randomIntBetween(1, 3);
        final BlobContainer blobContainer = createBlobContainer(maxRetries, TimeValue.timeValueMillis(between(100, 200)));

        // HTTP server does not send a response
        httpServer.createContext("/download/storage/v1/b/bucket/o/read_blob_unresponsive", exchange -> {});

        StorageException storageException = expectThrows(StorageException.class,
            () -> Streams.readFully(blobContainer.readBlob("read_blob_unresponsive")));
        assertThat(storageException.getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
        assertThat(storageException.getCause(), instanceOf(SocketTimeoutException.class));

        // HTTP server sends a partial response
        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/download/storage/v1/b/bucket/o/read_blob_incomplete", exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(HttpStatus.SC_OK, bytes.length);
            final int bytesToSend = randomIntBetween(0, bytes.length - 1);
            if (bytesToSend > 0) {
                exchange.getResponseBody().write(bytes, 0, bytesToSend);
            }
            if (randomBoolean()) {
                exchange.getResponseBody().flush();
            }
        });

        storageException = expectThrows(StorageException.class, () -> {
            try (InputStream stream = blobContainer.readBlob("read_blob_incomplete")) {
                Streams.readFully(stream);
            }
        });
        assertThat(storageException.getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
        assertThat(storageException.getCause(), instanceOf(SocketTimeoutException.class));
    }

    public void testWriteBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(2, 10);
        final CountDown countDown = new CountDown(maxRetries);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/upload/storage/v1/b/bucket/o", safeHandler(exchange -> {
            assertThat(exchange.getRequestURI().getQuery(), containsString("uploadType=multipart"));
            if (countDown.countDown()) {
                Optional<Tuple<String, BytesArray>> content = parseMultipartRequestBody(exchange.getRequestBody());
                assertThat(content.isPresent(), is(true));
                assertThat(content.get().v1(), equalTo("write_blob_max_retries"));
                if (Objects.deepEquals(bytes, content.get().v2().array())) {
                    byte[] response = ("{\"bucket\":\"bucket\",\"name\":\"" + content.get().v1() + "\"}").getBytes(UTF_8);
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
                    Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, Math.max(1, bytes.length - 1))]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                    exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
                }
            }
        }));

        final BlobContainer blobContainer = createBlobContainer(maxRetries, null);
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
            blobContainer.writeBlob("write_blob_max_retries", stream, bytes.length, false);
        }
        assertThat(countDown.isCountedDown(), is(true));
    }

    public void testWriteBlobWithReadTimeouts() {
        final byte[] bytes = randomByteArrayOfLength(randomIntBetween(10, 128));
        final TimeValue readTimeout = TimeValue.timeValueMillis(randomIntBetween(100, 500));
        final BlobContainer blobContainer = createBlobContainer(1, readTimeout);

        // HTTP server does not send a response
        httpServer.createContext("/upload/storage/v1/b/bucket/o", exchange -> {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, bytes.length - 1)]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                }
            }
        });

        Exception exception = expectThrows(StorageException.class, () -> {
            try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
                blobContainer.writeBlob("write_blob_timeout", stream, bytes.length, false);
            }
        });
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));

        assertThat(exception.getCause(), instanceOf(SocketTimeoutException.class));
        assertThat(exception.getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
    }

    public void testWriteLargeBlob() throws IOException {
        // See {@link BaseWriteChannel#DEFAULT_CHUNK_SIZE}
        final int defaultChunkSize = 8 * 256 * 1024;
        final int nbChunks = randomIntBetween(3, 5);
        final int lastChunkSize = randomIntBetween(1, defaultChunkSize - 1);
        final int totalChunks = nbChunks + 1;
        final byte[] data = randomBytes(defaultChunkSize * nbChunks + lastChunkSize);
        assertThat(data.length, greaterThan(GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE));

        logger.debug("resumable upload is composed of [{}] total chunks ([{}] chunks of length [{}] and last chunk of length [{}]",
            totalChunks, nbChunks, defaultChunkSize, lastChunkSize);

        final int nbErrors = 2; // we want all requests to fail at least once
        final AtomicInteger countInits = new AtomicInteger(nbErrors);
        final AtomicInteger countUploads = new AtomicInteger(nbErrors * totalChunks);
        final AtomicBoolean allow410Gone = new AtomicBoolean(randomBoolean());
        final AtomicBoolean allowReadTimeout = new AtomicBoolean(rarely());
        final int wrongChunk = randomIntBetween(1, totalChunks);

        final AtomicReference<String> sessionUploadId = new AtomicReference<>(UUIDs.randomBase64UUID());
        logger.debug("starting with resumable upload id [{}]", sessionUploadId.get());

        httpServer.createContext("/upload/storage/v1/b/bucket/o", safeHandler(exchange -> {
            final Map<String, String> params = new HashMap<>();
            RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
            assertThat(params.get("uploadType"), equalTo("resumable"));

            if ("POST".equals(exchange.getRequestMethod())) {
                assertThat(params.get("name"), equalTo("write_large_blob"));
                if (countInits.decrementAndGet() <= 0) {
                    byte[] response = Streams.readFully(exchange.getRequestBody()).utf8ToString().getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.getResponseHeaders().add("Location", httpServerUrl() +
                        "/upload/storage/v1/b/bucket/o?uploadType=resumable&upload_id=" + sessionUploadId.get());
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
                    Streams.readFully(exchange.getRequestBody());
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

                        Streams.readFully(exchange.getRequestBody());
                        exchange.sendResponseHeaders(HttpStatus.SC_GONE, -1);
                        return;
                    }
                }

                final String range = exchange.getRequestHeaders().getFirst("Content-Range");
                assertTrue(Strings.hasLength(range));

                if (countUploads.decrementAndGet() % 2 == 0) {
                    final ByteArrayOutputStream requestBody = new ByteArrayOutputStream();
                    final long bytesRead = Streams.copy(exchange.getRequestBody(), requestBody);
                    assertThat(Math.toIntExact(bytesRead), anyOf(equalTo(defaultChunkSize), equalTo(lastChunkSize)));

                    final int rangeStart = getContentRangeStart(range);
                    final int rangeEnd = getContentRangeEnd(range);
                    assertThat(rangeEnd + 1 - rangeStart, equalTo(Math.toIntExact(bytesRead)));
                    assertArrayEquals(Arrays.copyOfRange(data, rangeStart, rangeEnd + 1), requestBody.toByteArray());

                    final Integer limit = getContentRangeLimit(range);
                    if (limit != null) {
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                        return;
                    } else {
                        exchange.getResponseHeaders().add("Range", String.format(Locale.ROOT, "bytes=%d/%d", rangeStart, rangeEnd));
                        exchange.getResponseHeaders().add("Content-Length", "0");
                        exchange.sendResponseHeaders(308 /* Resume Incomplete */, -1);
                        return;
                    }
                }
            }

            // read all the request body, otherwise the SDK client throws a non-retryable StorageException
            Streams.readFully(exchange.getRequestBody());
            if (randomBoolean()) {
                exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
            }
        }));

        final TimeValue readTimeout = allowReadTimeout.get() ? TimeValue.timeValueSeconds(3) : null;

        final BlobContainer blobContainer = createBlobContainer(nbErrors + 1, readTimeout);
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", data), data.length)) {
            blobContainer.writeBlob("write_large_blob", stream, data.length, false);
        }

        assertThat(countInits.get(), equalTo(0));
        assertThat(countUploads.get(), equalTo(0));
        assertThat(allow410Gone.get(), is(false));
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

    private static byte[] randomBlobContent() {
        return randomByteArrayOfLength(randomIntBetween(1, frequently() ? 512 : 1 << 20)); // rarely up to 1mb
    }
}
