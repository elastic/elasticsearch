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
package org.elasticsearch.repositories.azure;

import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryPolicyFactory;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import fixture.azure.AzureHttpHandler;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.CONTAINER_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ENDPOINT_SUFFIX_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.KEY_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.TIMEOUT_SETTING;
import static org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase.randomBytes;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/**
 * This class tests how a {@link AzureBlobContainer} and its underlying SDK client are retrying requests when reading or writing blobs.
 */
@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerRetriesTests extends ESTestCase {

    private HttpServer httpServer;
    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        threadPool = new TestThreadPool(getTestClass().getName(), AzureRepositoryPlugin.executorBuilder());
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        httpServer.stop(0);
        super.tearDown();
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    private BlobContainer createBlobContainer(final int maxRetries) {
        final Settings.Builder clientSettings = Settings.builder();
        final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        final InetSocketAddress address = httpServer.getAddress();
        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=http://"
            + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
        clientSettings.put(ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint);
        clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxRetries);
        clientSettings.put(TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), TimeValue.timeValueMillis(500));

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "account");
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(10).getBytes(UTF_8));
        secureSettings.setString(KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), key);
        clientSettings.setSecureSettings(secureSettings);

        final AzureStorageService service = new AzureStorageService(clientSettings.build()) {
            @Override
            RetryPolicyFactory createRetryPolicy(final AzureStorageSettings azureStorageSettings) {
                return new RetryExponentialRetry(1, 100, 500, azureStorageSettings.getMaxRetries());
            }

            @Override
            BlobRequestOptions getBlobRequestOptionsForWriteBlob() {
                BlobRequestOptions options = new BlobRequestOptions();
                options.setSingleBlobPutThresholdInBytes(Math.toIntExact(ByteSizeUnit.MB.toBytes(1)));
                return options;
            }
        };

        final RepositoryMetaData repositoryMetaData = new RepositoryMetaData("repository", AzureRepository.TYPE,
            Settings.builder()
                .put(CONTAINER_SETTING.getKey(), "container")
                .put(ACCOUNT_SETTING.getKey(), clientName)
                .build());

        return new AzureBlobContainer(BlobPath.cleanPath(), new AzureBlobStore(repositoryMetaData, service, threadPool), threadPool);
    }

    public void testReadNonexistentBlobThrowsNoSuchFileException() {
        final BlobContainer blobContainer = createBlobContainer(between(1, 5));
        final Exception exception = expectThrows(NoSuchFileException.class, () -> blobContainer.readBlob("read_nonexistent_blob"));
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("not found"));
    }

    public void testReadBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(1, 5);
        final CountDown countDownHead = new CountDown(maxRetries);
        final CountDown countDownGet = new CountDown(maxRetries);
        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/container/read_blob_max_retries", exchange -> {
            Streams.readFully(exchange.getRequestBody());
            if ("HEAD".equals(exchange.getRequestMethod())) {
                if (countDownHead.countDown()) {
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(bytes.length));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                    exchange.close();
                    return;
                }
            } else if ("GET".equals(exchange.getRequestMethod())) {
                if (countDownGet.countDown()) {
                    final int rangeStart = getRangeStart(exchange);
                    assertThat(rangeStart, lessThan(bytes.length));
                    final int length = bytes.length - rangeStart;
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(length));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), length);
                    exchange.getResponseBody().write(bytes, rangeStart, length);
                    exchange.close();
                    return;
                }
            }
            if (randomBoolean()) {
                AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
            }
            exchange.close();
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        try (InputStream inputStream = blobContainer.readBlob("read_blob_max_retries")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));
            assertThat(countDownHead.isCountedDown(), is(true));
            assertThat(countDownGet.isCountedDown(), is(true));
        }
    }

    public void testWriteBlobWithRetries() throws Exception {
        final int maxRetries =  randomIntBetween(1, 5);
        final CountDown countDown = new CountDown(maxRetries);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/container/write_blob_max_retries", exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                if (countDown.countDown()) {
                    final BytesReference body = Streams.readFully(exchange.getRequestBody());
                    if (Objects.deepEquals(bytes, BytesReference.toBytes(body))) {
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
        final int maxRetries = randomIntBetween(1, 5);

        final int nbBlocks = randomIntBetween(1, 2);
        final byte[] data = randomBytes(Constants.DEFAULT_STREAM_WRITE_IN_BYTES * nbBlocks);

        final int nbErrors = 2; // we want all requests to fail at least once
        final AtomicInteger countDownUploads = new AtomicInteger(nbErrors * nbBlocks);
        final CountDown countDownComplete = new CountDown(nbErrors);

        final Map<String, BytesReference> blocks = new ConcurrentHashMap<>();
        httpServer.createContext("/container/write_large_blob", exchange -> {

            if ("PUT".equals(exchange.getRequestMethod())) {
                final Map<String, String> params = new HashMap<>();
                RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);

                final String blockId = params.get("blockid");
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
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", data), data.length)) {
            blobContainer.writeBlob("write_large_blob", stream, data.length * nbBlocks, false);
        }
        assertThat(countDownUploads.get(), equalTo(0));
        assertThat(countDownComplete.isCountedDown(), is(true));
        assertThat(blocks.isEmpty(), is(true));
    }

    public void testRetryUntilFail() throws IOException {
        final AtomicBoolean requestReceived = new AtomicBoolean(false);
        httpServer.createContext("/container/write_blob_max_retries", exchange -> {
            try {
                if (requestReceived.compareAndSet(false, true)) {
                    throw new AssertionError("Should not receive two requests");
                } else {
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
                }
            } finally {
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(randomIntBetween(2, 5));
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
            public void reset() {
                throw new AssertionError("should not be called");
            }
        }) {
            final IOException ioe = expectThrows(IOException.class, () ->
                blobContainer.writeBlob("write_blob_max_retries", stream, randomIntBetween(1, 128), randomBoolean()));
            assertThat(ioe.getMessage(), is("foo"));
        }
    }

    private static byte[] randomBlobContent() {
        return randomByteArrayOfLength(randomIntBetween(1, frequently() ? 512 : 1 << 20)); // rarely up to 1mb
    }

    private static int getRangeStart(final HttpExchange exchange) {
        final String rangeHeader = exchange.getRequestHeaders().getFirst("X-ms-range");
        if (rangeHeader == null) {
            return 0;
        }

        final Matcher matcher = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$").matcher(rangeHeader);
        assertTrue(rangeHeader + " matches expected pattern", matcher.matches());
        return Math.toIntExact(Long.parseLong(matcher.group(1)));
    }
}
