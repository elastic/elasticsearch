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
package org.elasticsearch.repositories.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.internal.MD5DigestCalculatingInputStream;
import com.amazonaws.util.Base16;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Nullable;
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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.repositories.s3.S3ClientSettings.DISABLE_CHUNKED_ENCODING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.s3.S3ClientSettings.READ_TIMEOUT_SETTING;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/**
 * This class tests how a {@link S3BlobContainer} and its underlying AWS S3 client are retrying requests when reading or writing blobs.
 */
@SuppressForbidden(reason = "use a http server")
public class S3BlobContainerRetriesTests extends ESTestCase {

    private HttpServer httpServer;
    private S3Service service;

    @Before
    public void setUp() throws Exception {
        service = new S3Service();
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.close(service);
        httpServer.stop(0);
        super.tearDown();
    }

    private BlobContainer createBlobContainer(final @Nullable Integer maxRetries,
                                              final @Nullable TimeValue readTimeout,
                                              final @Nullable Boolean disableChunkedEncoding,
                                              final @Nullable ByteSizeValue bufferSize) {
        final Settings.Builder clientSettings = Settings.builder();
        final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        final InetSocketAddress address = httpServer.getAddress();
        final String endpoint = "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
        clientSettings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint);

        if (maxRetries != null) {
            clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxRetries);
        }
        if (readTimeout != null) {
            clientSettings.put(READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), readTimeout);
        }
        if (disableChunkedEncoding != null) {
            clientSettings.put(DISABLE_CHUNKED_ENCODING.getConcreteSettingForNamespace(clientName).getKey(), disableChunkedEncoding);
        }

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(S3ClientSettings.ACCESS_KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "access");
        secureSettings.setString(S3ClientSettings.SECRET_KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "secret");
        clientSettings.setSecureSettings(secureSettings);
        service.refreshAndClearCache(S3ClientSettings.load(clientSettings.build()));

        final RepositoryMetaData repositoryMetaData = new RepositoryMetaData("repository", S3Repository.TYPE,
            Settings.builder().put(S3Repository.CLIENT_NAME.getKey(), clientName).build());

        return new S3BlobContainer(BlobPath.cleanPath(), new S3BlobStore(service, "bucket",
            S3Repository.SERVER_SIDE_ENCRYPTION_SETTING.getDefault(Settings.EMPTY),
            bufferSize == null ? S3Repository.BUFFER_SIZE_SETTING.getDefault(Settings.EMPTY) : bufferSize,
            S3Repository.CANNED_ACL_SETTING.getDefault(Settings.EMPTY),
            S3Repository.STORAGE_CLASS_SETTING.getDefault(Settings.EMPTY),
            repositoryMetaData));
    }

    public void testReadNonexistentBlobThrowsNoSuchFileException() {
        final BlobContainer blobContainer = createBlobContainer(between(1, 5), null, null, null);
        final Exception exception = expectThrows(NoSuchFileException.class, () -> blobContainer.readBlob("read_nonexistent_blob"));
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("blob object [read_nonexistent_blob] not found"));
    }

    public void testReadBlobWithRetries() throws Exception {
        final int maxRetries = randomInt(5);
        final CountDown countDown = new CountDown(maxRetries + 1);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/bucket/read_blob_max_retries", exchange -> {
            Streams.readFully(exchange.getRequestBody());
            if (countDown.countDown()) {
                final int rangeStart = getRangeStart(exchange);
                assertThat(rangeStart, lessThan(bytes.length));
                exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
                exchange.sendResponseHeaders(HttpStatus.SC_OK, bytes.length - rangeStart);
                exchange.getResponseBody().write(bytes, rangeStart, bytes.length - rangeStart);
                exchange.close();
                return;
            }
            if (randomBoolean()) {
                exchange.sendResponseHeaders(randomFrom(HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_BAD_GATEWAY,
                                                        HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_GATEWAY_TIMEOUT), -1);
            } else if (randomBoolean()) {
                sendIncompleteContent(exchange, bytes);
            }
            if (randomBoolean()) {
                exchange.close();
            }
        });

        final TimeValue readTimeout = TimeValue.timeValueMillis(between(100, 500));
        final BlobContainer blobContainer = createBlobContainer(maxRetries, readTimeout, null, null);
        try (InputStream inputStream = blobContainer.readBlob("read_blob_max_retries")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));
            assertThat(countDown.isCountedDown(), is(true));
        }
    }

    public void testReadBlobWithReadTimeouts() {
        final int maxRetries = randomInt(5);
        final TimeValue readTimeout = TimeValue.timeValueMillis(between(100, 200));
        final BlobContainer blobContainer = createBlobContainer(maxRetries, readTimeout, null, null);

        // HTTP server does not send a response
        httpServer.createContext("/bucket/read_blob_unresponsive", exchange -> {});

        Exception exception = expectThrows(SdkClientException.class, () -> blobContainer.readBlob("read_blob_unresponsive"));
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
        assertThat(exception.getCause(), instanceOf(SocketTimeoutException.class));

        // HTTP server sends a partial response
        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/bucket/read_blob_incomplete", exchange -> sendIncompleteContent(exchange, bytes));

        exception = expectThrows(SocketTimeoutException.class, () -> {
            try (InputStream stream = blobContainer.readBlob("read_blob_incomplete")) {
                Streams.readFully(stream);
            }
        });
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
        assertThat(exception.getSuppressed().length, equalTo(maxRetries));
    }

    public void testReadBlobWithNoHttpResponse() {
        final BlobContainer blobContainer = createBlobContainer(randomInt(5), null, null, null);

        // HTTP server closes connection immediately
        httpServer.createContext("/bucket/read_blob_no_response", HttpExchange::close);

        Exception exception = expectThrows(SdkClientException.class, () -> blobContainer.readBlob("read_blob_no_response"));
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("the target server failed to respond"));
        assertThat(exception.getCause(), instanceOf(NoHttpResponseException.class));
        assertThat(exception.getSuppressed().length, equalTo(0));
    }

    public void testReadBlobWithPrematureConnectionClose() {
        final int maxRetries = randomInt(20);
        final BlobContainer blobContainer = createBlobContainer(maxRetries, null, null, null);

        // HTTP server sends a partial response
        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/bucket/read_blob_incomplete", exchange -> {
            sendIncompleteContent(exchange, bytes);
            exchange.close();
        });

        final Exception exception = expectThrows(ConnectionClosedException.class, () -> {
            try (InputStream stream = blobContainer.readBlob("read_blob_incomplete")) {
                Streams.readFully(stream);
            }
        });
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT),
            containsString("premature end of content-length delimited message body"));
        assertThat(exception.getSuppressed().length, equalTo(Math.min(S3RetryingInputStream.MAX_SUPPRESSED_EXCEPTIONS, maxRetries)));
    }

    public void testWriteBlobWithRetries() throws Exception {
        final int maxRetries = randomInt(5);
        final CountDown countDown = new CountDown(maxRetries + 1);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/bucket/write_blob_max_retries", exchange -> {
            if ("PUT".equals(exchange.getRequestMethod()) && exchange.getRequestURI().getQuery() == null) {
                if (countDown.countDown()) {
                    final BytesReference body = Streams.readFully(exchange.getRequestBody());
                    if (Objects.deepEquals(bytes, BytesReference.toBytes(body))) {
                        exchange.sendResponseHeaders(HttpStatus.SC_OK, -1);
                    } else {
                        exchange.sendResponseHeaders(HttpStatus.SC_BAD_REQUEST, -1);
                    }
                    exchange.close();
                    return;
                }

                if (randomBoolean()) {
                    if (randomBoolean()) {
                        Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, Math.max(1, bytes.length - 1))]);
                    } else {
                        Streams.readFully(exchange.getRequestBody());
                        exchange.sendResponseHeaders(randomFrom(HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_BAD_GATEWAY,
                                                                HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_GATEWAY_TIMEOUT), -1);
                    }
                }
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries, null, true, null);
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
            blobContainer.writeBlob("write_blob_max_retries", stream, bytes.length, false);
        }
        assertThat(countDown.isCountedDown(), is(true));
    }

    public void testWriteBlobWithReadTimeouts() {
        final byte[] bytes = randomByteArrayOfLength(randomIntBetween(10, 128));
        final TimeValue readTimeout = TimeValue.timeValueMillis(randomIntBetween(100, 500));
        final BlobContainer blobContainer = createBlobContainer(1, readTimeout, true, null);

        // HTTP server does not send a response
        httpServer.createContext("/bucket/write_blob_timeout", exchange -> {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, bytes.length - 1)]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                }
            }
        });

        Exception exception = expectThrows(IOException.class, () -> {
            try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
                blobContainer.writeBlob("write_blob_timeout", stream, bytes.length, false);
            }
        });
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT),
            containsString("unable to upload object [write_blob_timeout] using a single upload"));

        assertThat(exception.getCause(), instanceOf(SdkClientException.class));
        assertThat(exception.getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));

        assertThat(exception.getCause().getCause(), instanceOf(SocketTimeoutException.class));
        assertThat(exception.getCause().getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
    }

    public void testWriteLargeBlob() throws Exception {
        final boolean useTimeout = rarely();
        final TimeValue readTimeout = useTimeout ? TimeValue.timeValueMillis(randomIntBetween(100, 500)) : null;
        final ByteSizeValue bufferSize = new ByteSizeValue(5, ByteSizeUnit.MB);
        final BlobContainer blobContainer = createBlobContainer(null, readTimeout, true, bufferSize);

        final int parts = randomIntBetween(1, 5);
        final long lastPartSize = randomLongBetween(10, 512);
        final long blobSize = (parts * bufferSize.getBytes()) + lastPartSize;

        final int nbErrors = 2; // we want all requests to fail at least once
        final CountDown countDownInitiate = new CountDown(nbErrors);
        final AtomicInteger countDownUploads = new AtomicInteger(nbErrors * (parts + 1));
        final CountDown countDownComplete = new CountDown(nbErrors);

        httpServer.createContext("/bucket/write_large_blob", exchange -> {
            final long contentLength = Long.parseLong(exchange.getRequestHeaders().getFirst("Content-Length"));

            if ("POST".equals(exchange.getRequestMethod())
                && exchange.getRequestURI().getQuery().equals("uploads")) {
                // initiate multipart upload request
                if (countDownInitiate.countDown()) {
                    byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<InitiateMultipartUploadResult>\n" +
                        "  <Bucket>bucket</Bucket>\n" +
                        "  <Key>write_large_blob</Key>\n" +
                        "  <UploadId>TEST</UploadId>\n" +
                        "</InitiateMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
                    exchange.getResponseBody().write(response);
                    exchange.close();
                    return;
                }
            } else if ("PUT".equals(exchange.getRequestMethod())
                && exchange.getRequestURI().getQuery().contains("uploadId=TEST")
                && exchange.getRequestURI().getQuery().contains("partNumber=")) {
                // upload part request
                MD5DigestCalculatingInputStream md5 = new MD5DigestCalculatingInputStream(exchange.getRequestBody());
                BytesReference bytes = Streams.readFully(md5);
                assertThat((long) bytes.length(), anyOf(equalTo(lastPartSize), equalTo(bufferSize.getBytes())));
                assertThat(contentLength, anyOf(equalTo(lastPartSize), equalTo(bufferSize.getBytes())));

                if (countDownUploads.decrementAndGet() % 2 == 0) {
                    exchange.getResponseHeaders().add("ETag", Base16.encodeAsString(md5.getMd5Digest()));
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, -1);
                    exchange.close();
                    return;
                }

            } else if ("POST".equals(exchange.getRequestMethod())
                && exchange.getRequestURI().getQuery().equals("uploadId=TEST")) {
                // complete multipart upload request
                if (countDownComplete.countDown()) {
                    Streams.readFully(exchange.getRequestBody());
                    byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<CompleteMultipartUploadResult>\n" +
                        "  <Bucket>bucket</Bucket>\n" +
                        "  <Key>write_large_blob</Key>\n" +
                        "</CompleteMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
                    exchange.getResponseBody().write(response);
                    exchange.close();
                    return;
                }
            }

            // sends an error back or let the request time out
            if (useTimeout == false) {
                if (randomBoolean() && contentLength > 0) {
                    Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, Math.toIntExact(contentLength - 1))]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                    exchange.sendResponseHeaders(randomFrom(HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_BAD_GATEWAY,
                        HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_GATEWAY_TIMEOUT), -1);
                }
                exchange.close();
            }
        });

        blobContainer.writeBlob("write_large_blob", new ZeroInputStream(blobSize), blobSize, false);

        assertThat(countDownInitiate.isCountedDown(), is(true));
        assertThat(countDownUploads.get(), equalTo(0));
        assertThat(countDownComplete.isCountedDown(), is(true));
    }

    private static byte[] randomBlobContent() {
        return randomByteArrayOfLength(randomIntBetween(1, frequently() ? 512 : 1 << 20)); // rarely up to 1mb
    }

    private static int getRangeStart(HttpExchange exchange) {
        final String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
        if (rangeHeader == null) {
            return 0;
        }

        final Matcher matcher = Pattern.compile("^bytes=([0-9]+)-9223372036854775806$").matcher(rangeHeader);
        assertTrue(rangeHeader + " matches expected pattern", matcher.matches());
        return Math.toIntExact(Long.parseLong(matcher.group(1)));
    }

    private static void sendIncompleteContent(HttpExchange exchange, byte[] bytes) throws IOException {
        final int rangeStart = getRangeStart(exchange);
        assertThat(rangeStart, lessThan(bytes.length));
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(HttpStatus.SC_OK, bytes.length - rangeStart);
        final int bytesToSend = randomIntBetween(0, bytes.length - rangeStart - 1);
        if (bytesToSend > 0) {
            exchange.getResponseBody().write(bytes, rangeStart, bytesToSend);
        }
        if (randomBoolean()) {
            exchange.getResponseBody().flush();
        }
    }

    /**
     * A resettable InputStream that only serves zeros.
     **/
    private static class ZeroInputStream extends InputStream {

        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final long length;
        private final AtomicLong reads;
        private volatile long mark;

        private ZeroInputStream(final long length) {
            this.length = length;
            this.reads = new AtomicLong(0);
            this.mark = -1;
        }

        @Override
        public int read() throws IOException {
            ensureOpen();
            return (reads.incrementAndGet() <= length) ? 0 : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            ensureOpen();
            if (len == 0) {
                return 0;
            }

            final int available = available();
            if (available == 0) {
                return -1;
            }

            final int toCopy = Math.min(len, available);
            Arrays.fill(b, off, off + toCopy, (byte) 0);
            reads.addAndGet(toCopy);
            return toCopy;
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public synchronized void mark(int readlimit) {
            mark = reads.get();
        }

        @Override
        public synchronized void reset() throws IOException {
            ensureOpen();
            reads.set(mark);
        }

        @Override
        public int available() throws IOException {
            ensureOpen();
            if (reads.get() >= length) {
                return 0;
            }
            try {
                return Math.toIntExact(length - reads.get());
            } catch (ArithmeticException e) {
                return Integer.MAX_VALUE;
            }
        }

        @Override
        public void close() {
            closed.set(true);
        }

        private void ensureOpen() throws IOException {
            if (closed.get()) {
                throw new IOException("Stream closed");
            }
        }
    }
}
