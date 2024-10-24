/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpStatus;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Locale;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.test.NeverMatcher.never;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@SuppressForbidden(reason = "use a http server")
public abstract class AbstractBlobContainerRetriesTestCase extends ESTestCase {

    private static final long MAX_RANGE_VAL = Long.MAX_VALUE - 1;

    protected HttpServer httpServer;

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

    protected abstract String downloadStorageEndpoint(BlobContainer container, String blob);

    protected abstract String bytesContentType();

    protected abstract Class<? extends Exception> unresponsiveExceptionType();

    protected abstract BlobContainer createBlobContainer(
        @Nullable Integer maxRetries,
        @Nullable TimeValue readTimeout,
        @Nullable Boolean disableChunkedEncoding,
        @Nullable ByteSizeValue bufferSize
    );

    protected org.hamcrest.Matcher<Object> readTimeoutExceptionMatcher() {
        return either(instanceOf(SocketTimeoutException.class)).or(instanceOf(ConnectionClosedException.class))
            .or(instanceOf(RuntimeException.class));
    }

    public void testReadNonexistentBlobThrowsNoSuchFileException() {
        final BlobContainer blobContainer = createBlobContainer(between(1, 5), null, null, null);
        final long position = randomLongBetween(0, MAX_RANGE_VAL);
        final int length = randomIntBetween(1, Math.toIntExact(Math.min(Integer.MAX_VALUE, MAX_RANGE_VAL - position)));
        final Exception exception = expectThrows(NoSuchFileException.class, () -> {
            if (randomBoolean()) {
                Streams.readFully(blobContainer.readBlob(randomPurpose(), "read_nonexistent_blob"));
            } else {
                Streams.readFully(blobContainer.readBlob(randomPurpose(), "read_nonexistent_blob", 0, 1));
            }
        });
        final String fullBlobPath = blobContainer.path().buildAsString() + "read_nonexistent_blob";
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("blob object [" + fullBlobPath + "] not found"));
        assertThat(
            expectThrows(
                NoSuchFileException.class,
                () -> Streams.readFully(blobContainer.readBlob(randomPurpose(), "read_nonexistent_blob", position, length))
            ).getMessage().toLowerCase(Locale.ROOT),
            containsString("blob object [" + fullBlobPath + "] not found")
        );
    }

    public void testReadBlobWithRetries() throws Exception {
        final int maxRetries = randomInt(5);
        final CountDown countDown = new CountDown(maxRetries + 1);

        final byte[] bytes = randomBlobContent();
        final TimeValue readTimeout = TimeValue.timeValueSeconds(between(1, 3));
        final BlobContainer blobContainer = createBlobContainer(maxRetries, readTimeout, null, null);
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_blob_max_retries"), exchange -> {
            Streams.readFully(exchange.getRequestBody());
            if (countDown.countDown()) {
                final int rangeStart = getRangeStart(exchange);
                assertThat(rangeStart, lessThan(bytes.length));
                exchange.getResponseHeaders().add("Content-Type", bytesContentType());
                exchange.sendResponseHeaders(HttpStatus.SC_OK, bytes.length - rangeStart);
                exchange.getResponseBody().write(bytes, rangeStart, bytes.length - rangeStart);
                exchange.close();
                return;
            }
            if (randomBoolean()) {
                exchange.sendResponseHeaders(
                    randomFrom(
                        HttpStatus.SC_INTERNAL_SERVER_ERROR,
                        HttpStatus.SC_BAD_GATEWAY,
                        HttpStatus.SC_SERVICE_UNAVAILABLE,
                        HttpStatus.SC_GATEWAY_TIMEOUT
                    ),
                    -1
                );
            } else if (randomBoolean()) {
                sendIncompleteContent(exchange, bytes);
            }
            if (randomBoolean()) {
                exchange.close();
            }
        });

        try (InputStream inputStream = blobContainer.readBlob(randomRetryingPurpose(), "read_blob_max_retries")) {
            final int readLimit;
            final InputStream wrappedStream;
            if (randomBoolean()) {
                // read stream only partly
                readLimit = randomIntBetween(0, bytes.length);
                wrappedStream = Streams.limitStream(inputStream, readLimit);
            } else {
                readLimit = bytes.length;
                wrappedStream = inputStream;
            }
            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(wrappedStream));
            logger.info("maxRetries={}, readLimit={}, byteSize={}, bytesRead={}", maxRetries, readLimit, bytes.length, bytesRead.length);
            assertArrayEquals(Arrays.copyOfRange(bytes, 0, readLimit), bytesRead);
            if (readLimit < bytes.length) {
                // we might have completed things based on an incomplete response, and we're happy with that
            } else {
                assertTrue(countDown.isCountedDown());
            }
        }
    }

    public void testReadRangeBlobWithRetries() throws Exception {
        final int maxRetries = rarely() ? randomInt(5) : 1;
        final CountDown countDown = new CountDown(maxRetries + 1);

        final TimeValue readTimeout = TimeValue.timeValueSeconds(between(5, 10));
        final BlobContainer blobContainer = createBlobContainer(maxRetries, readTimeout, null, null);
        final byte[] bytes = randomBlobContent();
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_range_blob_max_retries"), exchange -> {
            Streams.readFully(exchange.getRequestBody());
            if (countDown.countDown()) {
                final int rangeStart = getRangeStart(exchange);
                assertThat(rangeStart, lessThan(bytes.length));
                assertTrue(getRangeEnd(exchange).isPresent());
                final int rangeEnd = getRangeEnd(exchange).getAsInt();
                assertThat(rangeEnd, greaterThanOrEqualTo(rangeStart));
                // adapt range end to be compliant to https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
                final int effectiveRangeEnd = Math.min(bytes.length - 1, rangeEnd);
                final int length = (effectiveRangeEnd - rangeStart) + 1;
                exchange.getResponseHeaders().add("Content-Type", bytesContentType());
                exchange.sendResponseHeaders(HttpStatus.SC_OK, length);
                exchange.getResponseBody().write(bytes, rangeStart, length);
                exchange.close();
                return;
            }
            if (randomBoolean()) {
                exchange.sendResponseHeaders(
                    randomFrom(
                        HttpStatus.SC_INTERNAL_SERVER_ERROR,
                        HttpStatus.SC_BAD_GATEWAY,
                        HttpStatus.SC_SERVICE_UNAVAILABLE,
                        HttpStatus.SC_GATEWAY_TIMEOUT
                    ),
                    -1
                );
            } else if (randomBoolean()) {
                sendIncompleteContent(exchange, bytes);
            }
            if (randomBoolean()) {
                exchange.close();
            }
        });

        final int position = randomIntBetween(0, bytes.length - 1);
        final int length = randomIntBetween(0, randomBoolean() ? bytes.length : Integer.MAX_VALUE);
        try (InputStream inputStream = blobContainer.readBlob(randomRetryingPurpose(), "read_range_blob_max_retries", position, length)) {
            final int readLimit;
            final InputStream wrappedStream;
            if (randomBoolean()) {
                // read stream only partly
                readLimit = randomIntBetween(0, length);
                wrappedStream = Streams.limitStream(inputStream, readLimit);
            } else {
                readLimit = length;
                wrappedStream = inputStream;
            }
            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(wrappedStream));
            logger.info(
                "maxRetries={}, position={}, length={}, readLimit={}, byteSize={}, bytesRead={}",
                maxRetries,
                position,
                length,
                readLimit,
                bytes.length,
                bytesRead.length
            );
            assertArrayEquals(Arrays.copyOfRange(bytes, position, Math.min(bytes.length, position + readLimit)), bytesRead);
            if (readLimit == 0 || (readLimit < length && readLimit == bytesRead.length)) {
                // we might have completed things based on an incomplete response, and we're happy with that
            } else {
                assertTrue(countDown.isCountedDown());
            }
        }
    }

    public void testReadBlobWithReadTimeouts() {
        final int maxRetries = randomInt(5);
        final TimeValue readTimeout = TimeValue.timeValueMillis(between(100, 200));
        final BlobContainer blobContainer = createBlobContainer(maxRetries, readTimeout, null, null);

        // HTTP server does not send a response
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_blob_unresponsive"), exchange -> {});

        Exception exception = expectThrows(
            unresponsiveExceptionType(),
            () -> Streams.readFully(blobContainer.readBlob(randomFiniteRetryingPurpose(), "read_blob_unresponsive"))
        );
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
        assertThat(exception.getCause(), instanceOf(SocketTimeoutException.class));

        // HTTP server sends a partial response
        final byte[] bytes = randomBlobContent();
        httpServer.createContext(
            downloadStorageEndpoint(blobContainer, "read_blob_incomplete"),
            exchange -> sendIncompleteContent(exchange, bytes)
        );

        final int position = randomIntBetween(0, bytes.length - 1);
        final int length = randomIntBetween(1, randomBoolean() ? bytes.length : Integer.MAX_VALUE);
        exception = expectThrows(Exception.class, () -> {
            try (
                InputStream stream = randomBoolean()
                    ? blobContainer.readBlob(randomFiniteRetryingPurpose(), "read_blob_incomplete")
                    : blobContainer.readBlob(randomFiniteRetryingPurpose(), "read_blob_incomplete", position, length)
            ) {
                Streams.readFully(stream);
            }
        });
        assertThat(exception, readTimeoutExceptionMatcher());
        assertThat(
            exception.getMessage().toLowerCase(Locale.ROOT),
            anyOf(
                containsString("read timed out"),
                containsString("premature end of chunk coded message body: closing chunk expected"),
                containsString("Read timed out"),
                containsString("unexpected end of file from server")
            )
        );
        assertThat(exception.getSuppressed().length, getMaxRetriesMatcher(maxRetries));
    }

    protected org.hamcrest.Matcher<Integer> getMaxRetriesMatcher(int maxRetries) {
        return equalTo(maxRetries);
    }

    protected OperationPurpose randomRetryingPurpose() {
        return randomPurpose();
    }

    protected OperationPurpose randomFiniteRetryingPurpose() {
        return randomPurpose();
    }

    public void testReadBlobWithNoHttpResponse() {
        final TimeValue readTimeout = TimeValue.timeValueMillis(between(100, 200));
        final BlobContainer blobContainer = createBlobContainer(randomInt(5), readTimeout, null, null);

        // HTTP server closes connection immediately
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_blob_no_response"), HttpExchange::close);

        Exception exception = expectThrows(unresponsiveExceptionType(), () -> {
            if (randomBoolean()) {
                Streams.readFully(blobContainer.readBlob(randomFiniteRetryingPurpose(), "read_blob_no_response"));
            } else {
                Streams.readFully(blobContainer.readBlob(randomFiniteRetryingPurpose(), "read_blob_no_response", 0, 1));
            }
        });
        assertThat(
            exception.getMessage().toLowerCase(Locale.ROOT),
            either(containsString("the target server failed to respond")).or(containsString("unexpected end of file from server"))
        );
    }

    public void testReadBlobWithPrematureConnectionClose() {
        final int maxRetries = randomInt(20);
        final BlobContainer blobContainer = createBlobContainer(maxRetries, null, null, null);

        final boolean alwaysFlushBody = randomBoolean();

        // HTTP server sends a partial response
        final byte[] bytes = randomBlobContent(1);
        httpServer.createContext(downloadStorageEndpoint(blobContainer, "read_blob_incomplete"), exchange -> {
            sendIncompleteContent(exchange, bytes);
            if (alwaysFlushBody) {
                exchange.getResponseBody().flush();
            }
            exchange.close();
        });

        final Exception exception = expectThrows(Exception.class, () -> {
            try (
                InputStream stream = randomBoolean()
                    ? blobContainer.readBlob(randomFiniteRetryingPurpose(), "read_blob_incomplete", 0, 1)
                    : blobContainer.readBlob(randomFiniteRetryingPurpose(), "read_blob_incomplete")
            ) {
                Streams.readFully(stream);
            }
        });
        assertThat(
            exception.getMessage().toLowerCase(Locale.ROOT),
            anyOf(
                // closing the connection after sending the headers and some incomplete body might yield one of these:
                containsString("premature end of chunk coded message body: closing chunk expected"),
                containsString("premature end of content-length delimited message body"),
                containsString("connection closed prematurely"),
                // if we didn't call exchange.getResponseBody().flush() then we might not even have sent the response headers:
                alwaysFlushBody ? never() : containsString("the target server failed to respond")
            )
        );
        assertThat(exception.getSuppressed().length, getMaxRetriesMatcher(Math.min(10, maxRetries)));
    }

    protected static byte[] randomBlobContent() {
        return randomBlobContent(1);
    }

    protected static byte[] randomBlobContent(int minSize) {
        return randomByteArrayOfLength(randomIntBetween(minSize, frequently() ? 512 : 1 << 20)); // rarely up to 1mb
    }

    private static final Pattern RANGE_PATTERN = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$");

    protected static Tuple<Long, Long> getRange(HttpExchange exchange) {
        final String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
        if (rangeHeader == null) {
            return Tuple.tuple(0L, MAX_RANGE_VAL);
        }

        final Matcher matcher = RANGE_PATTERN.matcher(rangeHeader);
        assertTrue(rangeHeader + " matches expected pattern", matcher.matches());
        long rangeStart = Long.parseLong(matcher.group(1));
        long rangeEnd = Long.parseLong(matcher.group(2));
        assertThat(rangeStart, lessThanOrEqualTo(rangeEnd));
        return Tuple.tuple(rangeStart, rangeEnd);
    }

    protected static int getRangeStart(HttpExchange exchange) {
        return Math.toIntExact(getRange(exchange).v1());
    }

    protected static OptionalInt getRangeEnd(HttpExchange exchange) {
        final long rangeEnd = getRange(exchange).v2();
        if (rangeEnd == MAX_RANGE_VAL) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(Math.toIntExact(rangeEnd));
    }

    protected int sendIncompleteContent(HttpExchange exchange, byte[] bytes) throws IOException {
        final int rangeStart = getRangeStart(exchange);
        assertThat(rangeStart, lessThan(bytes.length));
        final OptionalInt rangeEnd = getRangeEnd(exchange);
        final int length;
        if (rangeEnd.isPresent()) {
            // adapt range end to be compliant to https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
            final int effectiveRangeEnd = Math.min(rangeEnd.getAsInt(), bytes.length - 1);
            length = effectiveRangeEnd - rangeStart + 1;
        } else {
            length = bytes.length - rangeStart;
        }
        exchange.getResponseHeaders().add("Content-Type", bytesContentType());
        exchange.sendResponseHeaders(HttpStatus.SC_OK, length);
        int minSend = Math.min(0, length - 1);
        final int bytesToSend = randomIntBetween(minSend, length - 1);
        if (bytesToSend > 0) {
            exchange.getResponseBody().write(bytes, rangeStart, bytesToSend);
        }
        if (randomBoolean() || Runtime.version().feature() >= 23) {
            // For now in JDK23 we need to always flush. See https://bugs.openjdk.org/browse/JDK-8331847.
            // TODO: remove the JDK version check once that issue is fixed
            exchange.getResponseBody().flush();
        }
        return bytesToSend;
    }

    /**
     * A resettable InputStream that only serves zeros.
     **/
    public static class ZeroInputStream extends InputStream {

        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final long length;
        private final AtomicLong reads;
        private volatile long mark;

        public ZeroInputStream(final long length) {
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
