/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.apache.logging.log4j.Level;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class MeteringCacheBlobReaderTests extends ESTestCase {

    public void testOnBytesReadCallbackViaStreamReads() throws IOException {
        final var capturedBytes = new ArrayList<Integer>();
        final var meteringCacheBlobReader = new MeteringCacheBlobReader(
            createFakeCacheBlobReader(),
            new MeteringCacheBlobReader.ReadCompleteCallback() {
                @Override
                public void onBytesRead(int bytesRead) {
                    capturedBytes.add(bytesRead);
                }
            }
        );

        final int chunk1 = randomIntBetween(1, 512);
        final int chunk2 = randomIntBetween(1, 512);
        final int length = chunk1 + chunk2;
        final List<Integer> readBytes = new ArrayList<>();
        meteringCacheBlobReader.getRangeInputStream(0, length, ActionListener.wrap(stream -> {
            final var buf = new byte[length];
            readBytes.add(stream.read(buf, 0, chunk1));
            readBytes.add(stream.read(buf, chunk1, chunk2));
        }, e -> fail("unexpected: " + e)));
        assertThat(capturedBytes, equalTo(readBytes));
    }

    public void testOnReadCompletedCallbackViaClose() throws IOException {
        var size = randomIntBetween(16, 1024);
        var cacheBlobReader = createFakeCacheBlobReader();

        var bytesReadHolder = new SetOnce<Integer>();
        var readTimeNanosHolder = new SetOnce<Long>();
        var meteringCacheBlobReader = new MeteringCacheBlobReader(cacheBlobReader, new MeteringCacheBlobReader.ReadCompleteCallback() {
            @Override
            public void onReadCompleted(int bytesRead, long timeToReadNanos) {
                bytesReadHolder.set(bytesRead);
                readTimeNanosHolder.set(timeToReadNanos);
            }
        });
        PlainActionFuture<InputStream> future = new PlainActionFuture<>();

        final long timeBeforeMethodCallNanos = System.nanoTime();

        meteringCacheBlobReader.getRangeInputStream(randomInt(), size, future);
        var meteredInputStream = safeGet(future);

        if (randomBoolean()) {
            Streams.consumeFully(meteredInputStream);
            assertEquals(size, bytesReadHolder.get().longValue());
            assertReadTimeIsReasonable(timeBeforeMethodCallNanos, readTimeNanosHolder.get());
        } else {
            int limit = randomIntBetween(1, size);
            try (var is = Streams.limitStream(meteredInputStream, limit)) {
                Streams.consumeFully(is);
            }
            assertEquals(limit, bytesReadHolder.get().longValue());
            assertReadTimeIsReasonable(timeBeforeMethodCallNanos, readTimeNanosHolder.get());
        }
    }

    public void testOnReadCompletedCallbackNotCalledWhenNoBytesRead() throws IOException {
        MeteringCacheBlobReader.ReadCompleteCallback readCompleteCallback = mock(MeteringCacheBlobReader.ReadCompleteCallback.class);
        var meteringCacheBlobReader = new MeteringCacheBlobReader(createFakeCacheBlobReader(), readCompleteCallback);
        PlainActionFuture<InputStream> future = new PlainActionFuture<>();
        meteringCacheBlobReader.getRangeInputStream(randomInt(), randomIntBetween(16, 1024), future);
        InputStream meteredInputStream = safeGet(future);
        meteredInputStream.close();
        verifyNoInteractions(readCompleteCallback);
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.cache.reader.MeteringCacheBlobReader:DEBUG", reason = "test debug log message")
    public void testExceptionIsLoggedAtDebugWhenBytesCallbackThrows() throws IOException {
        final var callbackException = new RuntimeException("Callback exception");
        final var throwingCallback = new MeteringCacheBlobReader.ReadCompleteCallback() {
            @Override
            public void onBytesRead(int bytesRead) {
                throw callbackException;
            }
        };
        final var meteringCacheBlobReader = new MeteringCacheBlobReader(createFakeCacheBlobReader(), throwingCallback);
        try (MockLog mockLog = MockLog.capture(MeteringCacheBlobReader.class)) {
            mockLog.addExpectation(
                new MockLog.ExceptionSeenEventExpectation(
                    "callback threw message",
                    MeteringCacheBlobReader.class.getName(),
                    Level.DEBUG,
                    "Error calling call-back",
                    callbackException.getClass(),
                    callbackException.getMessage()
                )
            );
            final int length = randomIntBetween(1, 1024);
            meteringCacheBlobReader.getRangeInputStream(0, length, ActionListener.wrap(stream -> {
                final byte[] buf = new byte[length];
                stream.read(buf, 0, length);
            }, e -> fail("unexpected: " + e)));
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.cache.reader.MeteringCacheBlobReader:DEBUG", reason = "test debug log message")
    public void testExceptionIsLoggedAtDebugWhenReadCompletedCallbackThrows() throws IOException {
        RuntimeException callbackException = new RuntimeException("Callback exception");
        MeteringCacheBlobReader.ReadCompleteCallback throwingReadCompleteCallback = new MeteringCacheBlobReader.ReadCompleteCallback() {
            @Override
            public void onReadCompleted(int bytesRead, long timeToReadNanos) {
                throw callbackException;
            }
        };
        var meteringCacheBlobReader = new MeteringCacheBlobReader(createFakeCacheBlobReader(), throwingReadCompleteCallback);
        PlainActionFuture<InputStream> future = new PlainActionFuture<>();
        meteringCacheBlobReader.getRangeInputStream(randomInt(), randomIntBetween(16, 1024), future);
        InputStream meteredInputStream = safeGet(future);
        meteredInputStream.read();
        try (MockLog mockLog = MockLog.capture(MeteringCacheBlobReader.class)) {
            mockLog.addExpectation(
                new MockLog.ExceptionSeenEventExpectation(
                    "callback threw message",
                    MeteringCacheBlobReader.class.getName(),
                    Level.DEBUG,
                    "Error calling call-back",
                    callbackException.getClass(),
                    callbackException.getMessage()
                )
            );
            meteredInputStream.close();
            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * Create a fake CacheBlobReader that returns random {@link InputStream}s of
     * the requested length
     */
    private static CacheBlobReader createFakeCacheBlobReader() {
        return new CacheBlobReader() {
            @Override
            public ByteRange getRange(long position, int length, long remainingFileLength) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                assert length < ByteSizeValue.ofMb(1).getBytes();
                listener.onResponse(new ByteArrayInputStream(randomByteArrayOfLength(length)));
            }
        };
    }

    private static void assertReadTimeIsReasonable(long timeBeforeMethodCallNanos, long reportedReadTimeNanos) {
        assertThat(reportedReadTimeNanos, greaterThan(0L));
        assertThat(reportedReadTimeNanos, lessThanOrEqualTo(System.nanoTime() - timeBeforeMethodCallNanos));
    }
}
