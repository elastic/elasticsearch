/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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
        final int[] read1 = { 0 };
        final int[] read2 = { 0 };
        meteringCacheBlobReader.getRangeInputStream(0, length, ActionListener.wrap(stream -> {
            final var buf = new byte[length];
            read1[0] = stream.read(buf, 0, chunk1);
            read2[0] = stream.read(buf, chunk1, chunk2);
        }, e -> fail("unexpected: " + e)));

        assertThat(read1[0], greaterThan(0));
        assertThat(read2[0], greaterThan(0));
        assertThat(capturedBytes, equalTo(List.of(read1[0], read2[0])));
    }

    public void testOnCopyCompletedCallback() {
        final var capturedTotal = new AtomicInteger();
        final var capturedTime = new AtomicLong(0);
        final var meteringCacheBlobReader = new MeteringCacheBlobReader(
            createFakeCacheBlobReader(),
            new MeteringCacheBlobReader.ReadCompleteCallback() {
                @Override
                public void onCopyCompleted(int totalBytesRead, long timeNanos) {
                    capturedTotal.set(totalBytesRead);
                    capturedTime.set(timeNanos);
                }
            }
        );

        final var total = randomIntBetween(1, 4096);
        final var time = randomLongBetween(1, 1_000_000);
        meteringCacheBlobReader.onCopyCompleted(total, time);

        assertThat(capturedTotal.get(), equalTo(total));
        assertThat(capturedTime.get(), equalTo(time));
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
    public void testExceptionIsLoggedAtDebugWhenTimingCallbackThrows() {
        final var callbackException = new RuntimeException("Timing callback exception");
        final var throwingCallback = new MeteringCacheBlobReader.ReadCompleteCallback() {
            @Override
            public void onCopyCompleted(int totalBytesRead, long timeNanos) {
                throw callbackException;
            }
        };
        final var meteringCacheBlobReader = new MeteringCacheBlobReader(createFakeCacheBlobReader(), throwingCallback);
        try (MockLog mockLog = MockLog.capture(MeteringCacheBlobReader.class)) {
            mockLog.addExpectation(
                new MockLog.ExceptionSeenEventExpectation(
                    "timing callback threw message",
                    MeteringCacheBlobReader.class.getName(),
                    Level.DEBUG,
                    "Error calling timing call-back",
                    callbackException.getClass(),
                    callbackException.getMessage()
                )
            );
            meteringCacheBlobReader.onCopyCompleted(randomIntBetween(1, 1024), randomLongBetween(1, 1_000_000));
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
}
