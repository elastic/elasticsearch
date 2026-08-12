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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MeteringCacheBlobReaderTests extends ESTestCase {

    public void testReadCompleteCallback() {
        final var capturedBytes = new ArrayList<Integer>();
        final var meteringCacheBlobReader = new MeteringCacheBlobReader(createFakeCacheBlobReader(), (bytesRead, timeToReadNanos) -> {
            capturedBytes.add(bytesRead);
            assertThat(timeToReadNanos, equalTo(0L));
        });

        final var consumer = meteringCacheBlobReader.newBytesCopiedConsumer();

        final var chunk1 = randomIntBetween(1, 1024);
        final var chunk2 = randomIntBetween(1, 1024);
        consumer.accept(chunk1);
        consumer.accept(chunk2);

        assertThat(capturedBytes, equalTo(List.of(chunk1, chunk2)));
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.cache.reader.MeteringCacheBlobReader:DEBUG", reason = "test debug log message")
    public void testExceptionIsLoggedAtDebugWhenCallbackThrows() {
        final var callbackException = new RuntimeException("Callback exception");
        final MeteringCacheBlobReader.ReadCompleteCallback throwingReadCompleteCallback = (bytesRead, timeToReadNanos) -> {
            throw callbackException;
        };
        final var meteringCacheBlobReader = new MeteringCacheBlobReader(createFakeCacheBlobReader(), throwingReadCompleteCallback);
        final var consumer = meteringCacheBlobReader.newBytesCopiedConsumer();
        try (MockLog mockLog = MockLog.capture(MeteringCacheBlobReader.class)) {
            mockLog.addExpectation(
                new MockLog.ExceptionSeenEventExpectation(
                    "callback threw message",
                    MeteringCacheBlobReader.class.getName(),
                    Level.DEBUG,
                    "Error calling readCompleteCallback",
                    callbackException.getClass(),
                    callbackException.getMessage()
                )
            );
            consumer.accept(randomIntBetween(1, 1024));
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
