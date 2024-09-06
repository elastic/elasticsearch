/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache.reader;

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

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class MeteringCacheBlobReaderTests extends ESTestCase {

    public void testReadCompleteCallback() throws IOException {

        var size = randomIntBetween(16, 1024);
        var cacheBlobReader = createFakeCacheBlobReader();

        var bytesReadHolder = new SetOnce<Integer>();
        var readTimeNanosHolder = new SetOnce<Long>();
        var meteringCacheBlobReader = new MeteringCacheBlobReader(cacheBlobReader, (bytesRead, timeToReadNanos) -> {
            bytesReadHolder.set(bytesRead);
            readTimeNanosHolder.set(timeToReadNanos);
        });
        PlainActionFuture<InputStream> future = new PlainActionFuture<>();

        // Measure time around the method call to confirm the reported read time is reasonable
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
                // consume up to a limit (partially) and close the underlying input stream
                Streams.consumeFully(is);
            }
            assertEquals(limit, bytesReadHolder.get().longValue());
            assertReadTimeIsReasonable(timeBeforeMethodCallNanos, readTimeNanosHolder.get());
        }
    }

    public void testReadCompleteCallbackNotCalledWhenNoBytesWereRead() throws IOException {
        MeteringCacheBlobReader.ReadCompleteCallback readCompleteCallback = mock(MeteringCacheBlobReader.ReadCompleteCallback.class);
        var meteringCacheBlobReader = new MeteringCacheBlobReader(createFakeCacheBlobReader(), readCompleteCallback);
        PlainActionFuture<InputStream> future = new PlainActionFuture<>();
        meteringCacheBlobReader.getRangeInputStream(randomInt(), randomIntBetween(16, 1024), future);
        InputStream meteredInputStream = safeGet(future);
        meteredInputStream.close();
        verifyNoInteractions(readCompleteCallback);
    }

    @TestLogging(value = "co.elastic.elasticsearch.stateless.cache.reader.MeteringCacheBlobReader:DEBUG", reason = "test debug log message")
    public void testExceptionIsLoggedAtDebugWhenCallbackThrows() throws IOException {
        RuntimeException callbackException = new RuntimeException("Callback exception");
        MeteringCacheBlobReader.ReadCompleteCallback throwingReadCompleteCallback = (bytesRead, timeToReadNanos) -> {
            throw callbackException;
        };
        var meteringCacheBlobReader = new MeteringCacheBlobReader(createFakeCacheBlobReader(), throwingReadCompleteCallback);
        PlainActionFuture<InputStream> future = new PlainActionFuture<>();
        meteringCacheBlobReader.getRangeInputStream(randomInt(), randomIntBetween(16, 1024), future);
        InputStream meteredInputStream = safeGet(future);
        // Read a byte so we execute the callback
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
