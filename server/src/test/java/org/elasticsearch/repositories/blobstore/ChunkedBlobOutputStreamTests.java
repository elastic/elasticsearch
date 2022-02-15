/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class ChunkedBlobOutputStreamTests extends ESTestCase {

    private BigArrays bigArrays;

    @Override
    public void setUp() throws Exception {
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSuccessfulChunkedWrite() throws IOException {
        final long chunkSize = randomLongBetween(10, 1024);
        final CRC32 checksumIn = new CRC32();
        final CRC32 checksumOut = new CRC32();
        final CheckedOutputStream out = new CheckedOutputStream(OutputStream.nullOutputStream(), checksumOut);
        final AtomicLong writtenBytesCounter = new AtomicLong(0L);
        final long bytesToWrite = randomLongBetween(chunkSize - 5, 1000 * chunkSize);
        long written = 0;
        try (ChunkedBlobOutputStream<Integer> stream = new ChunkedBlobOutputStream<>(bigArrays, chunkSize) {

            private final AtomicInteger partIdSupplier = new AtomicInteger();

            @Override
            protected void flushBuffer() throws IOException {
                final BytesReference bytes = buffer.bytes();
                bytes.writeTo(out);
                writtenBytesCounter.addAndGet(bytes.length());
                finishPart(partIdSupplier.incrementAndGet());
            }

            @Override
            protected void onCompletion() throws IOException {
                if (buffer.size() > 0) {
                    flushBuffer();
                }
                out.flush();
                for (int i = 0; i < partIdSupplier.get(); i++) {
                    assertEquals((long) i + 1, (long) parts.get(i));
                }
            }

            @Override
            protected void onFailure() {
                fail("not supposed to fail");
            }
        }) {
            final byte[] buffer = new byte[randomInt(Math.toIntExact(2 * chunkSize)) + 1];
            while (written < bytesToWrite) {
                if (randomBoolean()) {
                    random().nextBytes(buffer);
                    final int offset = randomInt(buffer.length - 1);
                    final int length = Math.toIntExact(Math.min(bytesToWrite - written, buffer.length - offset));
                    stream.write(buffer, offset, length);
                    checksumIn.update(buffer, offset, length);
                    written += length;
                } else {
                    int oneByte = randomByte();
                    stream.write(oneByte);
                    checksumIn.update(oneByte);
                    written++;
                }
            }
            stream.markSuccess();
        }
        assertEquals(bytesToWrite, written);
        assertEquals(bytesToWrite, writtenBytesCounter.get());
        assertEquals(checksumIn.getValue(), checksumOut.getValue());
    }

    public void testExceptionDuringChunkedWrite() throws IOException {
        final long chunkSize = randomLongBetween(10, 1024);
        final AtomicLong writtenBytesCounter = new AtomicLong(0L);
        final long bytesToWrite = randomLongBetween(chunkSize - 5, 1000 * chunkSize);
        long written = 0;
        final AtomicBoolean onFailureCalled = new AtomicBoolean(false);
        try (ChunkedBlobOutputStream<Integer> stream = new ChunkedBlobOutputStream<>(bigArrays, chunkSize) {

            private final AtomicInteger partIdSupplier = new AtomicInteger();

            @Override
            protected void flushBuffer() {
                writtenBytesCounter.addAndGet(buffer.size());
                finishPart(partIdSupplier.incrementAndGet());
            }

            @Override
            protected void onCompletion() {
                fail("supposed to fail");
            }

            @Override
            protected void onFailure() {
                for (int i = 0; i < partIdSupplier.get(); i++) {
                    assertEquals((long) i + 1, (long) parts.get(i));
                }
                assertTrue(onFailureCalled.compareAndSet(false, true));
            }
        }) {
            final byte[] buffer = new byte[randomInt(Math.toIntExact(2 * chunkSize)) + 1];
            while (written < bytesToWrite) {
                if (rarely()) {
                    break;
                } else if (randomBoolean()) {
                    random().nextBytes(buffer);
                    final int offset = randomInt(buffer.length - 1);
                    final int length = Math.toIntExact(Math.min(bytesToWrite - written, buffer.length - offset));
                    stream.write(buffer, offset, length);
                    written += length;
                } else {
                    int oneByte = randomByte();
                    stream.write(oneByte);
                    written++;
                }
            }
        }
        assertTrue(onFailureCalled.get());
    }
}
