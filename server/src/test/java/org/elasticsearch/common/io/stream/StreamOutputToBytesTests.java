/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.common.unit.ByteSizeUnit.KB;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class StreamOutputToBytesTests extends ESTestCase {

    public void testRandomWrites() throws IOException {
        final var bufferPool = randomByteArrayOfLength(between(KB.toIntBytes(1), KB.toIntBytes(4)));
        final var bufferStart = between(0, bufferPool.length - KB.toIntBytes(1));
        final var bufferLen = between(1, bufferPool.length - bufferStart);
        final var buffer = new BytesRef(bufferPool, bufferStart, bufferLen);

        final var bufferPoolCopy = ArrayUtil.copyArray(bufferPool); // kept so we can check no out-of-bounds writes
        Arrays.fill(bufferPoolCopy, bufferStart, bufferStart + bufferLen, (byte) 0xa5);

        final var isFullBufferWrite = equalTo(bufferLen);
        final var isExpectedWriteSize = new AtomicReference<>(isFullBufferWrite);

        final var targetSize = between(0, PageCacheRecycler.PAGE_SIZE_IN_BYTES * 2);

        try (
            var countingStream = new CountingStreamOutput();
            var mockRecycler = new MockBytesRefRecycler();
            var recyclerBytesStream = new RecyclerBytesStreamOutput(mockRecycler);
            var plainBytesStream = new BytesStreamOutput();
            var toBeBufferedStream = new ByteArrayOutputStream(targetSize) {
                @Override
                public void write(int b) {
                    fail("buffered stream should not write single bytes");
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    assertThat(len, isExpectedWriteSize.get());
                    super.write(b, off, len);
                }
            };
            var bufferedStream = new BufferedStreamOutput(toBeBufferedStream, buffer);
            var toBeWrappedStream = new ByteArrayOutputStream(targetSize);
            var wrappedStream = new OutputStreamStreamOutput(toBeWrappedStream)
        ) {
            final var streams = List.of(countingStream, recyclerBytesStream, plainBytesStream, bufferedStream, wrappedStream);

            final var writers = List.<Supplier<CheckedConsumer<StreamOutput, IOException>>>of(() -> {
                final var b = randomByte();
                return s -> s.writeByte(b);
            }, () -> {
                final var bytes = randomByteArrayOfLength(between(1, bufferLen * 4));
                final var start = between(0, bytes.length - 1);
                final var length = between(0, bytes.length - start - 1);
                return s -> {
                    if (length >= bufferLen) {
                        isExpectedWriteSize.set(greaterThanOrEqualTo(bufferLen)); // large writes may bypass the buffer
                    }
                    s.writeBytes(bytes, start, length);
                    isExpectedWriteSize.set(isFullBufferWrite);
                };
            }, () -> {
                final var value = randomShort();
                return s -> s.writeShort(value);
            }, () -> {
                final var value = randomInt();
                return s -> s.writeInt(value);
            }, () -> {
                final var value = randomInt();
                return s -> s.writeIntLE(value);
            }, () -> {
                final var value = randomLong();
                return s -> s.writeLong(value);
            }, () -> {
                final var value = randomLong();
                return s -> s.writeLongLE(value);
            }, () -> {
                final var value = randomInt() >> between(0, Integer.SIZE);
                return s -> s.writeVInt(value);
            }, () -> {
                final var value = randomInts(between(0, 200)).map(i -> i >> between(0, Integer.SIZE)).toArray();
                return s -> s.writeVIntArray(value);
            }, () -> {
                final var value = randomNonNegativeLong() >> between(0, Long.SIZE);
                return s -> s.writeVLong(value);
            }, () -> {
                final var value = randomLong() >> between(0, Long.SIZE);
                return s -> s.writeZLong(value);
            }, () -> {
                final var value = randomUnicodeOfLengthBetween(0, 2000);
                return s -> s.writeString(value);
            }, () -> {
                final var value = randomBoolean() ? null : randomUnicodeOfLengthBetween(0, 2000);
                return s -> s.writeOptionalString(value);
            }, () -> {
                final var value = randomUnicodeOfLengthBetween(0, 2000);
                return s -> s.writeGenericString(value);
            });

            while (countingStream.position() < targetSize) {
                var writerIndex = between(0, writers.size() - 1);
                var writer = writers.get(writerIndex).get();
                for (var stream : streams) {
                    writer.accept(stream);
                }
                assertEquals("recyclerBytesStream after " + writerIndex, countingStream.position(), recyclerBytesStream.position());
                assertEquals("plainBytesStream after " + writerIndex, countingStream.position(), plainBytesStream.position());
                assertEquals("bufferedStream after " + writerIndex, countingStream.position(), bufferedStream.position());
                assertEquals("wrappedStream after " + writerIndex, countingStream.position(), wrappedStream.position());
            }

            isExpectedWriteSize.set(allOf(lessThanOrEqualTo(bufferLen), greaterThan(0))); // last write may be undersized
            for (var stream : streams) {
                stream.flush();
            }
            final var isExpectedBytes = equalBytes(new BytesArray(toBeWrappedStream.toByteArray()));
            assertThat(new BytesArray(toBeBufferedStream.toByteArray()), isExpectedBytes);
            assertThat(recyclerBytesStream.bytes(), isExpectedBytes);
            assertThat(plainBytesStream.bytes(), isExpectedBytes);
        }

        if (Assertions.ENABLED == false) {
            // in this case the buffer is not trashed on flush so we must trash its contents manually before the corruption check
            Arrays.fill(bufferPool, bufferStart, bufferStart + bufferLen, (byte) 0xa5);
        }
        assertArrayEquals("wrote out of bounds", bufferPoolCopy, bufferPool);
    }

    public void testDoubleClose() throws IOException {
        var buffered = new BufferedStreamOutput(new AssertClosedOnceOutputStream(), new BytesRef(new byte[10], 0, 10));
        buffered.close();
        buffered.close();
    }

    public void testDoubleCloseInTryWithResources() throws IOException {
        try (
            var buffered = new BufferedStreamOutput(new AssertClosedOnceOutputStream(), new BytesRef(new byte[10], 0, 10));
            var wrapper = new FilterOutputStream(buffered)
        ) {
            if (randomBoolean()) {
                wrapper.write(0);
            }
            // {wrapper} is closed first, and propagates the close to {buffered}, but it's a separate resource so {buffered} is then closed
            // again
        }
    }

    private static class AssertClosedOnceOutputStream extends OutputStream {
        private final AtomicBoolean isClosed = new AtomicBoolean();

        @Override
        public void write(int b) {}

        @Override
        public void close() {
            assertTrue(isClosed.compareAndSet(false, true));
        }
    }
}
