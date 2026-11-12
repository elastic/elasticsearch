/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.test.ESTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.file.StandardOpenOption.READ;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class SegmentBuffersTests extends ESTestCase {

    /**
     * A scoped buffer of a confined segment rejects a put from another thread, but the unscoped buffer
     * over the same memory accepts it, and the owner thread sees the bytes in the confined segment.
     */
    public void testOtherThreadPutIsVisibleToOwner() throws Exception {
        final int length = randomIntBetween(1, 4096);
        final byte[] expected = randomByteArrayOfLength(length);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment scratch = arena.allocate(length);
            ByteBuffer scoped = scratch.asByteBuffer();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            onOtherThread(() -> {
                try {
                    scoped.put(expected);
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            assertThat(failure.get(), instanceOf(WrongThreadException.class));

            ByteBuffer unscoped = SegmentBuffers.unscopedByteBuffer(scratch.address(), length);
            assertThat(unscoped.remaining(), equalTo(length));
            onOtherThread(() -> unscoped.put(expected));

            assertSegmentEquals(expected, scratch);
        }
    }

    /**
     * Another thread can fill a slice of the unscoped buffer with a positional {@link FileChannel} read, and the owner thread sees the
     * bytes in the confined segment.
     */
    public void testOtherThreadChannelReadIsVisibleToOwner() throws Exception {
        final byte[] fileData = randomByteArrayOfLength(randomIntBetween(2, 4096));
        final int sliceOffset = randomIntBetween(0, fileData.length - 1);
        final int sliceLength = randomIntBetween(1, fileData.length - sliceOffset);
        final Path file = createTempDir().resolve("data");
        Files.write(file, fileData);
        try (Arena arena = Arena.ofConfined(); FileChannel channel = FileChannel.open(file, READ)) {
            MemorySegment scratch = arena.allocate(sliceLength);
            ByteBuffer unscoped = SegmentBuffers.unscopedByteBuffer(scratch.address(), sliceLength);

            onOtherThread(() -> {
                ByteBuffer dst = unscoped.slice(0, sliceLength);
                int read = 0;
                while (read < sliceLength) {
                    int n = channel.read(dst, sliceOffset + read);
                    assertThat("unexpected EOF", n, greaterThanOrEqualTo(0));
                    read += n;
                }
            });

            byte[] expected = new byte[sliceLength];
            System.arraycopy(fileData, sliceOffset, expected, 0, sliceLength);
            assertSegmentEquals(expected, scratch);
        }
    }

    /** The buffer is bounded to the requested length, so it cannot write beyond a sliced segment's end. */
    public void testBufferIsBoundedToRequestedLength() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment whole = arena.allocate(64);
            int offset = randomIntBetween(1, 32);
            int length = randomIntBetween(1, 32);
            ByteBuffer unscoped = SegmentBuffers.unscopedByteBuffer(whole.address() + offset, length);
            assertThat(unscoped.capacity(), equalTo(length));
            expectThrows(BufferOverflowException.class, () -> unscoped.put(new byte[length + 1]));
        }
    }

    public void testInvalidRangeRejected() {
        expectThrows(IllegalArgumentException.class, () -> SegmentBuffers.unscopedByteBuffer(0L, randomIntBetween(0, 16)));
        expectThrows(IllegalArgumentException.class, () -> SegmentBuffers.unscopedByteBuffer(randomLongBetween(1, 1 << 20), -1));
    }

    private static void assertSegmentEquals(byte[] expected, MemorySegment actual) {
        assertThat(actual.byteSize(), equalTo((long) expected.length));
        for (int i = 0; i < expected.length; i++) {
            assertThat("byte mismatch at " + i, actual.get(ValueLayout.JAVA_BYTE, i), equalTo(expected[i]));
        }
    }
}
