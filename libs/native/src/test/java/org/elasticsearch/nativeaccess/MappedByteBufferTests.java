/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.apache.lucene.util.Unwrappable;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.Matchers.equalTo;

public class MappedByteBufferTests extends ESTestCase {

    static NativeAccess nativeAccess;

    @BeforeClass
    public static void getAccess() {
        nativeAccess = NativeAccess.instance();
    }

    public void testBasic() throws IOException {
        int size = randomIntBetween(10, 4096);
        testBasicImpl(size, 0);
    }

    public void testBasicWithFileMapOffset() throws IOException {
        int size = randomIntBetween(10, 4096);
        testBasicImpl(size, 1);
    }

    public void testBasicTiny() throws IOException {
        int size = randomIntBetween(10, 20);
        testBasicImpl(size, 0);
        testBasicImpl(size, 1);
    }

    void testBasicImpl(int size, int filePositionOffset) throws IOException {
        var tmp = createTempDir();
        Path file = tmp.resolve("testBasic");
        Files.write(file, newByteArray(size, filePositionOffset), CREATE, WRITE);
        // we need to unwrap our test-only file system layers
        file = Unwrappable.unwrapAll(file);
        int len = size - filePositionOffset;
        try (
            FileChannel fileChannel = FileChannel.open(file, READ);
            CloseableMappedByteBuffer mappedByteBuffer = nativeAccess.map(fileChannel, MapMode.READ_ONLY, filePositionOffset, len)
        ) {
            mappedByteBuffer.prefetch(0, len);

            var buffer = mappedByteBuffer.buffer();
            assertThat(buffer.position(), equalTo(0));
            assertThat(buffer.limit(), equalTo(len));
            assertThat((byte) (0), equalTo(buffer.get(0)));  // expected first value
            assertThat((byte) (len - 1), equalTo(buffer.get(len - 1))); // expected last value
            mappedByteBuffer.prefetch(0, len);

            assertSliceOfBuffer(mappedByteBuffer, 0, len);
            assertSliceOfBuffer(mappedByteBuffer, 1, len - 1);
            assertSliceOfBuffer(mappedByteBuffer, 2, len - 2);
            assertSliceOfBuffer(mappedByteBuffer, 3, len - 3);

            assertOutOfBounds(mappedByteBuffer, len);
        }
    }

    public void testPrefetchWithOffsets() throws IOException {
        testPrefetchWithOffsetsImpl(0);
        testPrefetchWithOffsetsImpl(1);
        testPrefetchWithOffsetsImpl(2);
        testPrefetchWithOffsetsImpl(3);
    }

    // We just check that the variations do not fail or crash - no
    // positive assertion that the prefetch has any observable effect
    void testPrefetchWithOffsetsImpl(int filePositionOffset) throws IOException {
        int size = randomIntBetween(10, 4096);
        var tmp = createTempDir();
        Path file = tmp.resolve("testPrefetchWithOffsets");
        Files.write(file, newByteArray(size, 0), CREATE, WRITE);
        // we need to unwrap our test-only file system layers
        file = Unwrappable.unwrapAll(file);
        int len = size - filePositionOffset;
        try (
            FileChannel fileChannel = FileChannel.open(file, READ);
            CloseableMappedByteBuffer mappedByteBuffer = nativeAccess.map(fileChannel, MapMode.READ_ONLY, filePositionOffset, len)
        ) {
            mappedByteBuffer.prefetch(0, len);
            mappedByteBuffer.prefetch(0, 0);
            mappedByteBuffer.prefetch(0, len - 1);
            mappedByteBuffer.prefetch(0, len - 2);
            mappedByteBuffer.prefetch(0, len - 3);
            mappedByteBuffer.prefetch(0, randomIntBetween(1, len));
            mappedByteBuffer.prefetch(1, len - 1);
            mappedByteBuffer.prefetch(2, len - 2);
            mappedByteBuffer.prefetch(3, len - 3);
            mappedByteBuffer.prefetch(4, len - 4);
            mappedByteBuffer.prefetch(1, randomIntBetween(2, len - 1));

            assertOutOfBounds(mappedByteBuffer, len);
        }
    }

    static final Class<IndexOutOfBoundsException> IOOBE = IndexOutOfBoundsException.class;

    static void assertOutOfBounds(CloseableMappedByteBuffer mappedByteBuffer, int size) {
        expectThrows(IOOBE, () -> mappedByteBuffer.prefetch(-2, size));
        expectThrows(IOOBE, () -> mappedByteBuffer.prefetch(-1, size));
        expectThrows(IOOBE, () -> mappedByteBuffer.prefetch(1, size));
        expectThrows(IOOBE, () -> mappedByteBuffer.prefetch(2, size));
        expectThrows(IOOBE, () -> mappedByteBuffer.prefetch(3, size));
        expectThrows(IOOBE, () -> mappedByteBuffer.prefetch(0, size + 1));
        expectThrows(IOOBE, () -> mappedByteBuffer.prefetch(0, size + 2));
    }

    static void assertSliceOfBuffer(CloseableMappedByteBuffer mappedByteBuffer, int offset, int length) {
        var buffer = mappedByteBuffer.buffer();
        try (var slice = mappedByteBuffer.slice(offset, length)) {
            slice.prefetch(0, length);

            // sanitize backing data
            var sliceBuffer = slice.buffer();
            assertThat(sliceBuffer.position(), equalTo(0));
            assertThat(sliceBuffer.limit(), equalTo(length));
            assertThat(sliceBuffer.get(), equalTo((byte) offset));
            byte expectedLastByte = buffer.get(buffer.limit() - 1);
            byte sliceLastByte = sliceBuffer.get(sliceBuffer.limit() - 1);
            assertThat(sliceLastByte, equalTo(expectedLastByte));

            assertOutOfBounds(slice, length);
        }
    }

    // Creates a byte array containing monotonically incrementing values, starting
    // with a value of 0 at the given offset. Useful to assert positional values.
    private byte[] newByteArray(int size, int offset) {
        byte[] buffer = new byte[size];
        Arrays.fill(buffer, (byte) 0xFF);
        for (int i = 0; i < buffer.length - offset; i++) {
            buffer[i + offset] = (byte) i;
        }
        return buffer;
    }
}
