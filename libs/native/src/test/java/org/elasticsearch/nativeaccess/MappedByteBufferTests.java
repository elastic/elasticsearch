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
import java.util.stream.IntStream;

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
        var tmp = createTempDir();
        Path file = tmp.resolve("testBasic");
        Files.write(file, newByteArray(size), CREATE, WRITE);
        // we need to unwrap our test-only file system layers
        file = Unwrappable.unwrapAll(file);
        try (
            FileChannel fileChannel = FileChannel.open(file, READ);
            CloseableMappedByteBuffer mappedByteBuffer = nativeAccess.map(fileChannel, MapMode.READ_ONLY, 0, size)
        ) {
            mappedByteBuffer.prefetch(0, size);

            var buffer = mappedByteBuffer.buffer();
            assertThat(buffer.position(), equalTo(0));
            assertThat(buffer.limit(), equalTo(size));
            assertThat((byte) (size - 1), equalTo(buffer.get(size - 1)));
            mappedByteBuffer.prefetch(0, size);

            assertSliceOfBuffer(mappedByteBuffer, 0, size);
            assertSliceOfBuffer(mappedByteBuffer, 1, size - 1);
            assertSliceOfBuffer(mappedByteBuffer, 2, size - 2);
            assertSliceOfBuffer(mappedByteBuffer, 3, size - 3);

            assertOutOfBounds(mappedByteBuffer, size);
        }
    }

    // We just check that the variations do not fail or crash - no
    // positive assertion that the prefetch has any observable action
    public void testPrefetchWithOffsets() throws IOException {
        int size = randomIntBetween(10, 4096);
        var tmp = createTempDir();
        Path file = tmp.resolve("testPrefetchWithOffsets");
        Files.write(file, newByteArray(size), CREATE, WRITE);
        // we need to unwrap our test-only file system layers
        file = Unwrappable.unwrapAll(file);
        try (
            FileChannel fileChannel = FileChannel.open(file, READ);
            CloseableMappedByteBuffer mappedByteBuffer = nativeAccess.map(fileChannel, MapMode.READ_ONLY, 0, size)
        ) {
            mappedByteBuffer.prefetch(0, size);
            mappedByteBuffer.prefetch(0, 0);
            mappedByteBuffer.prefetch(0, size - 1);
            mappedByteBuffer.prefetch(0, size - 2);
            mappedByteBuffer.prefetch(0, size - 3);
            mappedByteBuffer.prefetch(0, randomIntBetween(1, size));
            mappedByteBuffer.prefetch(1, size - 1);
            mappedByteBuffer.prefetch(2, size - 2);
            mappedByteBuffer.prefetch(3, size - 3);
            mappedByteBuffer.prefetch(4, size - 4);
            mappedByteBuffer.prefetch(1, randomIntBetween(2, size - 1));

            assertOutOfBounds(mappedByteBuffer, size);
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

    private byte[] newByteArray(int size) {
        byte[] buffer = new byte[size];
        IntStream.range(0, buffer.length).forEach(i -> buffer[i] = (byte) i);
        return buffer;
    }
}
