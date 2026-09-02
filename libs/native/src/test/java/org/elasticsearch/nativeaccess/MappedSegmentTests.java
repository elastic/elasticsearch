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
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class MappedSegmentTests extends ESTestCase {

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
        file = Unwrappable.unwrapAll(file);
        int len = size - filePositionOffset;
        try (
            FileChannel fileChannel = FileChannel.open(file, READ);
            MappedSegment mapped = nativeAccess.map(fileChannel, MapMode.READ_ONLY, filePositionOffset, len)
        ) {
            mapped.prefetch(0, len);

            var segment = mapped.segment();
            assertThat(segment.byteSize(), equalTo((long) len));
            assertThat(segment.get(ValueLayout.JAVA_BYTE, 0), equalTo((byte) 0));
            assertThat(segment.get(ValueLayout.JAVA_BYTE, len - 1), equalTo((byte) (len - 1)));
            mapped.prefetch(0, len);

            assertSliceOfSegment(mapped, 0, len);
            assertSliceOfSegment(mapped, 1, len - 1);
            assertSliceOfSegment(mapped, 2, len - 2);
            assertSliceOfSegment(mapped, 3, len - 3);

            assertOutOfBounds(mapped, len);
        }
    }

    public void testPrefetchWithOffsets() throws IOException {
        testPrefetchWithOffsetsImpl(0);
        testPrefetchWithOffsetsImpl(1);
        testPrefetchWithOffsetsImpl(2);
        testPrefetchWithOffsetsImpl(3);
    }

    void testPrefetchWithOffsetsImpl(int filePositionOffset) throws IOException {
        int size = randomIntBetween(10, 4096);
        var tmp = createTempDir();
        Path file = tmp.resolve("testPrefetchWithOffsets");
        Files.write(file, newByteArray(size, 0), CREATE, WRITE);
        file = Unwrappable.unwrapAll(file);
        int len = size - filePositionOffset;
        try (
            FileChannel fileChannel = FileChannel.open(file, READ);
            MappedSegment mapped = nativeAccess.map(fileChannel, MapMode.READ_ONLY, filePositionOffset, len)
        ) {
            mapped.prefetch(0, len);
            mapped.prefetch(0, 0);
            mapped.prefetch(0, len - 1);
            mapped.prefetch(0, len - 2);
            mapped.prefetch(0, len - 3);
            mapped.prefetch(0, randomIntBetween(1, len));
            mapped.prefetch(1, len - 1);
            mapped.prefetch(2, len - 2);
            mapped.prefetch(3, len - 3);
            mapped.prefetch(4, len - 4);
            mapped.prefetch(1, randomIntBetween(2, len - 1));

            assertOutOfBounds(mapped, len);
        }
    }

    static final Class<IndexOutOfBoundsException> IOOBE = IndexOutOfBoundsException.class;

    static void assertOutOfBounds(MappedSegment mapped, int size) {
        expectThrows(IOOBE, () -> mapped.prefetch(-2, size));
        expectThrows(IOOBE, () -> mapped.prefetch(-1, size));
        expectThrows(IOOBE, () -> mapped.prefetch(1, size));
        expectThrows(IOOBE, () -> mapped.prefetch(2, size));
        expectThrows(IOOBE, () -> mapped.prefetch(3, size));
        expectThrows(IOOBE, () -> mapped.prefetch(0, size + 1));
        expectThrows(IOOBE, () -> mapped.prefetch(0, size + 2));
    }

    public void testSlicePreservesConcreteType() throws IOException {
        int size = 4096;
        var tmp = createTempDir();
        Path file = tmp.resolve("testSliceType");
        Files.write(file, newByteArray(size, 0), CREATE, WRITE);
        file = Unwrappable.unwrapAll(file);
        try (
            FileChannel fileChannel = FileChannel.open(file, READ);
            MappedSegment mapped = nativeAccess.map(fileChannel, MapMode.READ_ONLY, 0, size)
        ) {
            try (var slice = mapped.slice(0, size)) {
                assertThat(slice, instanceOf(mapped.getClass()));
            }
            try (var slice = mapped.slice(0, 1024)) {
                assertThat(slice, instanceOf(mapped.getClass()));
            }
            try (var slice = mapped.slice(1024, 1024)) {
                assertThat(slice, instanceOf(mapped.getClass()));
            }
        }
    }

    // Validates that a slice of the mapped segment has correct bounds, content, and that
    // closing the slice does not invalidate the parent segment.
    static void assertSliceOfSegment(MappedSegment mapped, int offset, int length) {
        var parentSegment = mapped.segment();
        try (var slice = mapped.slice(offset, length)) {
            slice.prefetch(0, length);

            var sliceSegment = slice.segment();
            assertThat(sliceSegment.byteSize(), equalTo((long) length));
            assertThat(sliceSegment.get(ValueLayout.JAVA_BYTE, 0), equalTo((byte) offset));
            byte expectedLastByte = parentSegment.get(ValueLayout.JAVA_BYTE, parentSegment.byteSize() - 1);
            byte sliceLastByte = sliceSegment.get(ValueLayout.JAVA_BYTE, sliceSegment.byteSize() - 1);
            assertThat(sliceLastByte, equalTo(expectedLastByte));

            assertOutOfBounds(slice, length);
        }
        // a closed slice should not close the parent — verify the parent is still readable
        assertThat(parentSegment.get(ValueLayout.JAVA_BYTE, offset), equalTo((byte) offset));
    }

    public void testMadviseWithVariousAdvice() throws IOException {
        int size = randomIntBetween(10, 4096);
        var tmp = createTempDir();
        Path file = tmp.resolve("testMadvise");
        Files.write(file, newByteArray(size, 0), CREATE, WRITE);
        file = Unwrappable.unwrapAll(file);
        try (
            FileChannel fileChannel = FileChannel.open(file, READ);
            MappedSegment mapped = nativeAccess.map(fileChannel, MapMode.READ_ONLY, 0, size)
        ) {
            mapped.madvise(0, size, MadviseAdvice.NORMAL);
            mapped.madvise(0, size, MadviseAdvice.RANDOM);
            mapped.madvise(0, size, MadviseAdvice.NORMAL);
        }
    }

    public void testMadviseWithOffsets() throws IOException {
        int size = randomIntBetween(100, 4096);
        var tmp = createTempDir();
        Path file = tmp.resolve("testMadviseOffsets");
        Files.write(file, newByteArray(size, 0), CREATE, WRITE);
        file = Unwrappable.unwrapAll(file);
        try (
            FileChannel fileChannel = FileChannel.open(file, READ);
            MappedSegment mapped = nativeAccess.map(fileChannel, MapMode.READ_ONLY, 0, size)
        ) {
            int mid = size / 2;
            mapped.madvise(0, mid, MadviseAdvice.RANDOM);
            mapped.madvise(mid, size - mid, MadviseAdvice.NORMAL);
            mapped.madvise(0, 0, MadviseAdvice.RANDOM);
        }
    }

    public void testMadviseOutOfBounds() throws IOException {
        int size = randomIntBetween(10, 4096);
        var tmp = createTempDir();
        Path file = tmp.resolve("testMadviseOOB");
        Files.write(file, newByteArray(size, 0), CREATE, WRITE);
        file = Unwrappable.unwrapAll(file);
        try (
            FileChannel fileChannel = FileChannel.open(file, READ);
            MappedSegment mapped = nativeAccess.map(fileChannel, MapMode.READ_ONLY, 0, size)
        ) {
            expectThrows(IOOBE, () -> mapped.madvise(-1, size, MadviseAdvice.NORMAL));
            expectThrows(IOOBE, () -> mapped.madvise(0, size + 1, MadviseAdvice.NORMAL));
            expectThrows(IOOBE, () -> mapped.madvise(1, size, MadviseAdvice.NORMAL));
        }
    }

    // Creates a byte array where bytes at positions [offset..size-1] contain values 0, 1, 2, ...
    // and bytes before the offset are 0xFF. This allows tests to verify correct file mapping offsets.
    private byte[] newByteArray(int size, int offset) {
        byte[] buffer = new byte[size];
        Arrays.fill(buffer, (byte) 0xFF);
        for (int i = 0; i < buffer.length - offset; i++) {
            buffer[i + offset] = (byte) i;
        }
        return buffer;
    }
}
