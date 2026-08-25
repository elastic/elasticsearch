/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Behavioral tests for {@link ImplClassWriter}-generated bounds-check code: unlike
 * {@link ImplClassWriterTests}' structural assertions on the emitted class/method shape, these tests
 * load the generated implementation through the {@code LibraryProvider} SPI (via {@link #loadLibrary})
 * and invoke it for real, proving the emitted bounds-check bytecode really does throw (or not)
 * depending on the input.
 *
 * <p>Each test binds to libc's {@code memcmp} with no {@code @LibrarySpecification} name, so
 * {@code <clinit>} resolves it via {@code LinkerHelper}'s default-lookup fallback instead of loading a
 * fake library — no native library build dependency, works on any platform. {@code memcmp} is a good
 * fit specifically because it's pure/read-only and doesn't itself care about alignment, so it's safe
 * to call regardless of whether our own checks behave correctly.
 */
public class BoundsCheckGeneratedClassBehaviorTests extends ProcessorTestCase {

    /**
     * Proves the emitted {@code Objects.checkFromIndexSize} call really does throw on an undersized
     * segment, and really does let a correctly-sized call through to the native function.
     */
    public void testVectorSegmentCheckThrowsOnUndersizedSegment() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @VectorSegment(countParam = "n", elementBits = 8) MemorySegment a,
                    @VectorSegment(countParam = "n", elementBits = 8) MemorySegment b,
                    long n);
            }
            """);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment good = arena.allocate(8);
            MemorySegment tooSmall = arena.allocate(4);
            lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", tooSmall, good, 8L);

            MemorySegment a = fill(arena.allocate(8));
            MemorySegment b = fill(arena.allocate(8));
            assertEquals(0, (int) lib.call("memcmp", a, b, 8L));
        }
    }

    /**
     * Proves the {@code aligned} attribute's emitted assert really does fire on a misaligned off-heap
     * segment (the test JVM runs with {@code -ea}), and really doesn't on an aligned one.
     */
    public void testAlignedVectorSegmentThrowsOnMisalignedSegment() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @VectorSegment(countParam = "n", elementBits = 64, aligned = true) MemorySegment a,
                    @VectorSegment(countParam = "n", elementBits = 64) MemorySegment b,
                    long n);
            }
            """);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(16, 8);
            MemorySegment misaligned = buf.asSlice(1, 8);
            MemorySegment aligned = buf.asSlice(0, 8);

            lib.expectThrows(AssertionError.class, "memcmp", misaligned, aligned, 1L);
            lib.call("memcmp", aligned, aligned, 1L);
        }
    }

    /**
     * Same idea as {@link #testVectorSegmentCheckThrowsOnUndersizedSegment}, for {@code @MatrixSegment}.
     * {@code rowsParam}/{@code colsParam} both reference the same parameter {@code n} to describe an n*n
     * "matrix", so the method's real arity stays at {@code (a, b, n)}, matching {@code memcmp}'s
     * actual signature.
     */
    public void testMatrixSegmentCheckThrowsOnUndersizedSegment() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8) MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8) MemorySegment b,
                    long n);
            }
            """);

        // rows = cols = n = 4, so each segment needs n*n = 16 bytes; memcmp itself only reads n=4.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment good = arena.allocate(16);
            MemorySegment tooSmall = arena.allocate(8);
            lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", tooSmall, good, 4L);

            MemorySegment a = fill(arena.allocate(16), 4);
            MemorySegment b = fill(arena.allocate(16), 4);
            assertEquals(0, (int) lib.call("memcmp", a, b, 4L));
        }
    }

    /**
     * Same idea as {@link #testAlignedVectorSegmentThrowsOnMisalignedSegment}, for
     * {@code @MatrixSegment}.
     */
    public void testAlignedMatrixSegmentThrowsOnMisalignedSegment() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 64, aligned = true) MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 64) MemorySegment b,
                    long n);
            }
            """);

        // rows = cols = n = 1 -> requires 1*1*64/8 = 8 bytes (elementBits=64), 8-byte aligned.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(16, 8);
            MemorySegment misaligned = buf.asSlice(1, 8);
            MemorySegment aligned = buf.asSlice(0, 8);

            lib.expectThrows(AssertionError.class, "memcmp", misaligned, aligned, 1L);
            lib.call("memcmp", aligned, aligned, 1L);
        }
    }

    /**
     * The {@code paddingBytesParam} size math. All attributes ({@code rowsParam}/{@code colsParam}/
     * {@code paddingBytesParam}) reference the same parameter {@code n}, so the method's real arity
     * stays at {@code (a, b, n)}, matching {@code memcmp}'s actual signature exactly.
     * With {@code elementBits = 8}, {@code rowBytes = n*8/8 + n = 2n}, so the required size is
     * {@code rows * rowBytes = n * 2n = 2n²} — twice the packed (no-padding) size {@code n²}. This
     * proves the padding is added to each row rather than ignored (a regression here would accept a
     * segment only {@code n²} bytes long).
     */
    public void testMatrixSegmentPaddingBytesAddsToRowSize() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8, paddingBytesParam = "n")
                    MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8, paddingBytesParam = "n")
                    MemorySegment b,
                    long n);
            }
            """);

        // n = 4 -> rowBytes = 2*4 = 8, size = 4*8 = 32 bytes required; the packed size would be only 16.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment good = arena.allocate(32);
            MemorySegment tooSmall = arena.allocate(16);
            lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", tooSmall, good, 4L);

            MemorySegment a = fill(arena.allocate(32), 4);
            MemorySegment b = fill(arena.allocate(32), 4);
            assertEquals(0, (int) lib.call("memcmp", a, b, 4L));
        }
    }

    /**
     * The {@code paddingBytesParam} guard rejects a negative padding value with an
     * {@code IllegalArgumentException}, thrown before any size check or native call. All attributes
     * reference the same {@code n}, so passing {@code n = -1} drives the padding negative and the
     * emitted {@code if (paddingBytes < 0) throw} fires.
     */
    public void testMatrixSegmentNegativePaddingThrows() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8, paddingBytesParam = "n")
                    MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8, paddingBytesParam = "n")
                    MemorySegment b,
                    long n);
            }
            """);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment a = arena.allocate(64);
            MemorySegment b = arena.allocate(64);
            lib.expectThrows(IllegalArgumentException.class, "memcmp", a, b, -1L);
        }
    }

    /**
     * Verifies a sub-byte {@code elementBits} check rounds each row's byte size <em>up</em> to whole
     * bytes: a 2D segment is {@code rows} independently packed vectors, and each vector must have room
     * for all its bits.
     */
    public void testMatrixSegmentSubByteRowSizeRoundsUpToWholeBytes() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 4) MemorySegment a,
                    MemorySegment b,
                    long n);
            }
            """);

        // rows = cols = n = 3, elementBits = 4: each row is 3*4 = 12 bits -> ceil(12/8) = 2 bytes, so the
        // required size is rows * 2 = 6 bytes. A 5-byte segment must be rejected.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment b = fill(arena.allocate(6), 3);
            MemorySegment tooSmall = arena.allocate(5);
            lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", tooSmall, b, 3L);

            MemorySegment a = fill(arena.allocate(6), 3);
            assertEquals(0, (int) lib.call("memcmp", a, b, 3L));
        }
    }

    /**
     * Same rounding-up rule as {@link #testMatrixSegmentSubByteRowSizeRoundsUpToWholeBytes}, for a 1D
     * {@code @VectorSegment}: a sub-byte packed vector's byte size is {@code ceil(count*elementBits/8)},
     * so a segment sized to the floored value is rejected.
     */
    public void testVectorSegmentSubByteSizeRoundsUpToWholeBytes() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @VectorSegment(countParam = "n", elementBits = 4) MemorySegment a,
                    MemorySegment b,
                    long n);
            }
            """);

        // n = 3, elementBits = 4: 3*4 = 12 bits -> ceil(12/8) = 2 bytes required. A 1-byte segment must be
        // rejected even though a floored size (12/8 = 1) would accept it. The check throws before memcmp runs.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment b = fill(arena.allocate(3), 3);
            MemorySegment tooSmall = arena.allocate(1);
            lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", tooSmall, b, 3L);

            MemorySegment a = fill(arena.allocate(3), 3);
            assertEquals(0, (int) lib.call("memcmp", a, b, 3L));
        }
    }

    /**
     * A negative element count must be rejected. Rounding the bit count up to whole bytes with
     * {@code (bits + 7) / 8} would truncate a small negative product toward zero -- for
     * {@code count = -1, elementBits = 8}, {@code (-8 + 7) / 8 == 0} -- and the check would pass a
     * negative count straight through to the native call.
     */
    public void testVectorSegmentRejectsNegativeCount() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @VectorSegment(countParam = "n", elementBits = 8) MemorySegment a,
                    MemorySegment b,
                    long n);
            }
            """);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment a = arena.allocate(3);
            MemorySegment b = arena.allocate(3);
            // -1 is the value that survives the (bits + 7) / 8 form; -2 fails under both forms.
            for (long n : new long[] { -1L, -2L }) {
                lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", a, b, n);
            }
        }
    }

    /** Fills the whole segment with ascending byte values and returns it. */
    private static MemorySegment fill(MemorySegment segment) {
        return fill(segment, (int) segment.byteSize());
    }

    /** Fills the first {@code count} bytes of the segment with ascending byte values and returns it. */
    private static MemorySegment fill(MemorySegment segment, int count) {
        for (int i = 0; i < count; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, (byte) i);
        }
        return segment;
    }
}
