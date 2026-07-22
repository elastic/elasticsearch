/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.core.SuppressForbidden;

import java.lang.foreign.MemorySegment;

/**
 * Behavioral tests for {@link ImplClassWriter}-generated code: unlike
 * {@link ImplClassWriterTests}, which never triggers {@code <clinit>} and only asserts on class/method
 * shape via reflection, these tests actually instantiate the generated class and invoke the method,
 * proving the emitted bounds-check bytecode really does throw (or not) depending on the input.
 *
 * <p>Each test binds to libc's {@code memcmp} with no {@code @LibrarySpecification} name, so
 * {@code <clinit>} resolves it via {@code LinkerHelper}'s default-lookup fallback instead of loading a
 * fake library — no native library build dependency, works on any platform. {@code memcmp} is a good
 * fit specifically because it's pure/read-only and doesn't itself care about alignment, so it's safe
 * to call regardless of whether our own checks behave correctly.
 */
@SuppressForbidden(
    reason = "tests instantiate package-private processor-generated classes and invoke their public methods "
        + "cross-package; getDeclaredConstructor/setAccessible is the only way to do that via reflection"
)
public class BoundsCheckGeneratedClassBehaviorTests extends ProcessorTestCase {

    /**
     * Proves the emitted {@code Objects.checkFromIndexSize} call really does throw on an undersized
     * segment, and really does let a correctly-sized call through to the native function.
     */
    public void testVectorSegmentCheckThrowsOnUndersizedSegment() throws Exception {
        String source = """
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
            """;

        CompilationResult result = compile("test.MemCmpLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.MemCmpLib$Impl");
        java.lang.reflect.Constructor<?> ctor = implClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object instance = ctor.newInstance();
        java.lang.reflect.Method memcmp = implClass.getMethod("memcmp", MemorySegment.class, MemorySegment.class, long.class);
        memcmp.setAccessible(true);

        try (var arena = java.lang.foreign.Arena.ofConfined()) {
            MemorySegment good = arena.allocate(8);
            MemorySegment tooSmall = arena.allocate(4);

            try {
                memcmp.invoke(instance, tooSmall, good, 8L);
                fail("Expected IndexOutOfBoundsException");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue("Expected IndexOutOfBoundsException, got: " + e.getCause(), e.getCause() instanceof IndexOutOfBoundsException);
            }

            MemorySegment a = arena.allocate(8);
            MemorySegment b = arena.allocate(8);
            for (int i = 0; i < 8; i++) {
                a.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
                b.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
            }
            int cmp = (int) memcmp.invoke(instance, a, b, 8L);
            assertEquals(0, cmp);
        }
    }

    /**
     * Proves the {@code aligned} attribute's emitted assert really does fire on a misaligned off-heap
     * segment (the test JVM runs with {@code -ea}), and really doesn't on an aligned one.
     */
    public void testAlignedVectorSegmentThrowsOnMisalignedSegment() throws Exception {
        String source = """
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
            """;

        CompilationResult result = compile("test.MemCmpLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.MemCmpLib$Impl");
        java.lang.reflect.Constructor<?> ctor = implClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object instance = ctor.newInstance();
        java.lang.reflect.Method memcmp = implClass.getMethod("memcmp", MemorySegment.class, MemorySegment.class, long.class);
        memcmp.setAccessible(true);

        try (var arena = java.lang.foreign.Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(16, 8);
            MemorySegment misaligned = buf.asSlice(1, 8);
            MemorySegment aligned = buf.asSlice(0, 8);

            try {
                memcmp.invoke(instance, misaligned, aligned, 1L);
                fail("Expected AssertionError");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue("Expected AssertionError, got: " + e.getCause(), e.getCause() instanceof AssertionError);
            }

            memcmp.invoke(instance, aligned, aligned, 1L);
        }
    }

    /**
     * Same idea as {@link #testVectorSegmentCheckThrowsOnUndersizedSegment}, for {@code @MatrixSegment}.
     * {@code rowsParam}/{@code colsParam} both reference the same parameter {@code n} to describe an n*n
     * "matrix", so the method's real arity stays at {@code (a, b, n)}, matching {@code memcmp}'s
     * actual signature.
     */
    public void testMatrixSegmentCheckThrowsOnUndersizedSegment() throws Exception {
        String source = """
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
            """;

        CompilationResult result = compile("test.MemCmpLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.MemCmpLib$Impl");
        java.lang.reflect.Constructor<?> ctor = implClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object instance = ctor.newInstance();
        java.lang.reflect.Method memcmp = implClass.getMethod("memcmp", MemorySegment.class, MemorySegment.class, long.class);
        memcmp.setAccessible(true);

        // rows = cols = n = 4, so each segment needs n*n = 16 bytes; memcmp itself only reads n=4.
        try (var arena = java.lang.foreign.Arena.ofConfined()) {
            MemorySegment good = arena.allocate(16);
            MemorySegment tooSmall = arena.allocate(8);

            try {
                memcmp.invoke(instance, tooSmall, good, 4L);
                fail("Expected IndexOutOfBoundsException");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue("Expected IndexOutOfBoundsException, got: " + e.getCause(), e.getCause() instanceof IndexOutOfBoundsException);
            }

            MemorySegment a = arena.allocate(16);
            MemorySegment b = arena.allocate(16);
            for (int i = 0; i < 4; i++) {
                a.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
                b.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
            }
            int cmp = (int) memcmp.invoke(instance, a, b, 4L);
            assertEquals(0, cmp);
        }
    }

    /**
     * Same idea as {@link #testAlignedVectorSegmentThrowsOnMisalignedSegment}, for
     * {@code @MatrixSegment}.
     */
    public void testAlignedMatrixSegmentThrowsOnMisalignedSegment() throws Exception {
        String source = """
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
            """;

        CompilationResult result = compile("test.MemCmpLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.MemCmpLib$Impl");
        java.lang.reflect.Constructor<?> ctor = implClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object instance = ctor.newInstance();
        java.lang.reflect.Method memcmp = implClass.getMethod("memcmp", MemorySegment.class, MemorySegment.class, long.class);
        memcmp.setAccessible(true);

        // rows = cols = n = 1 -> requires 1*1*64/8 = 8 bytes (elementBits=64), 8-byte aligned.
        try (var arena = java.lang.foreign.Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(16, 8);
            MemorySegment misaligned = buf.asSlice(1, 8);
            MemorySegment aligned = buf.asSlice(0, 8);

            try {
                memcmp.invoke(instance, misaligned, aligned, 1L);
                fail("Expected AssertionError");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue("Expected AssertionError, got: " + e.getCause(), e.getCause() instanceof AssertionError);
            }

            memcmp.invoke(instance, aligned, aligned, 1L);
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
    public void testMatrixSegmentPaddingBytesAddsToRowSize() throws Exception {
        String source = """
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
            """;

        CompilationResult result = compile("test.MemCmpLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.MemCmpLib$Impl");
        java.lang.reflect.Constructor<?> ctor = implClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object instance = ctor.newInstance();
        java.lang.reflect.Method memcmp = implClass.getMethod("memcmp", MemorySegment.class, MemorySegment.class, long.class);
        memcmp.setAccessible(true);

        // n = 4 -> rowBytes = 2*4 = 8, size = 4*8 = 32 bytes required; the packed size would be only 16.
        try (var arena = java.lang.foreign.Arena.ofConfined()) {
            MemorySegment good = arena.allocate(32);
            MemorySegment tooSmall = arena.allocate(16);

            try {
                memcmp.invoke(instance, tooSmall, good, 4L);
                fail("Expected IndexOutOfBoundsException");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue("Expected IndexOutOfBoundsException, got: " + e.getCause(), e.getCause() instanceof IndexOutOfBoundsException);
            }

            MemorySegment a = arena.allocate(32);
            MemorySegment b = arena.allocate(32);
            for (int i = 0; i < 4; i++) {
                a.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
                b.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
            }
            int cmp = (int) memcmp.invoke(instance, a, b, 4L);
            assertEquals(0, cmp);
        }
    }

    /**
     * The {@code paddingBytesParam} guard rejects a negative padding value with an
     * {@code IllegalArgumentException}, thrown before any size check or native call. All attributes
     * reference the same {@code n}, so passing {@code n = -1} drives the padding negative and the
     * emitted {@code if (paddingBytes < 0) throw} fires.
     */
    public void testMatrixSegmentNegativePaddingThrows() throws Exception {
        String source = """
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
            """;

        CompilationResult result = compile("test.MemCmpLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.MemCmpLib$Impl");
        java.lang.reflect.Constructor<?> ctor = implClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object instance = ctor.newInstance();
        java.lang.reflect.Method memcmp = implClass.getMethod("memcmp", MemorySegment.class, MemorySegment.class, long.class);
        memcmp.setAccessible(true);

        try (var arena = java.lang.foreign.Arena.ofConfined()) {
            MemorySegment a = arena.allocate(64);
            MemorySegment b = arena.allocate(64);
            try {
                memcmp.invoke(instance, a, b, -1L);
                fail("Expected IllegalArgumentException");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue("Expected IllegalArgumentException, got: " + e.getCause(), e.getCause() instanceof IllegalArgumentException);
            }
        }
    }

    /**
     * Verifies a sub-byte {@code elementBits} check rounds each row's byte size <em>up</em> to whole
     * bytes: a 2D segment is {@code rows} independently packed vectors, and each vector must have room
     * for all its bits.
     */
    public void testMatrixSegmentSubByteRowSizeRoundsUpToWholeBytes() throws Exception {
        String source = """
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
            """;

        CompilationResult result = compile("test.MemCmpLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.MemCmpLib$Impl");
        java.lang.reflect.Constructor<?> ctor = implClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object instance = ctor.newInstance();
        java.lang.reflect.Method memcmp = implClass.getMethod("memcmp", MemorySegment.class, MemorySegment.class, long.class);
        memcmp.setAccessible(true);

        // rows = cols = n = 3, elementBits = 4: each row is 3*4 = 12 bits -> ceil(12/8) = 2 bytes, so the
        // required size is rows * 2 = 6 bytes. A 5-byte segment must be rejected.
        try (var arena = java.lang.foreign.Arena.ofConfined()) {
            MemorySegment b = arena.allocate(6);
            MemorySegment tooSmall = arena.allocate(5);

            try {
                memcmp.invoke(instance, tooSmall, b, 3L);
                fail("Expected IndexOutOfBoundsException");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue("Expected IndexOutOfBoundsException, got: " + e.getCause(), e.getCause() instanceof IndexOutOfBoundsException);
            }

            MemorySegment a = arena.allocate(6);
            for (int i = 0; i < 3; i++) {
                a.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
                b.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
            }
            int cmp = (int) memcmp.invoke(instance, a, b, 3L);
            assertEquals(0, cmp);
        }
    }

    /**
     * Same rounding-up rule as {@link #testMatrixSegmentSubByteRowSizeRoundsUpToWholeBytes}, for a 1D
     * {@code @VectorSegment}: a sub-byte packed vector's byte size is {@code ceil(count*elementBits/8)},
     * so a segment sized to the floored value is rejected.
     */
    public void testVectorSegmentSubByteSizeRoundsUpToWholeBytes() throws Exception {
        String source = """
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
            """;

        CompilationResult result = compile("test.MemCmpLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.MemCmpLib$Impl");
        java.lang.reflect.Constructor<?> ctor = implClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object instance = ctor.newInstance();
        java.lang.reflect.Method memcmp = implClass.getMethod("memcmp", MemorySegment.class, MemorySegment.class, long.class);
        memcmp.setAccessible(true);

        // n = 3, elementBits = 4: 3*4 = 12 bits -> ceil(12/8) = 2 bytes required. A 1-byte segment must be
        // rejected even though a floored size (12/8 = 1) would accept it. The check throws before memcmp runs.
        try (var arena = java.lang.foreign.Arena.ofConfined()) {
            MemorySegment b = arena.allocate(3);
            MemorySegment tooSmall = arena.allocate(1);

            try {
                memcmp.invoke(instance, tooSmall, b, 3L);
                fail("Expected IndexOutOfBoundsException");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue("Expected IndexOutOfBoundsException, got: " + e.getCause(), e.getCause() instanceof IndexOutOfBoundsException);
            }

            MemorySegment a = arena.allocate(3);
            for (int i = 0; i < 3; i++) {
                a.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
                b.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
            }
            int cmp = (int) memcmp.invoke(instance, a, b, 3L);
            assertEquals(0, cmp);
        }
    }
}
