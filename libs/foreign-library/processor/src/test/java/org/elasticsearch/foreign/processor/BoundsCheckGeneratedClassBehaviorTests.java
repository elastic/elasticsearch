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
                    @VectorSegment(countParam = "n", elementBytes = 1) MemorySegment a,
                    @VectorSegment(countParam = "n", elementBytes = 1) MemorySegment b,
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
                    @VectorSegment(countParam = "n", elementBytes = 8, aligned = true) MemorySegment a,
                    @VectorSegment(countParam = "n", elementBytes = 8) MemorySegment b,
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
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBytes = 1) MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBytes = 1) MemorySegment b,
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
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBytes = 8, aligned = true) MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBytes = 8) MemorySegment b,
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

        // rows = cols = n = 1 -> requires 1*1*8 = 8 bytes (elementBytes=8), 8-byte aligned.
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
     * The {@code rowPitchBytesParam} relational check.
     * All attributes ({@code rowsParam}/{@code colsParam}/{@code rowPitchBytesParam}) reference the
     * same parameter {@code n}, so the method's real arity stays at {@code (a, b, n)}, matching
     * {@code memcmp}'s actual signature exactly.
     * With {@code elementBytes = 2}, {@code rowBytes = n*16/8 = 2n} while {@code pitch = n}, so
     * {@code pitch < rowBytes} for every {@code n > 0}: this always throws, by construction.
     */
    public void testRowPitchBytesCheckThrowsOnTooSmallPitch() throws Exception {
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
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBytes = 2, rowPitchBytesParam = "n")
                    MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBytes = 2) MemorySegment b,
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
                memcmp.invoke(instance, a, b, 4L);
                fail("Expected IllegalArgumentException");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue("Expected IllegalArgumentException, got: " + e.getCause(), e.getCause() instanceof IllegalArgumentException);
            }
        }
    }

    /**
     * The {@code rowPitchBytesParam} relational check, non-throwing branch.
     * With {@code elementBytes = 1}, {@code rowBytes = n*8/8 = n}, equal to {@code pitch = n},
     * so {@code pitch >= rowBytes} holds and the check passes; the size is then computed as
     * {@code rows*pitch = n*n}, and the call proceeds to the real {@code memcmp}.
     */
    public void testRowPitchBytesCheckDoesNotThrowOnSufficientPitch() throws Exception {
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
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBytes = 1, rowPitchBytesParam = "n")
                    MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBytes = 1, rowPitchBytesParam = "n")
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

        // n = 4 -> rows*pitch = 4*4 = 16 bytes required; memcmp itself only reads n=4.
        try (var arena = java.lang.foreign.Arena.ofConfined()) {
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
     * Verifies the sub-byte {@code elementBits} path, focusing in particular on the byte
     * size math.
     */
    public void testMatrixSegmentElementBitsMultipliesBeforeDividing() throws Exception {
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

        // rows = cols = n = 3, elementBits = 4: correct size = (3*3*4)/8 = 4 bytes. A naive
        // per-row computation ((3*4)/8 = 1, then 3*1 = 3) would wrongly accept a 3-byte segment.
        try (var arena = java.lang.foreign.Arena.ofConfined()) {
            MemorySegment b = arena.allocate(4);
            MemorySegment tooSmall = arena.allocate(3);

            try {
                memcmp.invoke(instance, tooSmall, b, 3L);
                fail("Expected IndexOutOfBoundsException");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue("Expected IndexOutOfBoundsException, got: " + e.getCause(), e.getCause() instanceof IndexOutOfBoundsException);
            }

            MemorySegment a = arena.allocate(4);
            for (int i = 0; i < 3; i++) {
                a.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
                b.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) i);
            }
            int cmp = (int) memcmp.invoke(instance, a, b, 3L);
            assertEquals(0, cmp);
        }
    }
}
