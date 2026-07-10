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
import java.lang.invoke.MethodHandle;

/**
 * Tests that {@link ImplClassWriter} generates correct {@code $Impl} class files.
 */
@SuppressForbidden(reason = "tests verify private fields of processor-generated classes; getDeclaredField is the only way to access them")
public class ImplClassWriterTests extends ProcessorTestCase {

    /**
     * A valid @LibrarySpecification interface with a single {@code int}-returning @Function method.
     * The processor must emit no errors and generate a $Impl class file with a
     * {@code private static final MethodHandle add$mh} field.
     */
    public void testValidLibraryGeneratesClass() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("native_add")
                int add(int a, int b);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // The generated class file must be loadable without initializing it (no native libs present)
        Class<?> implClass = result.loadClassNoInit("test.MyLib$Impl");
        assertNotNull("Generated MyLib$Impl class not found", implClass);

        // Must be package-private and final
        assertFalse("impl class must not be public", java.lang.reflect.Modifier.isPublic(implClass.getModifiers()));
        assertTrue("impl class must be final", java.lang.reflect.Modifier.isFinal(implClass.getModifiers()));

        // Must implement the interface
        assertEquals("test.MyLib", implClass.getInterfaces()[0].getName());

        // Must have a MethodHandle field named add$mh
        java.lang.reflect.Field mhField = implClass.getDeclaredField("add$mh");
        assertEquals("add$mh must be a MethodHandle", MethodHandle.class, mhField.getType());
        assertTrue("add$mh must be static", java.lang.reflect.Modifier.isStatic(mhField.getModifiers()));
        assertTrue("add$mh must be private", java.lang.reflect.Modifier.isPrivate(mhField.getModifiers()));
        assertTrue("add$mh must be final", java.lang.reflect.Modifier.isFinal(mhField.getModifiers()));
    }

    /**
     * Verifies that a MemorySegment return type is handled correctly in the generated class.
     */
    public void testMemorySegmentReturnType() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib")
            public interface PtrLib {
                @Function("get_ptr")
                MemorySegment getPtr(long size);
            }
            """;

        CompilationResult result = compile("test.PtrLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.PtrLib$Impl");
        assertNotNull("Generated PtrLib$Impl class not found", implClass);

        java.lang.reflect.Field mhField = implClass.getDeclaredField("getPtr$mh");
        assertEquals("getPtr$mh must be a MethodHandle", MethodHandle.class, mhField.getType());
    }

    /**
     * Verifies that a void return type is handled correctly.
     */
    public void testVoidReturnType() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib")
            public interface VoidLib {
                @Function("do_work")
                void doWork(int count);
            }
            """;

        CompilationResult result = compile("test.VoidLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.VoidLib$Impl");
        assertNotNull("Generated VoidLib$Impl class not found", implClass);
        java.lang.reflect.Field mhField = implClass.getDeclaredField("doWork$mh");
        assertEquals("doWork$mh must be a MethodHandle", MethodHandle.class, mhField.getType());
    }

    /**
     * An interface with a {@code String}-returning {@code @Function} method must generate
     * a class whose method body calls {@code reinterpret(Long.MAX_VALUE).getString(0)}.
     * We verify this structurally: the generated class must have a {@code getErrorName$mh} field
     * and the method must have return type {@code String} (not {@code MemorySegment}).
     */
    public void testStringReturnGeneratesClass() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib")
            public interface StringReturnLib {
                @Function("get_error_name")
                String getErrorName(long code);
            }
            """;

        CompilationResult result = compile("test.StringReturnLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.StringReturnLib$Impl");
        assertNotNull("Generated StringReturnLib$Impl class not found", implClass);

        // The $mh field must exist
        java.lang.reflect.Field mhField = implClass.getDeclaredField("getErrorName$mh");
        assertEquals("getErrorName$mh must be a MethodHandle", java.lang.invoke.MethodHandle.class, mhField.getType());

        // The generated method must have return type String
        java.lang.reflect.Method method = implClass.getMethod("getErrorName", long.class);
        assertEquals("getErrorName must return String", String.class, method.getReturnType());
    }

    /**
     * Verifies that a {@code String} parameter is accepted and generates a class whose method
     * takes a {@code String} on the Java side. The generated method body must open a confined
     * {@code Arena}, allocate the String into native memory via
     * {@code MemorySegmentUtil.allocateString}, pass the resulting {@code MemorySegment} to
     * {@code invokeExact}, and close the arena in both normal and exceptional paths.
     *
     * <p>We verify structurally: the generated class must have a {@code sandbox_init$mh} field
     * and the method must accept a {@code String} parameter (not {@code MemorySegment}).
     */
    public void testStringParamGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification
            public interface SandboxLib {
                @Function("sandbox_init")
                int sandboxInit(String profile, long flags, MemorySegment errorbuf);
            }
            """;

        CompilationResult result = compile("test.SandboxLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.SandboxLib$Impl");
        assertNotNull("Generated SandboxLib$Impl class not found", implClass);

        // The $mh field must exist
        java.lang.reflect.Field mhField = implClass.getDeclaredField("sandboxInit$mh");
        assertEquals("sandboxInit$mh must be a MethodHandle", java.lang.invoke.MethodHandle.class, mhField.getType());

        // The generated method must accept String, long, MemorySegment (not MemorySegment, long, MemorySegment)
        java.lang.reflect.Method method = implClass.getMethod("sandboxInit", String.class, long.class, MemorySegment.class);
        assertEquals("sandboxInit must return int", int.class, method.getReturnType());
        assertEquals("first param must be String", String.class, method.getParameterTypes()[0]);
    }

    // -------------------------------------------------------------------------
    // @VectorSegment / @MatrixSegment — valid usage generates the correct class shape.
    // Note: these tests confirm compilation succeeds and the generated class/method shape is
    // correct, not that the emitted checks fire at runtime.
    // -------------------------------------------------------------------------

    public void testVectorSegmentElementBytesGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("dot_product")
                int dotProduct(
                    @VectorSegment(countParam = "length", elementBytes = 1) MemorySegment a,
                    @VectorSegment(countParam = "length", elementBytes = 1) MemorySegment b,
                    int length);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        Class<?> implClass = result.loadClassNoInit("test.MyLib$Impl");
        assertNotNull("Generated MyLib$Impl class not found", implClass);

        java.lang.reflect.Method method = implClass.getMethod("dotProduct", MemorySegment.class, MemorySegment.class, int.class);
        assertEquals("dotProduct must still return int", int.class, method.getReturnType());
    }

    public void testVectorSegmentElementBitsGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("dot_product_i4")
                int dotProductI4(
                    @VectorSegment(countParam = "elementCount", elementBits = 4) MemorySegment a,
                    @VectorSegment(countParam = "elementCount", elementBytes = 1) MemorySegment b,
                    int elementCount);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertNotNull("Generated MyLib$Impl class not found", result.loadClassNoInit("test.MyLib$Impl"));
    }

    public void testVectorSegmentAlignedGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("dot_product_sparse")
                int dotProductSparse(
                    @VectorSegment(countParam = "count", elementBytes = 8, aligned = true) MemorySegment addresses,
                    int count);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        Class<?> implClass = result.loadClassNoInit("test.MyLib$Impl");
        assertNotNull("Generated MyLib$Impl class not found", implClass);
        assertAssertionsDisabledFieldPresent(implClass);
    }

    public void testMatrixSegmentColsElementBytesGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("dot_product_bulk")
                void dotProductBulk(
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBytes = 1) MemorySegment docs,
                    @VectorSegment(countParam = "length", elementBytes = 1) MemorySegment query,
                    int length, int count,
                    @VectorSegment(countParam = "count", elementBytes = 4) MemorySegment scores);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        Class<?> implClass = result.loadClassNoInit("test.MyLib$Impl");
        assertNotNull("Generated MyLib$Impl class not found", implClass);

        java.lang.reflect.Method method = implClass.getMethod(
            "dotProductBulk",
            MemorySegment.class,
            MemorySegment.class,
            int.class,
            int.class,
            MemorySegment.class
        );
        assertEquals("dotProductBulk must still return void", void.class, method.getReturnType());
    }

    public void testMatrixSegmentColsElementBitsGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("dot_product_i4_bulk")
                void dotProductI4Bulk(
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = 4) MemorySegment docs,
                    @VectorSegment(countParam = "length", elementBytes = 1) MemorySegment query,
                    int length, int count,
                    @VectorSegment(countParam = "count", elementBytes = 4) MemorySegment scores);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertNotNull("Generated MyLib$Impl class not found", result.loadClassNoInit("test.MyLib$Impl"));
    }

    public void testMatrixSegmentRowBytesGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("dot_product_bbq_bulk")
                void dotProductBBQBulk(
                    @MatrixSegment(rowsParam = "count", rowBytesParam = "datasetVectorLengthInBytes") MemorySegment docs,
                    int datasetVectorLengthInBytes,
                    @VectorSegment(countParam = "datasetVectorLengthInBytes", elementBytes = 1) MemorySegment query,
                    int count,
                    @VectorSegment(countParam = "count", elementBytes = 4) MemorySegment scores);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertNotNull("Generated MyLib$Impl class not found", result.loadClassNoInit("test.MyLib$Impl"));
    }

    public void testMatrixSegmentRowPitchBytesGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("dot_product_bulk_offsets")
                void dotProductBulkOffsets(
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBytes = 1, rowPitchBytesParam = "pitch")
                    MemorySegment docs,
                    @VectorSegment(countParam = "length", elementBytes = 1) MemorySegment query,
                    int length, int pitch,
                    @VectorSegment(countParam = "count", elementBytes = 4) MemorySegment offsets,
                    int count,
                    @VectorSegment(countParam = "count", elementBytes = 4) MemorySegment scores);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertNotNull("Generated MyLib$Impl class not found", result.loadClassNoInit("test.MyLib$Impl"));
    }

    /**
     * {@code rowPitchBytesParam} and {@code rowBytesParam} are NOT mutually exclusive — a matrix can
     * have both a directly-supplied packed row size and a separate, larger stride (e.g. BBQ's
     * {@code _bulk_offsets} path). This must compile and size via {@code rowPitchBytesParam}, not
     * {@code rowBytesParam}.
     */
    public void testMatrixSegmentRowPitchBytesWithRowBytesGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("dot_product_bbq_bulk_offsets")
                void dotProductBBQBulkOffsets(
                    @MatrixSegment(rowsParam = "count", rowBytesParam = "datasetVectorLengthInBytes", rowPitchBytesParam = "pitch")
                    MemorySegment docs,
                    int datasetVectorLengthInBytes, int pitch,
                    @VectorSegment(countParam = "datasetVectorLengthInBytes", elementBytes = 1) MemorySegment query,
                    @VectorSegment(countParam = "count", elementBytes = 4) MemorySegment offsets,
                    int count,
                    @VectorSegment(countParam = "count", elementBytes = 4) MemorySegment scores);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertNotNull("Generated MyLib$Impl class not found", result.loadClassNoInit("test.MyLib$Impl"));
    }

    public void testMatrixSegmentAlignedGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("read_matrix")
                void readMatrix(
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBytes = 4, aligned = true) MemorySegment m,
                    int length, int count);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        Class<?> implClass = result.loadClassNoInit("test.MyLib$Impl");
        assertNotNull("Generated MyLib$Impl class not found", implClass);
        assertAssertionsDisabledFieldPresent(implClass);
    }

    /**
     * The {@code $assertionsDisabled} field backing alignment asserts is emitted unconditionally on
     * every generated class, even when no parameter uses {@code aligned = true} — it's one boolean
     * field and a few one-time {@code <clinit>} instructions, not worth conditionally emitting.
     */
    public void testAssertionsDisabledFieldPresentEvenWithoutAlignedUsage() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("native_fn")
                int fn(MemorySegment a);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        Class<?> implClass = result.loadClassNoInit("test.MyLib$Impl");
        assertNotNull("Generated MyLib$Impl class not found", implClass);
        assertAssertionsDisabledFieldPresent(implClass);
    }

    private void assertAssertionsDisabledFieldPresent(Class<?> implClass) throws Exception {
        java.lang.reflect.Field field = implClass.getDeclaredField("$assertionsDisabled");
        assertEquals("$assertionsDisabled must be boolean", boolean.class, field.getType());
        assertTrue("$assertionsDisabled must be static", java.lang.reflect.Modifier.isStatic(field.getModifiers()));
        assertTrue("$assertionsDisabled must be private", java.lang.reflect.Modifier.isPrivate(field.getModifiers()));
        assertTrue("$assertionsDisabled must be final", java.lang.reflect.Modifier.isFinal(field.getModifiers()));
    }
}
