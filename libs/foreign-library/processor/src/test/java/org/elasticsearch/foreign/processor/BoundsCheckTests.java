/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

/**
 * Tests that {@code @VectorSegment}/{@code @MatrixSegment}/{@code @SlicedSegment} (backed by
 * {@code BoundsCheckModel}) emit the correct diagnostics for invalid parameter combinations.
 * Positive/structural cases (where a correctly-shaped {@code $Impl} class is generated) live in
 * {@link ImplClassWriterTests}, the test suite for class codegen.
 */
public class BoundsCheckTests extends ProcessorTestCase {

    // -------------------------------------------------------------------------
    // @VectorSegment
    // -------------------------------------------------------------------------

    public void testVectorSegmentOnNonMemorySegmentParamFails() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@VectorSegment(countParam = "length", elementBits = 8) int length);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about non-MemorySegment parameter but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("can only be applied to a MemorySegment parameter"))
        );
    }

    public void testVectorSegmentUnknownCountParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@VectorSegment(countParam = "nope", elementBits = 8) MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about unknown countParam but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@VectorSegment.countParam references unknown parameter [nope]"))
        );
    }

    public void testVectorSegmentCountParamWrongTypeFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@VectorSegment(countParam = "b", elementBits = 8) MemorySegment a, MemorySegment b);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about countParam type but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@VectorSegment.countParam parameter [b] must be int or long"))
        );
    }

    public void testVectorSegmentZeroElementBitsFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@VectorSegment(countParam = "length", elementBits = 0) MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about non-positive elementBits but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("elementBits on parameter [a] must be positive"))
        );
    }

    public void testVectorSegmentNegativeElementBitsFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@VectorSegment(countParam = "length", elementBits = -4) MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about negative elementBits but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("elementBits on parameter [a] must be positive"))
        );
    }

    public void testVectorSegmentAlignedWithSubByteElementBitsFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@VectorSegment(countParam = "length", elementBits = 4, aligned = true) MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about aligned requiring whole-byte elementBits but got: " + result.errors(),
            result.errors()
                .stream()
                .anyMatch(msg -> msg.contains("@VectorSegment.aligned") && msg.contains("requires 'elementBits' to be a multiple of 8"))
        );
    }

    // -------------------------------------------------------------------------
    // @MatrixSegment
    // -------------------------------------------------------------------------

    public void testMatrixSegmentOnNonMemorySegmentParamFails() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = 8) int count, int length);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about non-MemorySegment parameter but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("can only be applied to a MemorySegment parameter"))
        );
    }

    public void testMatrixSegmentUnknownRowsParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@MatrixSegment(rowsParam = "nope", colsParam = "length", elementBits = 8) MemorySegment a, int length, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about unknown rowsParam but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@MatrixSegment.rowsParam references unknown parameter [nope]"))
        );
    }

    public void testMatrixSegmentRowsParamWrongTypeFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(
                   @MatrixSegment(rowsParam = "b", colsParam = "length", elementBits = 8) MemorySegment a,
                   MemorySegment b,
                   int length
                );
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about rowsParam type but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@MatrixSegment.rowsParam parameter [b] must be int or long"))
        );
    }

    public void testMatrixSegmentUnknownColsParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@MatrixSegment(rowsParam = "count", colsParam = "nope", elementBits = 8) MemorySegment a, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about unknown colsParam but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@MatrixSegment.colsParam references unknown parameter [nope]"))
        );
    }

    public void testMatrixSegmentColsParamWrongTypeFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(
                    @MatrixSegment(rowsParam = "count", colsParam = "b", elementBits = 8) MemorySegment a,
                    MemorySegment b, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about colsParam type but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@MatrixSegment.colsParam parameter [b] must be int or long"))
        );
    }

    public void testMatrixSegmentUnknownPaddingBytesParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = 8, paddingBytesParam = "nope")
                    MemorySegment a,
                    int length, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about unknown paddingBytesParam but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@MatrixSegment.paddingBytesParam references unknown parameter [nope]"))
        );
    }

    public void testMatrixSegmentPaddingBytesParamWrongTypeFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = 8, paddingBytesParam = "b")
                    MemorySegment a,
                    MemorySegment b, int length, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about paddingBytesParam type but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@MatrixSegment.paddingBytesParam parameter [b] must be int or long"))
        );
    }

    public void testMatrixSegmentZeroElementBitsFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = 0) MemorySegment a, int length, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about non-positive elementBits but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("elementBits on parameter [a] must be positive"))
        );
    }

    public void testMatrixSegmentNegativeElementBitsFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = -4) MemorySegment a,
                    int length, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about negative elementBits but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("elementBits on parameter [a] must be positive"))
        );
    }

    public void testMatrixSegmentAlignedWithSubByteElementBitsFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = 4, aligned = true) MemorySegment a,
                    int length, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about aligned requiring whole-byte elementBits but got: " + result.errors(),
            result.errors()
                .stream()
                .anyMatch(msg -> msg.contains("@MatrixSegment.aligned") && msg.contains("requires 'elementBits' to be a multiple of 8"))
        );
    }

    // -------------------------------------------------------------------------
    // @SlicedSegment
    // -------------------------------------------------------------------------

    public void testSlicedSegmentOnNonMemorySegmentParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SlicedSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@SlicedSegment(offsetParam = "offset", sizeParam = "size") int offset, int size);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about non-MemorySegment parameter but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("can only be applied to a MemorySegment parameter"))
        );
    }

    public void testSlicedSegmentUnknownOffsetParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SlicedSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@SlicedSegment(offsetParam = "nope", sizeParam = "size") MemorySegment buf, int offset, int size);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about unknown offsetParam but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@SlicedSegment.offsetParam references unknown parameter [nope]"))
        );
    }

    public void testSlicedSegmentUnknownSizeParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SlicedSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@SlicedSegment(offsetParam = "offset", sizeParam = "nope") MemorySegment buf, int offset, int size);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about unknown sizeParam but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@SlicedSegment.sizeParam references unknown parameter [nope]"))
        );
    }

    public void testSlicedSegmentOffsetParamWrongTypeFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SlicedSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@SlicedSegment(offsetParam = "offset", sizeParam = "size") MemorySegment buf, MemorySegment offset, int size);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about offsetParam type but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@SlicedSegment.offsetParam parameter [offset] must be int or long"))
        );
    }

    public void testSlicedSegmentSizeParamWrongTypeFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SlicedSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@SlicedSegment(offsetParam = "offset", sizeParam = "size") MemorySegment buf, int offset, MemorySegment size);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about sizeParam type but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@SlicedSegment.sizeParam parameter [size] must be int or long"))
        );
    }

    public void testSlicedSegmentValidCompiles() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SlicedSegment;
            @LibrarySpecification(name = "testlib")
            public interface GoodLib {
                @Function("native_fn")
                int fn(@SlicedSegment(offsetParam = "offset", sizeParam = "size") MemorySegment buf, int offset, int size);
            }
            """;

        CompilationResult result = compile("test.GoodLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
    }

    public void testSlicedSegmentValidCompilesWithLongParams() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SlicedSegment;
            @LibrarySpecification(name = "testlib")
            public interface GoodLib {
                @Function("native_fn")
                int fn(@SlicedSegment(offsetParam = "offset", sizeParam = "size") MemorySegment buf, long offset, long size);
            }
            """;

        CompilationResult result = compile("test.GoodLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
    }

    public void testSlicedSegmentValidCompilesWithMixedIntLongParams() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SlicedSegment;
            @LibrarySpecification(name = "testlib")
            public interface GoodLib {
                @Function("native_fn")
                int fn(@SlicedSegment(offsetParam = "offset", sizeParam = "size") MemorySegment buf, int offset, long size);
            }
            """;

        CompilationResult result = compile("test.GoodLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
    }

    // -------------------------------------------------------------------------
    // Shared / cross-cutting
    // -------------------------------------------------------------------------

    public void testBothVectorAndMatrixSegmentOnSameParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(
                    @VectorSegment(countParam = "length", elementBits = 8)
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = 8)
                    MemorySegment a,
                    int length, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about combining multiple bounds-check annotations but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("cannot combine multiple bounds-check annotations"))
        );
    }

    /**
     * A hypothetical future bounds-check annotation, meta-annotated with {@code @BoundsCheck} but not
     * wired into {@code BoundsCheckModel.resolve}, must fail cleanly with a diagnostic rather than
     * crash the processor (e.g. with a {@code NullPointerException} from an unhandled annotation type).
     */
    public void testUnknownBoundsCheckAnnotationTypeFails() {
        String source = """
            package test;
            import java.lang.annotation.ElementType;
            import java.lang.annotation.Retention;
            import java.lang.annotation.RetentionPolicy;
            import java.lang.annotation.Target;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.BoundsCheck;
            @Retention(RetentionPolicy.SOURCE)
            @Target(ElementType.PARAMETER)
            @BoundsCheck
            @interface FutureSegment {}
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@FutureSegment MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about unknown bounds-check annotation type but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("Unknown bounds-check annotation type"))
        );
    }
}
