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
 * Tests that {@code @VectorSegment}/{@code @MatrixSegment} (backed by {@code BoundsCheckModel})
 * emit the correct diagnostics for invalid parameter combinations.
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
    // @OffsetSegment
    // -------------------------------------------------------------------------

    public void testOffsetSegmentOnNonMemorySegmentParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.OffsetSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@OffsetSegment(offset = "offset", length = "len") int offset, int len);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about non-MemorySegment parameter but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("can only be applied to a MemorySegment parameter"))
        );
    }

    public void testOffsetSegmentUnknownOffsetFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.OffsetSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@OffsetSegment(offset = "nope", length = "len") MemorySegment buf, int offset, int len);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about unknown offset but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@OffsetSegment.offset references unknown parameter [nope]"))
        );
    }

    public void testOffsetSegmentUnknownLengthFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.OffsetSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@OffsetSegment(offset = "offset", length = "nope") MemorySegment buf, int offset, int len);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about unknown length but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@OffsetSegment.length references unknown parameter [nope]"))
        );
    }

    public void testOffsetSegmentOffsetWrongTypeFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.OffsetSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@OffsetSegment(offset = "offset", length = "len") MemorySegment buf, MemorySegment offset, int len);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about offset type but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@OffsetSegment.offset parameter [offset] must be int or long"))
        );
    }

    public void testOffsetSegmentNegativePaddingBytesFails() {
        String source =
            """
                package test;
                import java.lang.foreign.MemorySegment;
                import org.elasticsearch.foreign.LibrarySpecification;
                import org.elasticsearch.foreign.Function;
                import org.elasticsearch.foreign.OffsetSegment;
                @LibrarySpecification(name = "testlib")
                public interface BadLib {
                    @Function("native_fn")
                    int fn(@OffsetSegment(offset = "offset", length = "len", paddingBytes = -1) MemorySegment buf, int offset, int len);
                }
                """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about negative paddingBytes but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("paddingBytes") && msg.contains("must be non-negative"))
        );
    }

    public void testOffsetSegmentValidCompiles() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.OffsetSegment;
            @LibrarySpecification(name = "testlib")
            public interface GoodLib {
                @Function("native_fn")
                int fn(@OffsetSegment(offset = "offset", length = "len") MemorySegment buf, int offset, int len);
            }
            """;

        CompilationResult result = compile("test.GoodLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
    }

    public void testOffsetSegmentWithPaddingCompiles() {
        String source =
            """
                package test;
                import java.lang.foreign.MemorySegment;
                import org.elasticsearch.foreign.LibrarySpecification;
                import org.elasticsearch.foreign.Function;
                import org.elasticsearch.foreign.OffsetSegment;
                @LibrarySpecification(name = "testlib")
                public interface GoodLib {
                    @Function("native_fn")
                    int fn(@OffsetSegment(offset = "offset", length = "len", paddingBytes = 64) MemorySegment buf, int offset, int len);
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
