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
                int fn(@VectorSegment(countParam = "length", elementBytes = 1) int length);
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
                int fn(@VectorSegment(countParam = "nope", elementBytes = 1) MemorySegment a, int length);
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
                int fn(@VectorSegment(countParam = "b", elementBytes = 1) MemorySegment a, MemorySegment b);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about countParam type but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@VectorSegment.countParam parameter [b] must be int or long"))
        );
    }

    public void testVectorSegmentNeitherElementSizeSetFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@VectorSegment(countParam = "length") MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about missing element size but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("requires exactly one of 'elementBytes' or 'elementBits'"))
        );
    }

    public void testVectorSegmentBothElementSizeSetFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@VectorSegment(countParam = "length", elementBytes = 1, elementBits = 4) MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about both element sizes set but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("requires exactly one of 'elementBytes' or 'elementBits'"))
        );
    }

    public void testVectorSegmentNegativeElementBytesFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@VectorSegment(countParam = "length", elementBytes = -1) MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about negative elementBytes but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("elementBytes on parameter [a] must be positive"))
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

    public void testVectorSegmentAlignedWithElementBitsFails() {
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
            "Expected error about aligned requiring elementBytes but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@VectorSegment.aligned") && msg.contains("requires 'elementBytes'"))
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
                int fn(@MatrixSegment(rowsParam = "count", colsParam = "length", elementBytes = 1) int count, int length);
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
                int fn(@MatrixSegment(rowsParam = "nope", colsParam = "length", elementBytes = 1) MemorySegment a, int length, int count);
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
                   @MatrixSegment(rowsParam = "b", colsParam = "length", elementBytes = 1) MemorySegment a,
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

    public void testMatrixSegmentUnknownRowPitchBytesParamFails() {
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
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBytes = 1, rowPitchBytesParam = "nope")
                    MemorySegment a,
                    int length, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about unknown rowPitchBytesParam but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@MatrixSegment.rowPitchBytesParam references unknown parameter [nope]"))
        );
    }

    public void testMatrixSegmentNeitherColsNorRowBytesSetFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@MatrixSegment(rowsParam = "count") MemorySegment a, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about missing colsParam/rowBytesParam but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("must set exactly one of 'colsParam' or 'rowBytesParam'"))
        );
    }

    public void testMatrixSegmentBothColsAndRowBytesSetFails() {
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
                    @MatrixSegment(rowsParam = "count", colsParam = "length", rowBytesParam = "rowBytes") MemorySegment a,
                    int length,
                    int count,
                    int rowBytes
                );
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about both colsParam and rowBytesParam set but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("must set exactly one of 'colsParam' or 'rowBytesParam'"))
        );
    }

    public void testMatrixSegmentRowBytesWithElementBytesFails() {
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
                    @MatrixSegment(rowsParam = "count", rowBytesParam = "rowBytes", elementBytes = 1) MemorySegment a,
                    int count, int rowBytes);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about rowBytesParam combined with elementBytes but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("cannot combine 'rowBytesParam' with 'elementBytes'/'elementBits'"))
        );
    }

    public void testMatrixSegmentRowBytesWithElementBitsFails() {
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
                    @MatrixSegment(rowsParam = "count", rowBytesParam = "rowBytes", elementBits = 4) MemorySegment a,
                    int count, int rowBytes);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about rowBytesParam combined with elementBits but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("cannot combine 'rowBytesParam' with 'elementBytes'/'elementBits'"))
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
                int fn(@MatrixSegment(rowsParam = "count", colsParam = "nope", elementBytes = 1) MemorySegment a, int count);
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
                    @MatrixSegment(rowsParam = "count", colsParam = "b", elementBytes = 1) MemorySegment a,
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

    public void testMatrixSegmentUnknownRowBytesParamFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@MatrixSegment(rowsParam = "count", rowBytesParam = "nope") MemorySegment a, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about unknown rowBytesParam but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@MatrixSegment.rowBytesParam references unknown parameter [nope]"))
        );
    }

    public void testMatrixSegmentColsNeitherElementSizeSetFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                int fn(@MatrixSegment(rowsParam = "count", colsParam = "length") MemorySegment a, int length, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about missing element size but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("requires exactly one of 'elementBytes' or 'elementBits'"))
        );
    }

    public void testMatrixSegmentColsBothElementSizeSetFails() {
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
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBytes = 1, elementBits = 4) MemorySegment a,
                    int length, int count);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about both element sizes set but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("requires exactly one of 'elementBytes' or 'elementBits'"))
        );
    }

    public void testMatrixSegmentAlignedWithRowBytesFails() {
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
                    @MatrixSegment(rowsParam = "count", rowBytesParam = "rowBytes", aligned = true) MemorySegment a,
                    int count, int rowBytes);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail", result.success());
        assertTrue(
            "Expected error about aligned requiring colsParam+elementBytes but got: " + result.errors(),
            result.errors()
                .stream()
                .anyMatch(msg -> msg.contains("@MatrixSegment.aligned") && msg.contains("requires 'colsParam' + 'elementBytes'"))
        );
    }

    public void testMatrixSegmentAlignedWithElementBitsFails() {
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
            "Expected error about aligned requiring elementBytes but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@MatrixSegment.aligned") && msg.contains("requires 'elementBytes'"))
        );
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
                    @VectorSegment(countParam = "length", elementBytes = 1)
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBytes = 1)
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
