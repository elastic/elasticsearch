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
 * Tests that {@link LibraryProcessor} emits the correct diagnostics for invalid inputs.
 */
public class LibraryProcessorTests extends ProcessorTestCase {

    /**
     * A @LibrarySpecification interface with a method that has no annotation should emit a Kind.ERROR.
     */
    public void testUnannotatedMethodEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            @LibrarySpecification
            public interface BadLib {
                int unannotated();
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail due to unannotated method", result.success());
        boolean hasProcessorError = result.errors().stream().anyMatch(msg -> msg.contains("unannotated"));
        assertTrue("Expected an error about 'unannotated' method but got: " + result.errors(), hasProcessorError);
    }

    /**
     * {@code @Critical} requires a fallback adapter. Missing it is a javac error from the annotation type itself
     * (no default value).
     */
    public void testCriticalWithoutFallbackAdapterFails() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Critical;
            @LibrarySpecification(name = "testlib")
            public interface CritLib {
                @Function("native_fn")
                @Critical
                long fn(MemorySegment dst, long dstCap);
            }
            """;

        CompilationResult result = compile("test.CritLib", source);

        assertFalse("Expected compilation to fail when @Critical has no fallbackAdapter", result.success());
    }

    /**
     * The fallback adapter class must declare a public static method named the same as the wrapped method,
     * with leading {@code MethodHandle} parameter and matching original signature. Wrong name → error.
     */
    public void testFallbackAdapterMissingMethodEmitsError() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import java.lang.invoke.MethodHandle;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Critical;
            class WrongAdapter {
                public static long otherName(MethodHandle mh, MemorySegment dst, long dstCap) throws Throwable {
                    return (long) mh.invokeExact(dst, dstCap);
                }
            }
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                @Critical(fallbackAdapter = WrongAdapter.class)
                long fn(MemorySegment dst, long dstCap);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail when fallbackAdapter has no matching method", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("has no public static method named 'fn'"));
        assertTrue("Expected error about missing adapter method but got: " + result.errors(), hasError);
    }

    /**
     * The fallback adapter method must have the leading {@code MethodHandle} parameter followed by the
     * original signature. Wrong arity / types → error.
     */
    public void testFallbackAdapterSignatureMismatchEmitsError() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import java.lang.invoke.MethodHandle;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Critical;
            class BadAdapter {
                public static long fn(MethodHandle mh, MemorySegment dst) throws Throwable {
                    return (long) mh.invokeExact(dst);
                }
            }
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("native_fn")
                @Critical(fallbackAdapter = BadAdapter.class)
                long fn(MemorySegment dst, long dstCap);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail when fallbackAdapter signature mismatches", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("must have signature"));
        assertTrue("Expected error about adapter signature but got: " + result.errors(), hasError);
    }

    /**
     * Happy path: a correct fallback adapter compiles cleanly and the {@code $Impl} is generated.
     */
    public void testFallbackAdapterValid() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import java.lang.invoke.MethodHandle;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Critical;
            class GoodAdapter {
                public static long fn(MethodHandle mh, MemorySegment dst, long dstCap) throws Throwable {
                    return (long) mh.invokeExact(dst, dstCap);
                }
            }
            @LibrarySpecification(name = "testlib")
            public interface GoodLib {
                @Function("native_fn")
                @Critical(fallbackAdapter = GoodAdapter.class)
                long fn(MemorySegment dst, long dstCap);
            }
            """;

        CompilationResult result = compile("test.GoodLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertNotNull(result.loadClassNoInit("test.GoodLib$Impl"));
    }

    /**
     * A {@code @LibrarySpecification} annotation on a class (not an interface) should emit an error.
     */
    public void testAnnotationOnClassEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            @LibrarySpecification
            public class NotAnInterface {
            }
            """;

        CompilationResult result = compile("test.NotAnInterface", source);

        assertFalse("Expected compilation to fail", result.success());
        boolean hasProcessorError = result.errors().stream().anyMatch(msg -> msg.contains("@LibrarySpecification must be on an interface"));
        assertTrue("Expected error about @LibrarySpecification on non-interface but got: " + result.errors(), hasProcessorError);
    }
}
