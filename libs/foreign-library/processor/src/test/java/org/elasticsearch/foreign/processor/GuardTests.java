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

import java.lang.invoke.MethodHandle;

/**
 * Tests that the annotation processor correctly handles {@code @Guard} annotations:
 * parsing, validation, and direct checker invocation in the generated method body.
 */
@SuppressForbidden(reason = "tests verify private fields of processor-generated classes; getDeclaredField is the only way to access them")
public class GuardTests extends ProcessorTestCase {

    /**
     * A valid {@code @Guard}: the checker takes the same parameters as the native method.
     * Compilation succeeds and the {@code $Impl} class is generated.
     */
    public void testGuard() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Guard;
            class Checker {
                public static void check(MemorySegment a, MemorySegment b, int length) {}
            }
            @LibrarySpecification(name = "testlib")
            public interface GuardLib {
                @Function("native_fn")
                @Guard(checkerClass = Checker.class, checkerMethod = "check")
                int fn(MemorySegment a, MemorySegment b, int length);
            }
            """;

        CompilationResult result = compile("test.GuardLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.GuardLib$Impl");
        assertNotNull("Generated GuardLib$Impl class not found", implClass);

        java.lang.reflect.Field mhField = implClass.getDeclaredField("fn$mh");
        assertEquals("fn$mh must be a MethodHandle", MethodHandle.class, mhField.getType());
    }

    /**
     * A valid {@code @Guard} on a void-returning native method. Compilation succeeds.
     */
    public void testGuardVoidReturn() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Guard;
            class Checker {
                public static void check(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result) {}
            }
            @LibrarySpecification(name = "testlib")
            public interface GuardLib {
                @Function("native_fn")
                @Guard(checkerClass = Checker.class, checkerMethod = "check")
                void fn(MemorySegment a, MemorySegment b, int length, int count, MemorySegment result);
            }
            """;

        CompilationResult result = compile("test.GuardLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
    }

    /**
     * A valid {@code @Guard} where the native method has a {@code String} parameter.
     * The checker receives the Java {@code String}, not the marshaled {@code MemorySegment}.
     */
    public void testGuardWithStringParam() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Guard;
            class Checker {
                public static void check(String name, int length) {}
            }
            @LibrarySpecification(name = "testlib")
            public interface GuardLib {
                @Function("native_fn")
                @Guard(checkerClass = Checker.class, checkerMethod = "check")
                int fn(String name, int length);
            }
            """;

        CompilationResult result = compile("test.GuardLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
    }

    /**
     * The checker class has no method with the specified name. Compilation must fail.
     */
    public void testGuardMissingCheckerMethod() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Guard;
            class Checker {
                public static void otherMethod(MemorySegment a, int length) {}
            }
            @LibrarySpecification(name = "testlib")
            public interface GuardLib {
                @Function("native_fn")
                @Guard(checkerClass = Checker.class, checkerMethod = "check")
                int fn(MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.GuardLib", source);
        assertFalse("Expected compilation to fail when checker method is missing", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("has no public static method named 'check'"));
        assertTrue("Expected error about missing checker method but got: " + result.errors(), hasError);
    }

    /**
     * The checker method is not static. Compilation must fail.
     */
    public void testGuardCheckerNotStatic() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Guard;
            class Checker {
                public void check(MemorySegment a, int length) {}
            }
            @LibrarySpecification(name = "testlib")
            public interface GuardLib {
                @Function("native_fn")
                @Guard(checkerClass = Checker.class, checkerMethod = "check")
                int fn(MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.GuardLib", source);
        assertFalse("Expected compilation to fail when checker is not static", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("has no public static method named 'check'"));
        assertTrue("Expected error about missing public static checker method but got: " + result.errors(), hasError);
    }

    /**
     * The checker method is static but private. Compilation must fail.
     */
    public void testGuardCheckerNotPublic() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Guard;
            class Checker {
                private static void check(MemorySegment a, int length) {}
            }
            @LibrarySpecification(name = "testlib")
            public interface GuardLib {
                @Function("native_fn")
                @Guard(checkerClass = Checker.class, checkerMethod = "check")
                int fn(MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.GuardLib", source);
        assertFalse("Expected compilation to fail when checker is not public", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("has no public static method named 'check'"));
        assertTrue("Expected error about missing public static checker method but got: " + result.errors(), hasError);
    }

    /**
     * The checker method returns {@code int} instead of {@code void}. Compilation must fail.
     */
    public void testGuardCheckerWrongReturnType() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Guard;
            class Checker {
                public static int check(MemorySegment a, int length) {
                    return 1;
                }
            }
            @LibrarySpecification(name = "testlib")
            public interface GuardLib {
                @Function("native_fn")
                @Guard(checkerClass = Checker.class, checkerMethod = "check")
                int fn(MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.GuardLib", source);
        assertFalse("Expected compilation to fail when checker has wrong return type", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("must return void and accept"));
        assertTrue("Expected error about checker signature but got: " + result.errors(), hasError);
    }

    /**
     * The checker has the wrong number of parameters. Compilation must fail.
     */
    public void testGuardCheckerParamCountMismatch() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Guard;
            class Checker {
                public static void check(MemorySegment a) {}
            }
            @LibrarySpecification(name = "testlib")
            public interface GuardLib {
                @Function("native_fn")
                @Guard(checkerClass = Checker.class, checkerMethod = "check")
                int fn(MemorySegment a, int length);
            }
            """;

        CompilationResult result = compile("test.GuardLib", source);
        assertFalse("Expected compilation to fail when checker param count doesn't match", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("must return void and accept"));
        assertTrue("Expected error about checker signature but got: " + result.errors(), hasError);
    }

    /**
     * The checker's parameters don't match the native method's types. Compilation must fail.
     */
    public void testGuardCheckerNativeParamTypeMismatch() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Guard;
            class Checker {
                public static void check(int a, long wrongLength) {}
            }
            @LibrarySpecification(name = "testlib")
            public interface GuardLib {
                @Function("native_fn")
                @Guard(checkerClass = Checker.class, checkerMethod = "check")
                int fn(int a, int length);
            }
            """;

        CompilationResult result = compile("test.GuardLib", source);
        assertFalse("Expected compilation to fail when native param types don't match", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("must return void and accept"));
        assertTrue("Expected error about checker signature but got: " + result.errors(), hasError);
    }

    /**
     * {@code @Guard} on a method without {@code @Function}. Compilation must fail because
     * {@code @Function} is required on every abstract method of a {@code @LibrarySpecification}.
     */
    public void testGuardWithoutFunction() {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Guard;
            class Checker {
                public static void check(int x) {}
            }
            @LibrarySpecification(name = "testlib")
            public interface GuardLib {
                @Guard(checkerClass = Checker.class, checkerMethod = "check")
                int fn(int x);
            }
            """;

        CompilationResult result = compile("test.GuardLib", source);
        assertFalse("Expected compilation to fail when @Guard is used without @Function", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("must be annotated with @Function"));
        assertTrue("Expected error about missing @Function but got: " + result.errors(), hasError);
    }
}
