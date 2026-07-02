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
 * Tests for the {@code @SymbolResolverClass} annotation support in the processor.
 */
@SuppressForbidden(reason = "tests verify private fields of processor-generated classes")
public class SymbolResolverClassTests extends ProcessorTestCase {

    /**
     * A valid resolver with the correct signature compiles cleanly and the $Impl class is generated.
     */
    public void testValidResolverCompiles() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.FunctionDescriptor;
            import java.lang.foreign.Linker;
            import java.lang.invoke.MethodHandle;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SymbolResolverClass;
            class MyResolver {
                public static MethodHandle resolve(String functionName, FunctionDescriptor descriptor, Linker.Option... options) {
                    return null;
                }
            }
            @LibrarySpecification(name = "testlib")
            @SymbolResolverClass(MyResolver.class)
            public interface MyLib {
                @Function("native_add")
                int add(int a, int b);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.MyLib$Impl");
        assertNotNull("Generated MyLib$Impl class not found", implClass);

        java.lang.reflect.Field mhField = implClass.getDeclaredField("add$mh");
        assertEquals("add$mh must be a MethodHandle", MethodHandle.class, mhField.getType());
    }

    /**
     * The resolver class must have a method named 'resolve'. Missing it is an error.
     */
    public void testResolverMissingResolveMethodEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SymbolResolverClass;
            class BadResolver {
                public static void doSomething() {}
            }
            @LibrarySpecification(name = "testlib")
            @SymbolResolverClass(BadResolver.class)
            public interface MyLib {
                @Function("native_add")
                int add(int a, int b);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertFalse("Expected compilation to fail when resolver has no 'resolve' method", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("has no public static method named 'resolve'"));
        assertTrue("Expected error about missing 'resolve' method but got: " + result.errors(), hasError);
    }

    /**
     * The resolve method must be public and static.
     */
    public void testResolverNonStaticMethodEmitsError() {
        String source = """
            package test;
            import java.lang.foreign.FunctionDescriptor;
            import java.lang.foreign.Linker;
            import java.lang.invoke.MethodHandle;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SymbolResolverClass;
            class BadResolver {
                public MethodHandle resolve(String functionName, FunctionDescriptor descriptor, Linker.Option... options) {
                    return null;
                }
            }
            @LibrarySpecification(name = "testlib")
            @SymbolResolverClass(BadResolver.class)
            public interface MyLib {
                @Function("native_add")
                int add(int a, int b);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertFalse("Expected compilation to fail when resolve is not static", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("has no public static method named 'resolve'"));
        assertTrue("Expected error about missing static 'resolve' method but got: " + result.errors(), hasError);
    }

    /**
     * The resolve method must have the correct parameter types. Wrong first param → error.
     */
    public void testResolverWrongParamTypesEmitsError() {
        String source = """
            package test;
            import java.lang.foreign.FunctionDescriptor;
            import java.lang.foreign.Linker;
            import java.lang.invoke.MethodHandle;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SymbolResolverClass;
            class BadResolver {
                public static MethodHandle resolve(int x, FunctionDescriptor descriptor, Linker.Option... options) {
                    return null;
                }
            }
            @LibrarySpecification(name = "testlib")
            @SymbolResolverClass(BadResolver.class)
            public interface MyLib {
                @Function("native_add")
                int add(int a, int b);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertFalse("Expected compilation to fail when resolve has wrong param types", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("must have signature"));
        assertTrue("Expected error about resolver signature but got: " + result.errors(), hasError);
    }

    /**
     * The resolve method must return MethodHandle, not void.
     */
    public void testResolverWrongReturnTypeEmitsError() {
        String source = """
            package test;
            import java.lang.foreign.FunctionDescriptor;
            import java.lang.foreign.Linker;
            import java.lang.invoke.MethodHandle;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SymbolResolverClass;
            class BadResolver {
                public static void resolve(String functionName, FunctionDescriptor descriptor, Linker.Option... options) {}
            }
            @LibrarySpecification(name = "testlib")
            @SymbolResolverClass(BadResolver.class)
            public interface MyLib {
                @Function("native_add")
                int add(int a, int b);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertFalse("Expected compilation to fail when resolve returns void", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("must have signature"));
        assertTrue("Expected error about resolver signature but got: " + result.errors(), hasError);
    }

    /**
     * The resolve method must be varargs. A fixed 3-param method without varargs → error.
     */
    public void testResolverNonVarargsEmitsError() {
        String source = """
            package test;
            import java.lang.foreign.FunctionDescriptor;
            import java.lang.foreign.Linker;
            import java.lang.invoke.MethodHandle;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SymbolResolverClass;
            class BadResolver {
                public static MethodHandle resolve(String functionName, FunctionDescriptor descriptor, Linker.Option[] options) {
                    return null;
                }
            }
            @LibrarySpecification(name = "testlib")
            @SymbolResolverClass(BadResolver.class)
            public interface MyLib {
                @Function("native_add")
                int add(int a, int b);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertFalse("Expected compilation to fail when resolve is not varargs", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("must have signature"));
        assertTrue("Expected error about resolver signature but got: " + result.errors(), hasError);
    }

    /**
     * Without @SymbolResolverClass, the generated code uses the default LinkerHelper.downcallHandle path.
     * This verifies that existing behavior is preserved (no resolver = standard path compiles).
     */
    public void testNoResolverUsesDefault() throws Exception {
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
        assertNotNull(result.loadClassNoInit("test.MyLib$Impl"));
    }
}
