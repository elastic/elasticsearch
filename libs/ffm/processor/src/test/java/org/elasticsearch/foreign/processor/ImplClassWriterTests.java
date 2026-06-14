/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import java.lang.invoke.MethodHandle;

/**
 * Tests that {@link ImplClassWriter} generates correct {@code $Impl} class files.
 */
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
     * An interface with a {@code @CaptureErrno} method must generate a {@code $Impl} class that
     * has a {@code private static final MemorySegment errnoState} field.
     */
    public void testCaptureErrnoGeneratesErrnoStateField() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.CaptureErrno;
            @LibrarySpecification(name = "testlib")
            public interface ErrnoLib {
                @Function("errno_op")
                @CaptureErrno
                int errnoOp(int x);
            }
            """;

        CompilationResult result = compile("test.ErrnoLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.ErrnoLib$Impl");
        assertNotNull("Generated ErrnoLib$Impl class not found", implClass);

        java.lang.reflect.Field errnoState = implClass.getDeclaredField("errnoState");
        assertEquals("errnoState must be MemorySegment", "java.lang.foreign.MemorySegment", errnoState.getType().getName());
        assertTrue("errnoState must be static", java.lang.reflect.Modifier.isStatic(errnoState.getModifiers()));
        assertTrue("errnoState must be private", java.lang.reflect.Modifier.isPrivate(errnoState.getModifiers()));
        assertTrue("errnoState must be final", java.lang.reflect.Modifier.isFinal(errnoState.getModifiers()));
    }

    /**
     * An interface with a {@code @SymbolResolverClass}-annotated method must generate a class whose
     * {@code $mh} field initialization uses the resolver (the resolver class must be accessible
     * at clinit time; here we use {@link TestSymbolResolver}).
     */
    public void testSymbolResolverGeneratesClass() throws Exception {
        String resolverFqn = TestSymbolResolver.class.getName();
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.SymbolResolverClass;
            import %s;
            @LibrarySpecification(name = "testlib")
            public interface ResolverLib {
                @Function("base_symbol")
                @SymbolResolverClass(TestSymbolResolver.class)
                int resolvedOp(int x);
            }
            """.formatted(resolverFqn);

        CompilationResult result = compile("test.ResolverLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.ResolverLib$Impl");
        assertNotNull("Generated ResolverLib$Impl class not found", implClass);

        // The $mh field must exist; the resolver would be invoked in clinit, so we just verify structure
        java.lang.reflect.Field mhField = implClass.getDeclaredField("resolvedOp$mh");
        assertEquals("resolvedOp$mh must be a MethodHandle", java.lang.invoke.MethodHandle.class, mhField.getType());
        assertTrue("resolvedOp$mh must be static", java.lang.reflect.Modifier.isStatic(mhField.getModifiers()));
        assertTrue("resolvedOp$mh must be private", java.lang.reflect.Modifier.isPrivate(mhField.getModifiers()));
        assertTrue("resolvedOp$mh must be final", java.lang.reflect.Modifier.isFinal(mhField.getModifiers()));
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
        java.lang.reflect.Method method = implClass.getDeclaredMethod("getErrorName", long.class);
        assertEquals("getErrorName must return String", String.class, method.getReturnType());
    }

    /**
     * A {@code @FunctionPointer}-typed parameter must generate an upcall stub inside the method body
     * and pass the resulting {@code MemorySegment} to {@code invokeExact} rather than the raw callback.
     * Verifies structurally: the generated class loads and has the expected {@code $mh} field.
     */
    public void testFunctionPointerMethodGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.Arena;
            import org.elasticsearch.foreign.FunctionPointer;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            @LibrarySpecification(name = "testlib")
            public interface CbLib {
                @FunctionPointer
                interface OnEvent {
                    void onEvent(int code);
                }
                @Function("register_callback")
                void registerCallback(OnEvent callback, Arena arena);
            }
            """;

        CompilationResult result = compile("test.CbLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.CbLib$Impl");
        assertNotNull("Generated CbLib$Impl class not found", implClass);

        // Must have a MethodHandle field for the native function
        java.lang.reflect.Field mhField = implClass.getDeclaredField("registerCallback$mh");
        assertEquals("registerCallback$mh must be a MethodHandle", MethodHandle.class, mhField.getType());
        assertTrue("registerCallback$mh must be static", java.lang.reflect.Modifier.isStatic(mhField.getModifiers()));
        assertTrue("registerCallback$mh must be private", java.lang.reflect.Modifier.isPrivate(mhField.getModifiers()));
        assertTrue("registerCallback$mh must be final", java.lang.reflect.Modifier.isFinal(mhField.getModifiers()));

        // Verify OnEvent interface exists in the generated output
        assertNotNull("CbLib$OnEvent interface must exist", result.loadClassNoInit("test.CbLib$OnEvent"));

        // Look up the method by name; getDeclaredMethod with class objects would fail because
        // CbLib$OnEvent was loaded by a different URLClassLoader instance than implClass.
        java.lang.reflect.Method method = null;
        for (java.lang.reflect.Method m : implClass.getDeclaredMethods()) {
            if ("registerCallback".equals(m.getName())) {
                method = m;
                break;
            }
        }
        assertNotNull("registerCallback method must exist on CbLib$Impl", method);
        assertEquals("registerCallback must return void", void.class, method.getReturnType());
    }
}
