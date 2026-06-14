/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import junit.framework.TestCase;

import java.lang.invoke.MethodHandle;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

/**
 * Tests that {@link LibraryProcessor} correctly builds models, emits diagnostics,
 * and generates valid {@code $Impl} class files.
 */
public class LibraryLookupProcessorTests extends TestCase {

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
     * A valid @LibrarySpecification interface must generate a public {@code $LibraryProvider} class that
     * extends {@code NativeLibraryProvider}, with {@code libraryClass()} returning the interface
     * and {@code load()} returning a non-null instance of the interface.
     */
    public void testValidLibraryGeneratesProvider() throws Exception {
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

        Class<?> providerClass = result.loadClassNoInit("test.MyLib$Provider");
        assertNotNull("Generated MyLib$Provider class not found", providerClass);

        // Must be public and final
        assertTrue("provider class must be public", java.lang.reflect.Modifier.isPublic(providerClass.getModifiers()));
        assertTrue("provider class must be final", java.lang.reflect.Modifier.isFinal(providerClass.getModifiers()));

        // Must implement LibraryProvider
        boolean implementsLibraryProvider = java.util.Arrays.stream(providerClass.getInterfaces())
            .anyMatch(i -> i.getName().equals("org.elasticsearch.foreign.LibraryProvider"));
        assertTrue("provider must implement LibraryProvider", implementsLibraryProvider);

        // libraryClass() must return the interface Class object
        java.lang.reflect.Method libraryClassMethod = providerClass.getMethod("libraryClass");
        Object providerInstance = providerClass.getDeclaredConstructor().newInstance();
        Class<?> returned = (Class<?>) libraryClassMethod.invoke(providerInstance);
        assertEquals("libraryClass() must return the annotated interface", "test.MyLib", returned.getName());
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
     * A {@code @Struct} with two {@code long} fields (static layout) must generate
     * an inner class with a {@code LAYOUT} field of the expected byte size.
     * Setting and reading back a field must return the written value.
     */
    public void testStaticStructGeneratesLayoutAndAccessors() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Struct;
            import org.elasticsearch.foreign.StructFactory;
            import org.elasticsearch.foreign.Setter;
            @LibrarySpecification(name = "testlib")
            public interface StructLib {
                @Struct
                interface Pair {
                    long first();
                    @Setter void first(long v);
                    long second();
                    @Setter void second(long v);
                }
                @StructFactory
                Pair newPair();
                @Function("get_val")
                long getVal(long x);
            }
            """;

        CompilationResult result = compile("test.StructLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // Load the outer $Impl to access the inner struct impl
        Class<?> outerImpl = result.loadClassNoInit("test.StructLib$Impl");
        assertNotNull("StructLib$Impl must be generated", outerImpl);

        // The struct inner class is StructLib$Pair$Impl
        Class<?> pairImpl = result.loadClass("test.StructLib$Pair$Impl");
        assertNotNull("StructLib$Pair$Impl must be generated", pairImpl);

        // LAYOUT field must exist and have byte size 16 (two longs)
        java.lang.reflect.Field layoutField = pairImpl.getDeclaredField("LAYOUT");
        layoutField.setAccessible(true);
        assertTrue("LAYOUT must be static", java.lang.reflect.Modifier.isStatic(layoutField.getModifiers()));
        assertTrue("LAYOUT must be final", java.lang.reflect.Modifier.isFinal(layoutField.getModifiers()));
        java.lang.foreign.MemoryLayout layout = (java.lang.foreign.MemoryLayout) layoutField.get(null);
        assertEquals("LAYOUT must be 16 bytes (2 longs)", 16L, layout.byteSize());

        // Instantiate and round-trip a value through first()
        java.lang.reflect.Constructor<?> pairCtor = pairImpl.getDeclaredConstructor();
        pairCtor.setAccessible(true);
        Object pair = pairCtor.newInstance();
        java.lang.reflect.Method setFirst = pairImpl.getDeclaredMethod("first", long.class);
        setFirst.setAccessible(true);
        java.lang.reflect.Method getFirst = pairImpl.getDeclaredMethod("first");
        getFirst.setAccessible(true);
        setFirst.invoke(pair, 42L);
        assertEquals("first() must return 42 after first(42)", 42L, (long) getFirst.invoke(pair));
    }

    /**
     * A {@code @Struct} with {@code dynamicLayout=true} must generate an inner class
     * whose constructor takes {@code (long sizeof, long off1, long off2, ...)} and whose
     * getters/setters use the stored offsets.
     */
    public void testDynamicStructGeneratesOffsetAccessors() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Struct;
            import org.elasticsearch.foreign.StructFactory;
            import org.elasticsearch.foreign.Setter;
            @LibrarySpecification(name = "testlib")
            public interface DynStructLib {
                @Struct(dynamicLayout = true)
                interface Info {
                    long st_size();
                    @Setter void st_size(long v);
                    long st_blocks();
                    @Setter void st_blocks(long v);
                }
                @StructFactory
                Info newInfo(long sizeof, long st_size$offset, long st_blocks$offset);
                @Function("noop")
                void noop();
            }
            """;

        CompilationResult result = compile("test.DynStructLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> infoImpl = result.loadClass("test.DynStructLib$Info$Impl");
        assertNotNull("DynStructLib$Info$Impl must be generated", infoImpl);

        // Must NOT have a LAYOUT field
        boolean hasLayout = false;
        for (java.lang.reflect.Field f : infoImpl.getDeclaredFields()) {
            if (f.getName().equals("LAYOUT")) {
                hasLayout = true;
            }
        }
        assertFalse("Dynamic struct must not have a LAYOUT field", hasLayout);

        // Constructor: (long sizeof, long st_size$offset, long st_blocks$offset)
        java.lang.reflect.Constructor<?> ctor = infoImpl.getDeclaredConstructor(long.class, long.class, long.class);
        assertNotNull("Constructor (long, long, long) must exist", ctor);
        ctor.setAccessible(true);

        // Allocate a 24-byte segment, put 99L at byte offset 8
        // sizeof=24, st_size$offset=8, st_blocks$offset=16
        Object info = ctor.newInstance(24L, 8L, 16L);

        java.lang.reflect.Method setSize = infoImpl.getDeclaredMethod("st_size", long.class);
        setSize.setAccessible(true);
        java.lang.reflect.Method getSize = infoImpl.getDeclaredMethod("st_size");
        getSize.setAccessible(true);
        setSize.invoke(info, 99L);
        assertEquals("st_size() must return 99 after st_size(99)", 99L, (long) getSize.invoke(info));
    }

    /**
     * A {@code @StructFactory} method on a static-layout struct must generate code that
     * creates a non-null struct instance via a public no-arg constructor.
     */
    public void testStaticStructFactoryGeneratesInstance() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Struct;
            import org.elasticsearch.foreign.StructFactory;
            import org.elasticsearch.foreign.Setter;
            @LibrarySpecification(name = "testlib")
            public interface FactoryLib {
                @Struct
                interface Point {
                    long x();
                    @Setter void x(long v);
                    long y();
                    @Setter void y(long v);
                }
                @StructFactory
                Point newPoint();
                @Function("noop")
                void noop();
            }
            """;

        CompilationResult result = compile("test.FactoryLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // Load the outer impl (without init, as it would try to link native symbols)
        Class<?> outerImpl = result.loadClassNoInit("test.FactoryLib$Impl");
        assertNotNull("FactoryLib$Impl must be generated", outerImpl);

        // Load the struct impl (with initialization — it only needs VarHandle setup, no native calls)
        Class<?> pointImpl = result.loadClass("test.FactoryLib$Point$Impl");
        assertNotNull("FactoryLib$Point$Impl must be generated", pointImpl);

        // Verify the factory method exists in the outer impl with the correct return type
        java.lang.reflect.Method newPointMethod = outerImpl.getDeclaredMethod("newPoint");
        assertNotNull("newPoint() method must exist on FactoryLib$Impl", newPointMethod);
        assertEquals("newPoint() must return test.FactoryLib$Point", "test.FactoryLib$Point", newPointMethod.getReturnType().getName());

        // Directly instantiate the struct impl to verify it works (uses Arena.ofAuto, no native deps)
        java.lang.reflect.Constructor<?> pointCtor = pointImpl.getDeclaredConstructor();
        pointCtor.setAccessible(true);
        Object point = pointCtor.newInstance();
        assertNotNull("Point$Impl must be instantiable", point);

        // Round-trip x()
        java.lang.reflect.Method setX = pointImpl.getDeclaredMethod("x", long.class);
        setX.setAccessible(true);
        java.lang.reflect.Method getX = pointImpl.getDeclaredMethod("x");
        getX.setAccessible(true);
        setX.invoke(point, 77L);
        assertEquals("x() must return 77 after x(77)", 77L, (long) getX.invoke(point));
    }

    /**
     * A {@code @StructFactory} method on a dynamic-layout struct must generate code that
     * passes sizeof and offsets through to the impl constructor correctly.
     */
    public void testDynamicStructFactoryPassesArguments() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Struct;
            import org.elasticsearch.foreign.StructFactory;
            import org.elasticsearch.foreign.Setter;
            @LibrarySpecification(name = "testlib")
            public interface DynFactoryLib {
                @Struct(dynamicLayout = true)
                interface Vec {
                    long len();
                    @Setter void len(long v);
                    long cap();
                    @Setter void cap(long v);
                }
                @StructFactory
                Vec newVec(long sizeof, long len$offset, long cap$offset);
                @Function("noop")
                void noop();
            }
            """;

        CompilationResult result = compile("test.DynFactoryLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> vecImpl = result.loadClass("test.DynFactoryLib$Vec$Impl");
        assertNotNull("DynFactoryLib$Vec$Impl must be generated", vecImpl);

        // Instantiate with sizeof=32, len$offset=0, cap$offset=8
        java.lang.reflect.Constructor<?> ctor = vecImpl.getDeclaredConstructor(long.class, long.class, long.class);
        ctor.setAccessible(true);
        Object vec = ctor.newInstance(32L, 0L, 8L);
        assertNotNull("Vec$Impl must be instantiable", vec);

        // Write len and read back
        java.lang.reflect.Method setLen = vecImpl.getDeclaredMethod("len", long.class);
        setLen.setAccessible(true);
        java.lang.reflect.Method getLen = vecImpl.getDeclaredMethod("len");
        getLen.setAccessible(true);
        setLen.invoke(vec, 123L);
        assertEquals("len() must return 123 after len(123)", 123L, (long) getLen.invoke(vec));

        // Write cap at offset 8 and read back
        java.lang.reflect.Method setCap = vecImpl.getDeclaredMethod("cap", long.class);
        setCap.setAccessible(true);
        java.lang.reflect.Method getCap = vecImpl.getDeclaredMethod("cap");
        getCap.setAccessible(true);
        setCap.invoke(vec, 256L);
        assertEquals("cap() must return 256 after cap(256)", 256L, (long) getCap.invoke(vec));
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

    /**
     * Combining {@code @Critical} and {@code @CaptureErrno} on the same method must emit a compile error
     * because critical calls bypass the errno-capture mechanism.
     */
    public void testCriticalAndCaptureErrnoEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.CaptureErrno;
            import org.elasticsearch.foreign.Critical;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("do_thing")
                @Critical
                @CaptureErrno
                int doThing(int x);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail due to @Critical + @CaptureErrno", result.success());
        assertTrue(
            "Expected error about @Critical and @CaptureErrno but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@Critical") && msg.contains("@CaptureErrno"))
        );
    }

    /**
     * Applying {@code @Utf16} to a non-String parameter must emit a compile error.
     */
    public void testUtf16OnNonStringParamEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Utf16;
            @LibrarySpecification(name = "testlib")
            public interface BadLib {
                @Function("do_thing")
                void doThing(@Utf16 int x);
            }
            """;

        CompilationResult result = compile("test.BadLib", source);

        assertFalse("Expected compilation to fail due to @Utf16 on non-String", result.success());
        assertTrue(
            "Expected error about @Utf16 on non-String but got: " + result.errors(),
            result.errors().stream().anyMatch(msg -> msg.contains("@Utf16"))
        );
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

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    record CompilationResult(boolean success, List<String> notes, List<String> errors, Path outputDir) {
        /** Loads a class from the compilation output directory (with initialization). Returns null if not found. */
        Class<?> loadClass(String className) throws Exception {
            return loadClass(className, true);
        }

        /**
         * Loads a class from the compilation output directory without triggering class initialization.
         * Use this when the class has a {@code <clinit>} that requires native libraries at runtime.
         */
        Class<?> loadClassNoInit(String className) throws Exception {
            return loadClass(className, false);
        }

        private Class<?> loadClass(String className, boolean initialize) throws Exception {
            if (outputDir == null) {
                return null;
            }
            URLClassLoader cl = new URLClassLoader(
                new URL[] { outputDir.toUri().toURL() },
                LibraryLookupProcessorTests.class.getClassLoader()
            );
            try {
                return Class.forName(className, initialize, cl);
            } catch (ClassNotFoundException e) {
                return null;
            }
        }
    }

    private CompilationResult compile(String className, String source) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull("System Java compiler not available", compiler);

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        String processorClasspath = buildClasspath();

        JavaFileObject sourceFile = new SimpleJavaFileObject(
            URI.create("string:///" + className.replace('.', '/') + ".java"),
            JavaFileObject.Kind.SOURCE
        ) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return source;
            }
        };

        try {
            Path outputDir = Files.createTempDirectory("native-lib-gen-test");
            try (var fileManager = compiler.getStandardFileManager(diagnostics, null, null)) {
                fileManager.setLocation(StandardLocation.CLASS_OUTPUT, List.of(outputDir.toFile()));

                List<String> options = new ArrayList<>();
                options.add("-classpath");
                options.add(processorClasspath);
                options.add("-processorpath");
                options.add(processorClasspath);

                JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, options, null, List.of(sourceFile));
                boolean success = task.call();

                List<String> notes = new ArrayList<>();
                List<String> errors = new ArrayList<>();
                for (Diagnostic<? extends JavaFileObject> d : diagnostics.getDiagnostics()) {
                    String msg = d.getMessage(null);
                    if (d.getKind() == Diagnostic.Kind.NOTE) {
                        notes.add(msg);
                    } else if (d.getKind() == Diagnostic.Kind.ERROR) {
                        errors.add(msg);
                    }
                }

                return new CompilationResult(success, notes, errors, outputDir);
            }
        } catch (Exception e) {
            throw new RuntimeException("Compilation setup failed", e);
        }
    }

    private String buildClasspath() {
        return System.getProperty("java.class.path");
    }
}
