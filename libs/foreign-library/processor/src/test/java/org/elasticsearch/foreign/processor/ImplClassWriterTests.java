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

import java.lang.classfile.ClassFile;
import java.lang.classfile.Opcode;
import java.lang.classfile.instruction.BranchInstruction;
import java.lang.classfile.instruction.InvokeInstruction;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Tests that {@link ImplClassWriter} generates correct {@code $Impl} class files. This is the suite for
 * positive codegen: both structural assertions on the emitted class/method shape and behavioral tests
 * that actually load the generated implementation (through the {@code LibraryProvider} SPI, via
 * {@link #loadLibrary}) and invoke its methods end to end. Compile-error diagnostics for invalid inputs
 * live in the per-feature suites such as {@link BoundsCheckTests} and {@link LibraryProcessorTests}.
 *
 * <p>The behavioral tests need no native library build dependency: the {@code @Upcall} codegen is
 * exercised both cross-platform (the generated stub is driven through a custom {@code MethodHandleResolver},
 * no external symbol required) and end to end against POSIX {@code qsort} where it is reachable through
 * the default linker lookup.
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
     * {@code Arena.allocateFrom}, pass the resulting {@code MemorySegment} to
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

    /**
     * A {@code @CaptureSystemError @Function} method on a POSIX-targeting library (errno channel) must
     * generate a class WITHOUT a per-class {@code systemErrorState} field — the shared
     * {@code LinkerHelper.SYSTEM_ERROR_STATE} is used instead. Also initializes the class to exercise the
     * emitted {@code Linker.Option.captureCallState} (and {@code firstVariadicArg} in the
     * {@code @Variadic} case) bytecode against the real FFM API, catching descriptor mismatches that
     * {@code loadClassNoInit} would miss.
     *
     * <p>The custom {@link org.elasticsearch.foreign.SymbolResolver} returns a fake non-null
     * address so {@code linker.downcallHandle} succeeds at class-init time without needing a
     * real native symbol on the classpath. Any linkage error from the descriptor construction
     * (e.g. {@code captureCallState} declared as varargs but emitted as a single {@code String})
     * still surfaces here because it fires before {@code downcallHandle} is even called.
     */
    public void testSystemErrorErrnoAndVariadicInitializeAgainstFfmApi() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.SymbolLookup;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Platform;
            import org.elasticsearch.foreign.ResolvedSymbol;
            import org.elasticsearch.foreign.SymbolResolver;
            import org.elasticsearch.foreign.CaptureSystemError;
            import org.elasticsearch.foreign.Variadic;
            @LibrarySpecification(unavailableOn = { Platform.WINDOWS_X64 }, symbolResolver = ErrnoLib.FakeResolver.class)
            public interface ErrnoLib {
                @CaptureSystemError
                @Function("foo")
                int foo(int x);

                @CaptureSystemError
                @Variadic(firstArg = 1)
                @Function("bar")
                long bar(long a, int b);

                class FakeResolver implements SymbolResolver {
                    public FakeResolver() {}
                    public ResolvedSymbol resolve(String name, SymbolLookup lookup) {
                        // downcallHandle validates the address is non-NULL; any positive value works.
                        return new ResolvedSymbol(name, MemorySegment.ofAddress(1L));
                    }
                }
            }
            """;

        CompilationResult result = compile("test.ErrnoLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // Loading with init runs the whole downcall-handle build path for both methods:
        // Linker.nativeLinker().downcallHandle(FakeResolver.resolve(...), descriptor,
        // [captureCallState("errno"), firstVariadicArg(1)])
        // A descriptor mismatch (e.g. captureCallState declared as varargs but emitted as
        // (String)) throws NoSuchMethodError from <clinit>.
        Class<?> implClass = result.loadClass("test.ErrnoLib$Impl");
        assertNotNull("Generated ErrnoLib$Impl class not found", implClass);

        // Must NOT have a per-class systemErrorState field — the shared LinkerHelper.SYSTEM_ERROR_STATE is used.
        try {
            implClass.getDeclaredField("systemErrorState");
            fail("ErrnoLib$Impl must not have a per-class systemErrorState field");
        } catch (NoSuchFieldException expected) {
            // expected
        }

        // Both MethodHandle fields must exist.
        assertEquals(MethodHandle.class, implClass.getDeclaredField("foo$mh").getType());
        assertEquals(MethodHandle.class, implClass.getDeclaredField("bar$mh").getType());
    }

    /**
     * A {@code @CaptureSystemError @Function} method on a Windows-only library (GetLastError channel) must
     * generate a class WITHOUT a per-class {@code systemErrorState} field — the shared segment is
     * obtained at call-time via {@code LinkerHelper.systemErrorState()}. Verified structurally via
     * {@code loadClassNoInit}: class-init cannot be driven on non-Windows because
     * {@code Linker.Option.captureCallState("GetLastError")} throws
     * {@code IllegalArgumentException} before any descriptor is even built. On Windows, class-init
     * is exercised by the Windows-gated {@code LinkerHelperTests} test.
     *
     * <p>The library marks every POSIX platform unavailable, which is what makes {@code @CaptureSystemError}
     * resolve to the {@code GetLastError} channel rather than {@code errno}.
     *
     * <p>The custom {@link org.elasticsearch.foreign.SymbolResolver} returns a fake non-null
     * address so a hypothetical {@code linker.downcallHandle} call would succeed without needing a
     * real native symbol on the classpath; it is unused by the {@code loadClassNoInit} path but
     * kept for parity with the errno test and to keep the fixture realistic.
     */
    public void testSystemErrorLastErrorInitializesAgainstFfmApi() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.SymbolLookup;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Platform;
            import org.elasticsearch.foreign.ResolvedSymbol;
            import org.elasticsearch.foreign.SymbolResolver;
            import org.elasticsearch.foreign.CaptureSystemError;
            @LibrarySpecification(
                unavailableOn = {
                    Platform.LINUX_X64,
                    Platform.LINUX_AARCH64,
                    Platform.DARWIN_X64,
                    Platform.DARWIN_AARCH64
                },
                symbolResolver = LastErrorLib.FakeResolver.class
            )
            public interface LastErrorLib {
                @CaptureSystemError
                @Function("foo")
                int foo(int x);

                class FakeResolver implements SymbolResolver {
                    public FakeResolver() {}
                    public ResolvedSymbol resolve(String name, SymbolLookup lookup) {
                        // downcallHandle validates the address is non-NULL; any positive value works.
                        return new ResolvedSymbol(name, MemorySegment.ofAddress(1L));
                    }
                }
            }
            """;

        CompilationResult result = compile("test.LastErrorLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.LastErrorLib$Impl");
        assertNotNull("Generated LastErrorLib$Impl class not found", implClass);

        // Must NOT have a per-class systemErrorState field — shared via LinkerHelper.systemErrorState().
        try {
            implClass.getDeclaredField("systemErrorState");
            fail("LastErrorLib$Impl must not have a per-class systemErrorState field");
        } catch (NoSuchFieldException expected) {
            // expected
        }

        assertEquals(MethodHandle.class, implClass.getDeclaredField("foo$mh").getType());
    }

    /**
     * A minimal {@code @LibrarySpecification} with a {@code @StructSpecification} record element,
     * a {@code @StructSpecification} interface with an {@code @ArrayField} method, and a
     * {@code @StructFactory} method must generate loadable classes AND, when the factory is
     * actually invoked from usage code, produce a working {@code Buf} without hitting any
     * runtime linkage errors. The usage class {@code BufDriver} is compiled alongside the
     * library and calls {@code BufLib$Impl} directly — that side-steps ServiceLoader for the
     * test while still exercising the full generated factory body (Arena allocation, per-element
     * pack, len + pointer writes).
     */
    public void testStructFactoryGeneratesLoadableImplClass() throws Exception {
        String libSource = """
            package test;
            import org.elasticsearch.foreign.Addressable;
            import org.elasticsearch.foreign.ArrayField;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructFactory;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface BufLib {
                @StructSpecification
                record Elem(short x) {}

                @StructSpecification
                interface Buf extends Addressable {
                    short len();

                    @ArrayField(lengthField = "len")
                    Elem elem(int index);
                }

                @StructFactory
                Buf newBuf(Elem[] elems);
            }
            """;
        String driverSource = """
            package test;
            public final class BufDriver {
                public static BufLib.Buf create(short x) {
                    BufLib lib = new BufLib$Impl();
                    return lib.newBuf(new BufLib.Elem[] { new BufLib.Elem(x) });
                }
            }
            """;

        var sources = new java.util.LinkedHashMap<String, String>();
        sources.put("test.BufLib", libSource);
        sources.put("test.BufDriver", driverSource);
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // Loading BufLib$Buf$Impl triggers <clinit>, which builds the LAYOUT via
        // MemoryLayout.structLayout(SHORT.withName("len"), paddingLayout(6), ADDRESS.withName("elem"))
        // and creates the two VarHandles. A descriptor mismatch (e.g. paddingLayout return type)
        // would surface here.
        assertNotNull("Generated BufLib$Buf$Impl not found", result.loadClass("test.BufLib$Buf$Impl"));
        assertNotNull("Generated BufLib$Elem$Pack not found", result.loadClass("test.BufLib$Elem$Pack"));

        // Invoke the driver's create(short) method to exercise the full generated factory body:
        // Arena.ofAuto().allocate(layout, count) -> per-element Pack.pack loop
        // -> len$vh.set / elem$ptr$vh.set. This is the assertion that would catch a signature
        // mismatch in the emitted invokeinterface descriptors.
        Class<?> driver = result.loadClass("test.BufDriver");
        Object buf = driver.getMethod("create", short.class).invoke(null, (short) 42);
        assertNotNull("BufDriver.create must return a non-null Buf", buf);
    }

    /**
     * End-to-end test: a library whose {@code @LibrarySpecification} names a custom
     * {@link org.elasticsearch.foreign.MethodHandleResolver} that ignores the native symbol and
     * returns a constant-returning handle. When the generated {@code $Impl} is invoked, the
     * transformation from the custom resolver must be visible — proving the processor routes
     * method-handle creation through {@code MethodHandleResolver.resolve} rather than directly
     * calling {@code Linker.downcallHandle}.
     *
     * <p>The custom resolver returns {@code MethodHandles.constant(int.class, 99)} so the
     * generated method always yields {@code 99}, regardless of what native symbol was resolved.
     * Both a custom {@link org.elasticsearch.foreign.SymbolResolver} (returning a fake address so
     * {@code defaultLookup()} is never consulted) and the custom {@code MethodHandleResolver} are
     * specified in the same {@code @LibrarySpecification}.
     */
    public void testCustomMethodHandleResolverIsInvokedByGeneratedClinit() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.FunctionDescriptor;
            import java.lang.foreign.Linker;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.SymbolLookup;
            import java.lang.invoke.MethodHandle;
            import java.lang.invoke.MethodHandles;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.MethodHandleResolver;
            import org.elasticsearch.foreign.ResolvedSymbol;
            import org.elasticsearch.foreign.SymbolResolver;
            @LibrarySpecification(
                symbolResolver = ConstLib.FakeSymbolResolver.class,
                methodHandleResolver = ConstLib.ConstantResolver.class
            )
            public interface ConstLib {
                @Function("getpid")
                int getpid();

                class FakeSymbolResolver implements SymbolResolver {
                    public FakeSymbolResolver() {}
                    public ResolvedSymbol resolve(String name, SymbolLookup lookup) {
                        return new ResolvedSymbol(name, MemorySegment.ofAddress(1L));
                    }
                }

                class ConstantResolver implements MethodHandleResolver {
                    public ConstantResolver() {}
                    public MethodHandle resolve(ResolvedSymbol symbol, FunctionDescriptor descriptor,
                                                Linker linker, Linker.Option... options) {
                        // Return a handle that always yields the constant 99, ignoring the native symbol.
                        return MethodHandles.constant(int.class, 99);
                    }
                }
            }
            """;
        String driverSource = """
            package test;
            public final class ConstLibDriver {
                public static int call() throws Throwable {
                    ConstLib lib = new ConstLib$Impl();
                    return lib.getpid();
                }
            }
            """;

        var sources = new java.util.LinkedHashMap<String, String>();
        sources.put("test.ConstLib", source);
        sources.put("test.ConstLibDriver", driverSource);
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // Loading the $Impl triggers <clinit>: the custom ConstantResolver.resolve() is called and
        // returns MethodHandles.constant(int.class, 99), which is stored in the getpid$mh field.
        Class<?> implClass = result.loadClass("test.ConstLib$Impl");
        assertNotNull("Generated ConstLib$Impl class not found", implClass);

        // Drive the generated method: it must return 99 (the constant from ConstantResolver),
        // not the real pid, proving the custom MethodHandleResolver was used.
        Class<?> driver = result.loadClass("test.ConstLibDriver");
        Object returnVal = driver.getMethod("call").invoke(null);
        assertEquals("getpid() must return the constant 99 from ConstantResolver", 99, returnVal);
    }

    /**
     * A {@code @StructFactory} method returning a struct with only scalar fields (no
     * {@code @ArrayField}) must generate a simple factory that returns a fresh {@code $Impl}
     * whose {@code segment()} is a live native segment with the expected layout byte size.
     */
    public void testSimpleStructFactoryGeneratesWorkingFactory() throws Exception {
        String libSource = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructFactory;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface PointLib {
                @StructSpecification
                interface Point {
                    int x();
                    void x(int v);
                    int y();
                    void y(int v);
                }

                @StructFactory
                Point newPoint();
            }
            """;
        String driverSource = """
            package test;
            import java.lang.foreign.MemorySegment;
            public final class PointDriver {
                public static long create(int x, int y) {
                    PointLib lib = new PointLib$Impl();
                    PointLib.Point p = lib.newPoint();
                    p.x(x);
                    p.y(y);
                    PointLib$Point$Impl impl = (PointLib$Point$Impl) p;
                    return impl.segment().byteSize();
                }
            }
            """;

        var sources = new java.util.LinkedHashMap<String, String>();
        sources.put("test.PointLib", libSource);
        sources.put("test.PointDriver", driverSource);
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        assertNotNull("Generated PointLib$Point$Impl not found", result.loadClass("test.PointLib$Point$Impl"));

        // Factory method must exist on the $Impl with zero parameters
        Class<?> implClass = result.loadClassNoInit("test.PointLib$Impl");
        java.lang.reflect.Method factoryMethod = implClass.getMethod("newPoint");
        assertEquals("newPoint must return PointLib.Point", "test.PointLib$Point", factoryMethod.getReturnType().getName());

        // Invoke the driver: factory creates a fresh $Impl, sets x and y via setters, returns byteSize
        Class<?> driver = result.loadClass("test.PointDriver");
        long byteSize = (long) driver.getMethod("create", int.class, int.class).invoke(null, 3, 7);

        // Two int fields = 8 bytes
        assertEquals("Point segment must be 8 bytes (2 x int)", 8L, byteSize);
    }

    /**
     * A {@code @StructSpecification} interface that does NOT declare {@code extends Addressable}
     * must still compile cleanly, and its generated {@code $Impl} class must implement
     * {@code Addressable} (guaranteeing the runtime cast in native call marshaling works).
     */
    public void testStructInterfaceWithoutAddressableCompilesAndImplIsAddressable() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface NoAddrLib {
                @StructSpecification
                interface Coord {
                    int x();
                    int y();
                }
            }
            """;

        CompilationResult result = compile("test.NoAddrLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.NoAddrLib$Coord$Impl");
        assertNotNull("Generated NoAddrLib$Coord$Impl class not found", implClass);

        // Even though Coord does not extend Addressable, the generated $Impl must implement it
        boolean implementsAddressable = false;
        for (Class<?> iface : implClass.getInterfaces()) {
            if (iface.getName().equals("org.elasticsearch.foreign.Addressable")) {
                implementsAddressable = true;
                break;
            }
        }
        assertTrue("NoAddrLib$Coord$Impl must implement Addressable", implementsAddressable);
    }

    /**
     * A {@code @Function} method whose parameter is a struct-typed interface (without
     * {@code extends Addressable}) must generate a loadable {@code $Impl} class whose method
     * accepts the struct interface type. Verified by compiling a driver class that calls
     * {@code doWork} with a concrete {@code Point$Impl} instance — this exercises the generated
     * checkcast-to-Addressable bytecode at runtime.
     */
    public void testStructParamWithoutAddressableGeneratesCorrectMethod() throws Exception {
        String libSource = """
            package test;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.SymbolLookup;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.ResolvedSymbol;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.SymbolResolver;
            @LibrarySpecification(symbolResolver = StructParamLib.FakeResolver.class)
            public interface StructParamLib {
                @StructSpecification
                interface Point {
                    int x();
                    int y();
                }

                @Function("native_fn")
                int doWork(Point p, int flags);

                class FakeResolver implements SymbolResolver {
                    public FakeResolver() {}
                    public ResolvedSymbol resolve(String name, SymbolLookup lookup) {
                        return new ResolvedSymbol(name, MemorySegment.ofAddress(1L));
                    }
                }
            }
            """;
        // A driver class that verifies: (a) Point$Impl can be constructed,
        // (b) the generated $Impl.doWork(Point, int) compiles (accepts the struct interface type),
        // (c) the generated checkcast-to-Addressable code is reachable at runtime.
        String driverSource = """
            package test;
            public final class StructParamDriver {
                public static Class<?> pointImplClass() {
                    return StructParamLib$Point$Impl.class;
                }
                public static Class<?>[] doWorkParamTypes() throws Exception {
                    return StructParamLib$Impl.class.getMethod("doWork", StructParamLib.Point.class, int.class).getParameterTypes();
                }
            }
            """;

        var sources = new java.util.LinkedHashMap<String, String>();
        sources.put("test.StructParamLib", libSource);
        sources.put("test.StructParamDriver", driverSource);
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // Load with init: <clinit> of $Impl runs downcallHandle (FakeResolver returns non-null)
        assertNotNull("Generated StructParamLib$Impl not found", result.loadClass("test.StructParamLib$Impl"));
        assertNotNull("Generated StructParamLib$Point$Impl not found", result.loadClass("test.StructParamLib$Point$Impl"));

        // Driver confirms doWork takes (Point, int) via reflection on the generated $Impl
        Class<?> driver = result.loadClass("test.StructParamDriver");
        Class<?>[] paramTypes = (Class<?>[]) driver.getMethod("doWorkParamTypes").invoke(null);
        assertEquals("doWork must take 2 parameters", 2, paramTypes.length);
        assertEquals("first param must be StructParamLib.Point", "test.StructParamLib$Point", paramTypes[0].getName());
        assertEquals("second param must be int", int.class, paramTypes[1]);
    }

    /**
     * A {@code @StructSpecification} interface that explicitly declares {@code extends Addressable}
     * must continue to compile and function correctly — backward compatibility.
     */
    public void testStructInterfaceExplicitAddressableStillWorks() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.Addressable;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface ExplicitAddrLib {
                @StructSpecification
                interface Value extends Addressable {
                    long val();
                }
            }
            """;

        CompilationResult result = compile("test.ExplicitAddrLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.ExplicitAddrLib$Value$Impl");
        assertNotNull("Generated ExplicitAddrLib$Value$Impl class not found", implClass);

        // Must still implement Addressable
        boolean implementsAddressable = false;
        for (Class<?> iface : implClass.getInterfaces()) {
            if (iface.getName().equals("org.elasticsearch.foreign.Addressable")) {
                implementsAddressable = true;
                break;
            }
        }
        assertTrue("ExplicitAddrLib$Value$Impl must implement Addressable", implementsAddressable);

        // Must have a long val() method
        java.lang.reflect.Method valMethod = implClass.getMethod("val");
        assertEquals("val() must return long", long.class, valMethod.getReturnType());
    }

    /**
     * A struct interface with getter+setter for the same field name must generate both methods
     * backed by a single VarHandle. A round-trip set-then-get must return the written value.
     */
    public void testScalarGetterSetterPairGeneratesRoundTrip() throws Exception {
        String libSource = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface GetSetLib {
                @StructSpecification
                interface Counter {
                    long value();
                    void value(long v);
                }
            }
            """;
        String driverSource = """
            package test;
            public final class GetSetDriver {
                public static long roundTrip(long x) {
                    GetSetLib$Counter$Impl c = new GetSetLib$Counter$Impl();
                    c.value(x);
                    return c.value();
                }
            }
            """;

        var sources = new java.util.LinkedHashMap<String, String>();
        sources.put("test.GetSetLib", libSource);
        sources.put("test.GetSetDriver", driverSource);
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // The $Impl must have exactly one VarHandle field: value$vh (not two)
        Class<?> implClass = result.loadClass("test.GetSetLib$Counter$Impl");
        assertNotNull("Generated GetSetLib$Counter$Impl not found", implClass);
        java.lang.reflect.Field vh = implClass.getDeclaredField("value$vh");
        assertEquals("value$vh must be a VarHandle", java.lang.invoke.VarHandle.class, vh.getType());
        assertTrue("value$vh must be static", java.lang.reflect.Modifier.isStatic(vh.getModifiers()));

        // Both getter and setter methods must be present
        java.lang.reflect.Method getter = implClass.getMethod("value");
        assertEquals("getter must return long", long.class, getter.getReturnType());
        java.lang.reflect.Method setter = implClass.getMethod("value", long.class);
        assertEquals("setter must return void", void.class, setter.getReturnType());

        // Round-trip: writing 42L and reading it back must return 42L
        Class<?> driver = result.loadClass("test.GetSetDriver");
        long result2 = (long) driver.getMethod("roundTrip", long.class).invoke(null, 42L);
        assertEquals("Round-trip set/get must return the written value", 42L, result2);
    }

    /**
     * A struct interface with a setter-only field must generate a setter method but no getter.
     */
    public void testSetterOnlyFieldGeneratesSetterNoGetter() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface SetOnlyLib {
                @StructSpecification
                interface Flags {
                    void set_flags(int f);
                }
            }
            """;

        CompilationResult result = compile("test.SetOnlyLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.SetOnlyLib$Flags$Impl");
        assertNotNull("Generated SetOnlyLib$Flags$Impl not found", implClass);

        // Setter must exist
        java.lang.reflect.Method setter = implClass.getMethod("set_flags", int.class);
        assertEquals("set_flags must return void", void.class, setter.getReturnType());

        // No getter (set_flags() with no params) should exist
        try {
            implClass.getMethod("set_flags");
            fail("set_flags() getter must not be generated for a setter-only field");
        } catch (NoSuchMethodException expected) {
            // expected
        }
    }

    /**
     * An {@code @InlineArrayField} getter+setter pair must generate indexed accessors backed by a
     * single sequence-element VarHandle. A round-trip index-based write-then-read must return the
     * written byte value. The struct layout must have the expected total byte size.
     */
    public void testInlineArrayFieldRoundTrip() throws Exception {
        String libSource = """
            package test;
            import org.elasticsearch.foreign.InlineArrayField;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface SockAddrLib {
                @StructSpecification
                interface SockAddr {
                    short sa_family();
                    void sa_family(short v);

                    @InlineArrayField(length = 108)
                    byte sun_path(int index);

                    @InlineArrayField(length = 108)
                    void sun_path(int index, byte value);
                }
            }
            """;
        String driverSource = """
            package test;
            import java.lang.foreign.MemorySegment;
            public final class SockAddrDriver {
                public static byte roundTrip(int index, byte value) {
                    SockAddrLib$SockAddr$Impl impl = new SockAddrLib$SockAddr$Impl();
                    impl.sun_path(index, value);
                    return impl.sun_path(index);
                }
                public static long layoutByteSize() {
                    SockAddrLib$SockAddr$Impl impl = new SockAddrLib$SockAddr$Impl();
                    return impl.segment().byteSize();
                }
            }
            """;

        var sources = new java.util.LinkedHashMap<String, String>();
        sources.put("test.SockAddrLib", libSource);
        sources.put("test.SockAddrDriver", driverSource);
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        assertNotNull("Generated SockAddrLib$SockAddr$Impl not found", result.loadClass("test.SockAddrLib$SockAddr$Impl"));

        // VarHandle field for the inline array must exist
        Class<?> implClass = result.loadClass("test.SockAddrLib$SockAddr$Impl");
        java.lang.reflect.Field vh = implClass.getDeclaredField("sun_path$vh");
        assertEquals("sun_path$vh must be a VarHandle", java.lang.invoke.VarHandle.class, vh.getType());

        // Both indexed getter and setter must exist on the $Impl
        java.lang.reflect.Method getter = implClass.getMethod("sun_path", int.class);
        assertEquals("sun_path getter must return byte", byte.class, getter.getReturnType());
        java.lang.reflect.Method setter = implClass.getMethod("sun_path", int.class, byte.class);
        assertEquals("sun_path setter must return void", void.class, setter.getReturnType());

        // Round-trip: write 'A' at index 3, read it back
        Class<?> driver = result.loadClass("test.SockAddrDriver");
        byte got = (byte) driver.getMethod("roundTrip", int.class, byte.class).invoke(null, 3, (byte) 65);
        assertEquals("Round-trip inline array write/read must return written value", (byte) 65, got);

        // Layout: short(2) + sequence(108 x byte) = 110 bytes
        long byteSize = (long) driver.getMethod("layoutByteSize").invoke(null);
        assertEquals("SockAddr layout must be 110 bytes (short + 108 bytes)", 110L, byteSize);
    }

    /**
     * An {@code @InlineStringField} getter+setter pair must generate String accessors that operate
     * via {@code MemorySegment.getString/setString}. A write-then-read must return the
     * written string value.
     */
    public void testInlineStringFieldRoundTrip() throws Exception {
        String libSource = """
            package test;
            import org.elasticsearch.foreign.InlineStringField;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface UnixAddrLib {
                @StructSpecification
                interface UnixAddr {
                    short sa_family();
                    void sa_family(short v);

                    @InlineStringField(length = 108)
                    String sun_path();

                    @InlineStringField(length = 108)
                    void sun_path(String value);
                }
            }
            """;
        String driverSource = """
            package test;
            import java.lang.foreign.MemorySegment;
            public final class UnixAddrDriver {
                public static String roundTrip(String value) {
                    UnixAddrLib$UnixAddr$Impl impl = new UnixAddrLib$UnixAddr$Impl();
                    impl.sun_path(value);
                    return impl.sun_path();
                }
                public static long layoutByteSize() {
                    UnixAddrLib$UnixAddr$Impl impl = new UnixAddrLib$UnixAddr$Impl();
                    return impl.segment().byteSize();
                }
            }
            """;

        var sources = new java.util.LinkedHashMap<String, String>();
        sources.put("test.UnixAddrLib", libSource);
        sources.put("test.UnixAddrDriver", driverSource);
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.UnixAddrLib$UnixAddr$Impl");
        assertNotNull("Generated UnixAddrLib$UnixAddr$Impl not found", implClass);

        // No VarHandle for string field — String accessors use direct segment operations
        try {
            implClass.getDeclaredField("sun_path$vh");
            fail("InlineStringField must not generate a VarHandle field");
        } catch (NoSuchFieldException expected) {
            // expected
        }

        // Getter returns String; setter takes String
        java.lang.reflect.Method getter = implClass.getMethod("sun_path");
        assertEquals("sun_path getter must return String", String.class, getter.getReturnType());
        java.lang.reflect.Method setter = implClass.getMethod("sun_path", String.class);
        assertEquals("sun_path setter must return void", void.class, setter.getReturnType());

        // Round-trip: write "/tmp/test.sock" and read it back
        Class<?> driver = result.loadClass("test.UnixAddrDriver");
        String got = (String) driver.getMethod("roundTrip", String.class).invoke(null, "/tmp/test.sock");
        assertEquals("Round-trip inline string write/read must return written value", "/tmp/test.sock", got);

        // Layout: short(2) + sequence(108 x byte) = 110 bytes
        long byteSize = (long) driver.getMethod("layoutByteSize").invoke(null);
        assertEquals("UnixAddr layout must be 110 bytes (short + 108 bytes)", 110L, byteSize);
    }

    /**
     * A {@code @Function} method with a {@code @WideString String} parameter must generate a body
     * that calls the charset-aware {@code Arena.allocateFrom(String, Charset)} overload rather than
     * the plain 1-arg {@code allocateFrom(String)} used for ordinary (UTF-8) {@code String}
     * parameters. Verified structurally by parsing the generated bytecode.
     */
    public void testWideStringParamGeneratesCharsetAwareAllocation() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.WideString;
            @LibrarySpecification
            public interface WideParamLib {
                @Function("native_op")
                int op(@WideString String name, int flags);
            }
            """;

        CompilationResult result = compile("test.WideParamLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Path classFile = result.outputDir().resolve("test/WideParamLib$Impl.class");
        assertTrue("Generated WideParamLib$Impl.class not found", Files.exists(classFile));
        byte[] bytes = Files.readAllBytes(classFile);
        var cm = ClassFile.of().parse(bytes);

        List<InvokeInstruction> allocateStringCalls = cm.methods()
            .stream()
            .filter(m -> m.methodName().equalsString("op"))
            .flatMap(m -> m.code().stream())
            .flatMap(ca -> ca.elementStream())
            .filter(e -> e instanceof InvokeInstruction)
            .map(e -> (InvokeInstruction) e)
            .filter(i -> i.name().equalsString("allocateFrom"))
            .toList();
        assertEquals("Expected exactly one allocateFrom call in op", 1, allocateStringCalls.size());
        assertEquals(
            "@WideString param must use the 2-arg charset-aware allocateFrom overload",
            2,
            allocateStringCalls.get(0).typeSymbol().parameterCount()
        );
    }

    /**
     * A {@code @Function} method with a mix of ordinary {@code String} and {@code @WideString}
     * parameters must route each to the correct {@code Arena.allocateFrom} overload: the plain 1-arg
     * form for UTF-8 and the 2-arg charset-aware form for UTF-16LE, in parameter order.
     * This guards against the {@code paramIndex} bookkeeping in {@link ImplClassWriter} drifting out
     * of sync with the index set built by {@link org.elasticsearch.foreign.processor.model.MethodModel}.
     */
    public void testMixedWideAndNarrowStringParamsGenerateCorrectOverloads() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.WideString;
            @LibrarySpecification
            public interface MixedParamLib {
                @Function("native_op")
                int op(String narrowPath, @WideString String wideName);
            }
            """;

        CompilationResult result = compile("test.MixedParamLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Path classFile = result.outputDir().resolve("test/MixedParamLib$Impl.class");
        assertTrue("Generated MixedParamLib$Impl.class not found", Files.exists(classFile));
        byte[] bytes = Files.readAllBytes(classFile);
        var cm = ClassFile.of().parse(bytes);

        List<InvokeInstruction> allocateStringCalls = cm.methods()
            .stream()
            .filter(m -> m.methodName().equalsString("op"))
            .flatMap(m -> m.code().stream())
            .flatMap(ca -> ca.elementStream())
            .filter(e -> e instanceof InvokeInstruction)
            .map(e -> (InvokeInstruction) e)
            .filter(i -> i.name().equalsString("allocateFrom"))
            .toList();
        assertEquals("Expected two allocateFrom calls in op (one per String param)", 2, allocateStringCalls.size());
        assertEquals(
            "Narrow (UTF-8) param must use the 1-arg allocateFrom overload",
            1,
            allocateStringCalls.get(0).typeSymbol().parameterCount()
        );
        assertEquals(
            "@WideString param must use the 2-arg charset-aware allocateFrom overload",
            2,
            allocateStringCalls.get(1).typeSymbol().parameterCount()
        );
    }

    /**
     * A struct with a {@code @InlineStringField(length = 16, wide = true)} getter+setter pair must
     * round-trip a non-ASCII string through real bytecode execution, proving the codegen calls the
     * UTF-16LE-aware {@code MemorySegment.getString/setString} overloads.
     */
    public void testWideInlineStringFieldRoundTrip() throws Exception {
        String libSource = """
            package test;
            import org.elasticsearch.foreign.InlineStringField;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface WinStructLib {
                @StructSpecification
                interface WinStruct {
                    @InlineStringField(length = 16, wide = true)
                    String name();

                    @InlineStringField(length = 16, wide = true)
                    void name(String value);
                }
            }
            """;
        String driverSource = """
            package test;
            import java.lang.foreign.ValueLayout;
            public final class WinStructDriver {
                public static String roundTrip(String value) {
                    WinStructLib$WinStruct$Impl impl = new WinStructLib$WinStruct$Impl();
                    impl.name(value);
                    return impl.name();
                }
                public static byte[] rawBytes(String value) {
                    WinStructLib$WinStruct$Impl impl = new WinStructLib$WinStruct$Impl();
                    impl.name(value);
                    return impl.segment().toArray(ValueLayout.JAVA_BYTE);
                }
            }
            """;

        var sources = new java.util.LinkedHashMap<String, String>();
        sources.put("test.WinStructLib", libSource);
        sources.put("test.WinStructDriver", driverSource);
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> driver = result.loadClass("test.WinStructDriver");

        // Functional round-trip: writing a non-ASCII string and reading it back must decode correctly.
        String got = (String) driver.getMethod("roundTrip", String.class).invoke(null, "héllo");
        assertEquals("Round-trip wide inline string write/read must return written value", "héllo", got);

        // Byte-exact check: proves the setter actually wrote UTF-16LE, not just a self-consistent
        // encoding. Under UTF-8 (the narrow default), "abc" would be written as 3 bytes + 1-byte NUL;
        // under UTF-16LE it must be 3 code units (2 bytes each, low byte first for ASCII) + a 2-byte
        // NUL terminator. A wide->narrow regression that still round-trips correctly (getter and
        // setter consistently wrong) would still fail this assertion.
        byte[] expected = new byte[] { 'a', 0, 'b', 0, 'c', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        byte[] rawBytes = (byte[]) driver.getMethod("rawBytes", String.class).invoke(null, "abc");
        assertTrue(
            "Expected UTF-16LE byte layout " + java.util.Arrays.toString(expected) + " but got " + java.util.Arrays.toString(rawBytes),
            java.util.Arrays.equals(expected, rawBytes)
        );
    }

    /**
     * End-to-end test: on Windows x64 only, a {@code @WideString String} parameter is actually
     * encoded as UTF-16LE before the native call. Verified by calling the real {@code FormatMessageW}
     * from {@code kernel32.dll} with {@code FORMAT_MESSAGE_FROM_STRING | FORMAT_MESSAGE_IGNORE_INSERTS}
     * and a non-ASCII format string, then reading the output back from the wide-char buffer.
     * Using a non-ASCII character (é, U+00E9) ensures the test would fail if the framework used
     * UTF-8 rather than UTF-16LE: FormatMessageW would receive a mis-encoded string and produce
     * wrong or garbage output.
     */
    public void testWideStringParamEndToEndOnWindows() throws Exception {
        if (System.getProperty("os.name", "").startsWith("Windows") == false) {
            return;
        }
        String libSource = """
            package test;
            import java.lang.foreign.Arena;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.SymbolLookup;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.ResolvedSymbol;
            import org.elasticsearch.foreign.SymbolResolver;
            import org.elasticsearch.foreign.WideString;
            @LibrarySpecification(symbolResolver = FormatMessageLib.Kernel32Resolver.class)
            public interface FormatMessageLib {
                int FORMAT_MESSAGE_FROM_STRING = 0x00000400;
                int FORMAT_MESSAGE_IGNORE_INSERTS = 0x00000200;
                @Function("FormatMessageW")
                int formatMessage(
                    int dwFlags,
                    @WideString String lpSource,
                    int dwMessageId,
                    int dwLanguageId,
                    MemorySegment lpBuffer,
                    int nSize,
                    MemorySegment pArguments
                );
                class Kernel32Resolver implements SymbolResolver {
                    public Kernel32Resolver() {}
                    public ResolvedSymbol resolve(String name, SymbolLookup ignored) {
                        return new ResolvedSymbol(name,
                            SymbolLookup.libraryLookup("kernel32.dll", Arena.global())
                                .find(name)
                                .orElseThrow(() -> new UnsatisfiedLinkError("Not found in kernel32.dll: " + name)));
                    }
                }
            }
            """;
        String driverSource = """
            package test;
            import java.lang.foreign.Arena;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.ValueLayout;
            public final class FormatMessageDriver {
                public static String callWithFormat(String fmt) throws Exception {
                    FormatMessageLib lib = new FormatMessageLib$Impl();
                    try (Arena arena = Arena.ofConfined()) {
                        int bufChars = 512;
                        MemorySegment buf = arena.allocate(ValueLayout.JAVA_CHAR, bufChars);
                        int chars = lib.formatMessage(
                            FormatMessageLib.FORMAT_MESSAGE_FROM_STRING
                                | FormatMessageLib.FORMAT_MESSAGE_IGNORE_INSERTS,
                            fmt,
                            0, 0,
                            buf, bufChars,
                            MemorySegment.NULL
                        );
                        StringBuilder sb = new StringBuilder(chars);
                        for (int i = 0; i < chars; i++) {
                            sb.append(buf.getAtIndex(ValueLayout.JAVA_CHAR, i));
                        }
                        return sb.toString();
                    }
                }
            }
            """;

        var sources = new java.util.LinkedHashMap<String, String>();
        sources.put("test.FormatMessageLib", libSource);
        sources.put("test.FormatMessageDriver", driverSource);
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // Loading FormatMessageLib$Impl triggers <clinit>, which resolves FormatMessageW from kernel32.dll.
        assertNotNull("Generated FormatMessageLib$Impl not found", result.loadClass("test.FormatMessageLib$Impl"));

        Class<?> driver = result.loadClass("test.FormatMessageDriver");
        // "café" contains é (U+00E9): two bytes in UTF-8 (0xC3 0xA9) but one UTF-16LE code unit (0xE9 0x00).
        // If the framework mistakenly used UTF-8, FormatMessageW would receive a 6-byte sequence that
        // does not represent valid UTF-16LE text and would produce wrong output.
        String input = "café";
        String got = (String) driver.getMethod("callWithFormat", String.class).invoke(null, input);
        assertEquals("FormatMessageW must echo the @WideString format string unchanged", input, got);
    }

    /**
     * An abstract-class {@code @LibrarySpecification} must generate a {@code $Impl} that extends
     * the abstract class (not {@code Object}) and implements the abstract methods. The generated
     * {@code $Impl.getSuperclass()} must equal the abstract class, and a {@code protected abstract}
     * method must carry {@code Modifier.PROTECTED} in the generated impl.
     */
    public void testAbstractClassImplExtendsSuperclass() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib")
            public abstract class MyAbstractLib {
                @Function("native_add")
                public abstract int add(int a, int b);

                @Function("native_sub")
                protected abstract int sub(int a, int b);
            }
            """;

        CompilationResult result = compile("test.MyAbstractLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.MyAbstractLib$Impl");
        assertNotNull("Generated MyAbstractLib$Impl class not found", implClass);

        // $Impl must extend the abstract class, not Object
        Class<?> superClass = implClass.getSuperclass();
        assertEquals("test.MyAbstractLib", superClass.getName());

        // $Impl must not implement any separate interface for the library type
        Class<?>[] ifaces = implClass.getInterfaces();
        for (Class<?> iface : ifaces) {
            assertFalse("$Impl must not implement MyAbstractLib as an interface", iface.getName().equals("test.MyAbstractLib"));
        }

        // The protected method must carry ACC_PROTECTED (not ACC_PUBLIC)
        Method subMethod = null;
        for (Method m : implClass.getDeclaredMethods()) {
            if (m.getName().equals("sub")) {
                subMethod = m;
                break;
            }
        }
        assertNotNull("sub method not found in MyAbstractLib$Impl", subMethod);
        assertTrue("sub must be protected", Modifier.isProtected(subMethod.getModifiers()));
        assertFalse("sub must not be public", Modifier.isPublic(subMethod.getModifiers()));
    }

    /**
     * A {@code String} parameter passed as {@code null} must not reach
     * {@code allocateFrom}, which would throw {@link NullPointerException}. The generated
     * bytecode must contain an {@code IFNONNULL} guard that routes null strings to
     * {@code MemorySegment.NULL} instead.
     */
    public void testNullStringParamGeneratesNullCheck() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification
            public interface NullableStringLib {
                @Function("native_op")
                int op(String name, int flags);
            }
            """;

        CompilationResult result = compile("test.NullableStringLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Path classFile = result.outputDir().resolve("test/NullableStringLib$Impl.class");
        assertTrue("Generated NullableStringLib$Impl.class not found", Files.exists(classFile));
        byte[] bytes = Files.readAllBytes(classFile);

        var cm = ClassFile.of().parse(bytes);
        boolean hasNullCheck = cm.methods()
            .stream()
            .filter(m -> m.methodName().equalsString("op"))
            .flatMap(m -> m.code().stream())
            .flatMap(ca -> ca.elementStream())
            .anyMatch(e -> e instanceof BranchInstruction bi && bi.opcode() == Opcode.IFNONNULL);
        assertTrue("Generated op body must contain IFNONNULL for null-String guard", hasNullCheck);
    }

    /**
     * {@code @LibrarySpecification(system = true)} must generate a {@code <clinit>} that loads the
     * native library via {@code System.loadLibrary}, not the default
     * {@code LoaderHelper.loadLibrary}. Verified structurally by parsing the generated bytecode.
     */
    public void testSystemLibraryLoadedViaSystemLoadLibrary() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "sysvvv", system = true)
            public interface SystemLib {
                @Function("native_op")
                int op(int flags);
            }
            """;

        CompilationResult result = compile("test.SystemLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Path classFile = result.outputDir().resolve("test/SystemLib$Impl.class");
        assertTrue("Generated SystemLib$Impl.class not found", Files.exists(classFile));
        byte[] bytes = Files.readAllBytes(classFile);
        var cm = ClassFile.of().parse(bytes);

        List<InvokeInstruction> loadLibraryCalls = cm.methods()
            .stream()
            .filter(m -> m.methodName().equalsString("<clinit>"))
            .flatMap(m -> m.code().stream())
            .flatMap(ca -> ca.elementStream())
            .filter(e -> e instanceof InvokeInstruction)
            .map(e -> (InvokeInstruction) e)
            .filter(i -> i.name().equalsString("loadLibrary"))
            .toList();
        assertEquals("Expected exactly one loadLibrary call in <clinit>", 1, loadLibraryCalls.size());
        assertEquals(
            "system = true must call System.loadLibrary, not LoaderHelper.loadLibrary",
            "java/lang/System",
            loadLibraryCalls.get(0).owner().asInternalName()
        );
    }

    /**
     * The default ({@code system = false}) case must keep loading the library via
     * {@code LoaderHelper.loadLibrary}, proving the {@code system} flag is additive and does not
     * change existing generated bytecode.
     */
    public void testDefaultLibraryLoadedViaLoaderHelper() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib")
            public interface DefaultLoadLib {
                @Function("native_op")
                int op(int flags);
            }
            """;

        CompilationResult result = compile("test.DefaultLoadLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Path classFile = result.outputDir().resolve("test/DefaultLoadLib$Impl.class");
        assertTrue("Generated DefaultLoadLib$Impl.class not found", Files.exists(classFile));
        byte[] bytes = Files.readAllBytes(classFile);
        var cm = ClassFile.of().parse(bytes);

        List<InvokeInstruction> loadLibraryCalls = cm.methods()
            .stream()
            .filter(m -> m.methodName().equalsString("<clinit>"))
            .flatMap(m -> m.code().stream())
            .flatMap(ca -> ca.elementStream())
            .filter(e -> e instanceof InvokeInstruction)
            .map(e -> (InvokeInstruction) e)
            .filter(i -> i.name().equalsString("loadLibrary"))
            .toList();
        assertEquals("Expected exactly one loadLibrary call in <clinit>", 1, loadLibraryCalls.size());
        assertEquals(
            "system = false (default) must call LoaderHelper.loadLibrary",
            "org/elasticsearch/foreign/LoaderHelper",
            loadLibraryCalls.get(0).owner().asInternalName()
        );
    }

    /**
     * Two overloaded Java methods binding to the same C symbol must generate two distinct
     * {@code MethodHandle} fields using the ordinal-suffix naming strategy:
     * {@code open$0$mh} and {@code open$1$mh}.
     */
    public void testOverloadedMethodsSameCSymbolGetDisambiguatedFields() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.SymbolLookup;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Platform;
            import org.elasticsearch.foreign.ResolvedSymbol;
            import org.elasticsearch.foreign.SymbolResolver;
            import org.elasticsearch.foreign.CaptureSystemError;
            import org.elasticsearch.foreign.Variadic;
            @LibrarySpecification(unavailableOn = { Platform.WINDOWS_X64 }, symbolResolver = OpenLib.FakeResolver.class)
            public interface OpenLib {
                @CaptureSystemError @Variadic(firstArg = 2) @Function("open")
                int open(String pathname, int flags);

                @CaptureSystemError @Variadic(firstArg = 2) @Function("open")
                int open(String pathname, int flags, int mode);

                class FakeResolver implements SymbolResolver {
                    public FakeResolver() {}
                    public ResolvedSymbol resolve(String name, SymbolLookup lookup) {
                        return new ResolvedSymbol(name, MemorySegment.ofAddress(1L));
                    }
                }
            }
            """;

        CompilationResult result = compile("test.OpenLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.OpenLib$Impl");
        assertNotNull("Generated OpenLib$Impl class not found", implClass);

        // The collision case must use positional suffixes; there must be no plain open$mh field.
        java.lang.reflect.Field mh0 = implClass.getDeclaredField("open$0$mh");
        java.lang.reflect.Field mh1 = implClass.getDeclaredField("open$1$mh");
        assertEquals("open$0$mh must be a MethodHandle", MethodHandle.class, mh0.getType());
        assertEquals("open$1$mh must be a MethodHandle", MethodHandle.class, mh1.getType());

        try {
            implClass.getDeclaredField("open$mh");
            fail("open$mh must not exist when overloads are present");
        } catch (NoSuchFieldException expected) {
            // expected
        }
    }

    /**
     * Two overloaded Java methods binding to *different* C symbols must still be disambiguated
     * by ordinal, since they share the same Java method name.
     */
    public void testOverloadedMethodsDifferentCSymbolsGetDisambiguatedFields() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            @LibrarySpecification(name = "testlib")
            public interface MultiSymbolLib {
                @Function("foo_v1")
                int foo(int x);

                @Function("foo_v2")
                int foo(int x, int y);
            }
            """;

        CompilationResult result = compile("test.MultiSymbolLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.MultiSymbolLib$Impl");
        assertNotNull("Generated MultiSymbolLib$Impl class not found", implClass);

        java.lang.reflect.Field mh0 = implClass.getDeclaredField("foo$0$mh");
        java.lang.reflect.Field mh1 = implClass.getDeclaredField("foo$1$mh");
        assertEquals("foo$0$mh must be a MethodHandle", MethodHandle.class, mh0.getType());
        assertEquals("foo$1$mh must be a MethodHandle", MethodHandle.class, mh1.getType());

        try {
            implClass.getDeclaredField("foo$mh");
            fail("foo$mh must not exist when overloads are present");
        } catch (NoSuchFieldException expected) {
            // expected
        }
    }

    /**
     * Three overloaded Java methods with the same name must produce {@code name$0$mh},
     * {@code name$1$mh}, and {@code name$2$mh} in declaration order.
     */
    public void testThreeOverloadsGetCorrectOrdinals() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            @LibrarySpecification(name = "testlib")
            public interface TripleLib {
                @Function("native_bar")
                int bar(int a);

                @Function("native_bar")
                int bar(int a, int b);

                @Function("native_bar")
                int bar(int a, int b, int c);
            }
            """;

        CompilationResult result = compile("test.TripleLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.TripleLib$Impl");
        assertNotNull("Generated TripleLib$Impl class not found", implClass);

        java.lang.reflect.Field mh0 = implClass.getDeclaredField("bar$0$mh");
        java.lang.reflect.Field mh1 = implClass.getDeclaredField("bar$1$mh");
        java.lang.reflect.Field mh2 = implClass.getDeclaredField("bar$2$mh");
        assertEquals(MethodHandle.class, mh0.getType());
        assertEquals(MethodHandle.class, mh1.getType());
        assertEquals(MethodHandle.class, mh2.getType());

        try {
            implClass.getDeclaredField("bar$mh");
            fail("bar$mh must not exist when overloads are present");
        } catch (NoSuchFieldException expected) {
            // expected
        }
    }

    /**
     * A library with a mix of unique-named methods and overloaded methods must generate the plain
     * {@code <name>$mh} form for the unique method and ordinal-suffixed fields for the overloads.
     */
    public void testMixedUniqueAndOverloadedMethodsGetCorrectFieldNames() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            @LibrarySpecification(name = "testlib")
            public interface MixedLib {
                @Function("unique_fn")
                int baz(int x);

                @Function("multi_fn")
                int foo(int x);

                @Function("multi_fn")
                int foo(int x, int y);
            }
            """;

        CompilationResult result = compile("test.MixedLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.MixedLib$Impl");
        assertNotNull("Generated MixedLib$Impl class not found", implClass);

        // Unique method keeps plain $mh suffix.
        assertEquals(MethodHandle.class, implClass.getDeclaredField("baz$mh").getType());

        // Overloaded methods get ordinal suffixes.
        assertEquals(MethodHandle.class, implClass.getDeclaredField("foo$0$mh").getType());
        assertEquals(MethodHandle.class, implClass.getDeclaredField("foo$1$mh").getType());

        try {
            implClass.getDeclaredField("foo$mh");
            fail("foo$mh must not exist when overloads are present");
        } catch (NoSuchFieldException expected) {
            // expected
        }
    }

    // -------------------------------------------------------------------------
    // @VectorSegment / @MatrixSegment — valid usage generates the correct class shape.
    // Note: these tests confirm compilation succeeds and the generated class/method shape is
    // correct, not that the emitted checks fire at runtime.
    // -------------------------------------------------------------------------

    public void testVectorSegmentGeneratesClass() throws Exception {
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
                    @VectorSegment(countParam = "length", elementBits = 8) MemorySegment a,
                    @VectorSegment(countParam = "length", elementBits = 8) MemorySegment b,
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

    public void testVectorSegmentSubByteElementBitsGeneratesClass() throws Exception {
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
                    @VectorSegment(countParam = "elementCount", elementBits = 8) MemorySegment b,
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
                    @VectorSegment(countParam = "count", elementBits = 64, aligned = true) MemorySegment addresses,
                    int count);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        Class<?> implClass = result.loadClassNoInit("test.MyLib$Impl");
        assertNotNull("Generated MyLib$Impl class not found", implClass);
        assertAssertionsDisabledFieldPresent(implClass);
    }

    public void testMatrixSegmentGeneratesClass() throws Exception {
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
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = 8) MemorySegment docs,
                    @VectorSegment(countParam = "length", elementBits = 8) MemorySegment query,
                    int length, int count,
                    @VectorSegment(countParam = "count", elementBits = 32) MemorySegment scores);
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

    public void testMatrixSegmentSubByteElementBitsGeneratesClass() throws Exception {
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
                    @VectorSegment(countParam = "length", elementBits = 8) MemorySegment query,
                    int length, int count,
                    @VectorSegment(countParam = "count", elementBits = 32) MemorySegment scores);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertNotNull("Generated MyLib$Impl class not found", result.loadClassNoInit("test.MyLib$Impl"));
    }

    public void testMatrixSegmentPaddingBytesGeneratesClass() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification(name = "testlib")
            public interface MyLib {
                @Function("dot_product_bulk_padded")
                void dotProductBulkPadded(
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = 8, paddingBytesParam = "padding")
                    MemorySegment docs,
                    @VectorSegment(countParam = "length", elementBits = 8) MemorySegment query,
                    int length, int count, int padding,
                    @VectorSegment(countParam = "count", elementBits = 32) MemorySegment scores);
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
                    @MatrixSegment(rowsParam = "count", colsParam = "length", elementBits = 32, aligned = true) MemorySegment m,
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

    // ---------------------------------------------------------------------------------------------
    // Behavioral tests: load the generated $Impl through the LibraryProvider SPI and invoke it for
    // real, proving the emitted upcall bytecode actually works end to end.
    // ---------------------------------------------------------------------------------------------

    /**
     * Drives a real {@code Arena}-scoped {@code @Upcall} stub through libc's {@code qsort}, proving native
     * code calling back into the JVM through the generated stub actually sorts the array.
     *
     * <p>qsort's comparator symbol is resolved via the CRT on Windows rather than the default linker
     * lookup, so this test is skipped there.
     */
    public void testQsortUpcallSortsArray() throws Throwable {
        if (System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows")) {
            return;
        }

        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("test.IntCompare", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.Upcall;
            @Upcall
            @FunctionalInterface
            public interface IntCompare {
                int compare(MemorySegment a, MemorySegment b);
            }
            """);
        sources.put("test.AscendingIntCompare", """
            package test;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.ValueLayout;
            public final class AscendingIntCompare implements IntCompare {
                public int compare(MemorySegment a, MemorySegment b) {
                    // qsort passes raw element pointers as zero-length MemorySegments; widen before reading.
                    int x = a.reinterpret(ValueLayout.JAVA_INT.byteSize()).get(ValueLayout.JAVA_INT, 0);
                    int y = b.reinterpret(ValueLayout.JAVA_INT.byteSize()).get(ValueLayout.JAVA_INT, 0);
                    return Integer.compare(x, y);
                }
            }
            """);
        sources.put("test.QsortLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification
            public interface QsortLib {
                @Function("qsort")
                void qsort(MemorySegment base, long nmemb, long size, IntCompare compar);
            }
            """);

        LoadedLibrary lib = loadLibrary(sources, "test.QsortLib");
        Object comparator = lib.newInstance("test.AscendingIntCompare");

        int[] unsorted = { 5, 3, 4, 1, 2 };
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment base = arena.allocate((long) unsorted.length * Integer.BYTES, Integer.BYTES);
            for (int i = 0; i < unsorted.length; i++) {
                base.setAtIndex(ValueLayout.JAVA_INT, i, unsorted[i]);
            }

            lib.call("qsort", base, (long) unsorted.length, (long) Integer.BYTES, comparator);

            int[] sorted = new int[unsorted.length];
            for (int i = 0; i < sorted.length; i++) {
                sorted[i] = base.getAtIndex(ValueLayout.JAVA_INT, i);
            }
            assertEquals("qsort must sort ascending via the upcall comparator", "[1, 2, 3, 4, 5]", Arrays.toString(sorted));
        }
    }

    /**
     * Cross-platform behavioral proof that the generated {@code @Upcall} marshaling works, without
     * depending on any specific native symbol (so unlike {@link #testQsortUpcallSortsArray} it also runs
     * on Windows). The generated {@code apply(IntCallback)} builds an FFM upcall stub from the Java
     * callback and passes its address to the downcall; a custom {@link org.elasticsearch.foreign.MethodHandleResolver}
     * stands in for the native function, receiving that stub, building a downcall onto it, and invoking
     * it with a fixed argument. If the emitted stub-creation code is correct the Java callback runs and
     * its result flows back out, so asserting on the returned value alone exercises the whole path with
     * no reflection into {@code $Impl}.
     */
    public void testUpcallStubInvokesJavaCallback() throws Throwable {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("test.CallbackLib", """
            package test;
            import java.lang.foreign.FunctionDescriptor;
            import java.lang.foreign.Linker;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.SymbolLookup;
            import java.lang.foreign.ValueLayout;
            import java.lang.invoke.MethodHandle;
            import java.lang.invoke.MethodHandles;
            import java.lang.invoke.MethodType;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.LinkerHelper;
            import org.elasticsearch.foreign.MethodHandleResolver;
            import org.elasticsearch.foreign.ResolvedSymbol;
            import org.elasticsearch.foreign.SymbolResolver;
            import org.elasticsearch.foreign.Upcall;
            @Upcall
            @FunctionalInterface
            interface IntCallback {
                int call(int x);
            }
            @LibrarySpecification(
                symbolResolver = CallbackLib.FakeSymbolResolver.class,
                methodHandleResolver = CallbackLib.StubInvokingResolver.class
            )
            public interface CallbackLib {
                @Function("apply")
                int apply(IntCallback cb);

                // Stands in for the native function. The generated apply() creates an upcall stub from the
                // Java callback and passes its address here; build a downcall onto that stub and call it
                // with a fixed argument, returning whatever the callback computed. Linking goes through
                // LinkerHelper so the restricted Linker call runs in the native-access-enabled module.
                static int invokeStub(MemorySegment stub) throws Throwable {
                    MethodHandle mh = LinkerHelper.downcallHandle(
                        stub,
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT)
                    );
                    return (int) mh.invokeExact(21);
                }

                class FakeSymbolResolver implements SymbolResolver {
                    public FakeSymbolResolver() {}
                    public ResolvedSymbol resolve(String name, SymbolLookup lookup) {
                        return new ResolvedSymbol(name, MemorySegment.ofAddress(1L));
                    }
                }

                class StubInvokingResolver implements MethodHandleResolver {
                    public StubInvokingResolver() {}
                    public MethodHandle resolve(ResolvedSymbol symbol, FunctionDescriptor descriptor,
                                                Linker linker, Linker.Option... options) {
                        try {
                            return MethodHandles.lookup()
                                .findStatic(CallbackLib.class, "invokeStub",
                                    MethodType.methodType(int.class, MemorySegment.class));
                        } catch (ReflectiveOperationException e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            }
            """);
        sources.put("test.Doubler", """
            package test;
            public final class Doubler implements IntCallback {
                public Doubler() {}
                public int call(int x) {
                    return x * 2;
                }
            }
            """);

        LoadedLibrary lib = loadLibrary(sources, "test.CallbackLib");
        Object callback = lib.newInstance("test.Doubler");
        assertEquals("the upcall stub must invoke the Java callback (21 doubled)", 42, (int) lib.call("apply", callback));
    }

    /**
     * Verifies that a nested {@code @Upcall} interface (declared inside the library interface) works
     * end-to-end. The bug was that {@link org.elasticsearch.foreign.processor.model.UpcallModel} used
     * {@code getQualifiedName()} (dot-separated
     * canonical name) when building the {@code ClassDesc} for the callback interface, which produced
     * {@code Outer.Inner} instead of the binary name {@code Outer$Inner} that {@code ClassDesc.of()}
     * requires, causing a {@code ClassNotFoundException} at class-init time.
     */
    public void testUpcallStubWithNestedCallbackInterfaceWorks() throws Throwable {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("test.CallbackLib", """
            package test;
            import java.lang.foreign.FunctionDescriptor;
            import java.lang.foreign.Linker;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.SymbolLookup;
            import java.lang.foreign.ValueLayout;
            import java.lang.invoke.MethodHandle;
            import java.lang.invoke.MethodHandles;
            import java.lang.invoke.MethodType;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.LinkerHelper;
            import org.elasticsearch.foreign.MethodHandleResolver;
            import org.elasticsearch.foreign.ResolvedSymbol;
            import org.elasticsearch.foreign.SymbolResolver;
            import org.elasticsearch.foreign.Upcall;
            @LibrarySpecification(
                symbolResolver = CallbackLib.FakeSymbolResolver.class,
                methodHandleResolver = CallbackLib.StubInvokingResolver.class
            )
            public interface CallbackLib {
                @Upcall
                @FunctionalInterface
                interface NestedCallback {
                    int call(int x);
                }

                @Function("apply")
                int apply(NestedCallback cb);

                static int invokeStub(MemorySegment stub) throws Throwable {
                    MethodHandle mh = LinkerHelper.downcallHandle(
                        stub,
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT)
                    );
                    return (int) mh.invokeExact(21);
                }

                class FakeSymbolResolver implements SymbolResolver {
                    public FakeSymbolResolver() {}
                    public ResolvedSymbol resolve(String name, SymbolLookup lookup) {
                        return new ResolvedSymbol(name, MemorySegment.ofAddress(1L));
                    }
                }

                class StubInvokingResolver implements MethodHandleResolver {
                    public StubInvokingResolver() {}
                    public MethodHandle resolve(ResolvedSymbol symbol, FunctionDescriptor descriptor,
                                                Linker linker, Linker.Option... options) {
                        try {
                            return MethodHandles.lookup()
                                .findStatic(CallbackLib.class, "invokeStub",
                                    MethodType.methodType(int.class, MemorySegment.class));
                        } catch (ReflectiveOperationException e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            }
            """);
        sources.put("test.Tripler", """
            package test;
            public final class Tripler implements CallbackLib.NestedCallback {
                public Tripler() {}
                public int call(int x) {
                    return x * 3;
                }
            }
            """);

        LoadedLibrary lib = loadLibrary(sources, "test.CallbackLib");
        Object callback = lib.newInstance("test.Tripler");
        assertEquals("nested @Upcall callback must be reachable via binary name (21 tripled)", 63, (int) lib.call("apply", callback));
    }
}
