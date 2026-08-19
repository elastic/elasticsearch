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
 * <p>The behavioral tests bind to ubiquitous libc symbols reachable through the default linker lookup —
 * {@code memcmp} (pure/read-only, alignment-agnostic, on every platform) and POSIX {@code qsort} (for
 * {@code @Upcall} callbacks) — so they need no native library build dependency.
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
     * {@code MemorySegmentAdapter.allocateString}, pass the resulting {@code MemorySegment} to
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
        // Arena.ofAuto() -> ArenaAdapter.allocate(arena, layout, count) -> per-element Pack.pack loop
        // -> len$vh.set / elem$ptr$vh.set. This is the assertion that would catch, for example, an
        // Arena.allocate(MemoryLayout, long) direct call (JDK 22+ only) instead of going through
        // ArenaAdapter, or any similar cross-JDK signature mismatch in the emitted invokestatic/
        // invokeinterface descriptors.
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
     * via {@code MemorySegmentAdapter.getString/setString}. A write-then-read must return the
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
     * that calls the charset-aware {@code MemorySegmentAdapter.allocateString(Arena, String, Charset)}
     * overload rather than the plain 2-arg {@code allocateString(Arena, String)} used for ordinary
     * (UTF-8) {@code String} parameters. Verified structurally by parsing the generated bytecode.
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
            .filter(i -> i.name().equalsString("allocateString"))
            .toList();
        assertEquals("Expected exactly one allocateString call in op", 1, allocateStringCalls.size());
        assertEquals(
            "@WideString param must use the 3-arg charset-aware allocateString overload",
            3,
            allocateStringCalls.get(0).typeSymbol().parameterCount()
        );
    }

    /**
     * A {@code @Function} method with a mix of ordinary {@code String} and {@code @WideString}
     * parameters must route each to the correct {@code MemorySegmentAdapter.allocateString} overload:
     * the plain 2-arg form for UTF-8 and the 3-arg charset-aware form for UTF-16LE, in parameter order.
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
            .filter(i -> i.name().equalsString("allocateString"))
            .toList();
        assertEquals("Expected two allocateString calls in op (one per String param)", 2, allocateStringCalls.size());
        assertEquals(
            "Narrow (UTF-8) param must use the 2-arg allocateString overload",
            2,
            allocateStringCalls.get(0).typeSymbol().parameterCount()
        );
        assertEquals(
            "@WideString param must use the 3-arg charset-aware allocateString overload",
            3,
            allocateStringCalls.get(1).typeSymbol().parameterCount()
        );
    }

    /**
     * A struct with a {@code @InlineStringField(length = 16, wide = true)} getter+setter pair must
     * round-trip a non-ASCII string through real bytecode execution, proving the codegen calls the
     * UTF-16LE-aware {@code MemorySegmentAdapter} overloads (whichever JDK 21 or JDK 22+ branch is
     * active for the running JVM).
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
     * {@code allocateString}, which would throw {@link NullPointerException}. The generated
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
     * A {@code @Function} method with an {@code Arena} parameter and a parameter of an
     * {@code @Upcall}-typed callback interface must generate static {@code $upcallFd} /
     * {@code $upcallMh} fields (keyed by the callback's parameter index) and a method body that
     * installs the stub via {@code LinkerHelper.upcallStub(...)} before the downcall. Verified
     * structurally via {@code loadClassNoInit} — building the downcall {@code MethodHandle} in
     * {@code <clinit>} would require a real native symbol, but the upcall fields and bytecode
     * shape can be asserted without initializing the class.
     */
    public void testUpcallParamGeneratesStubCall() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Upcall;
            @Upcall
            @FunctionalInterface
            interface IntCallback {
                int call(int x);
            }
            @LibrarySpecification(name = "testlib")
            public interface CallbackLib {
                @Function("native_fn")
                void fn(IntCallback cb);
            }
            """;

        CompilationResult result = compile("test.CallbackLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClassNoInit("test.CallbackLib$Impl");
        assertNotNull("Generated CallbackLib$Impl class not found", implClass);

        // The callback is the first parameter (index 0), so the field names are keyed "upcall0".
        java.lang.reflect.Field fdField = implClass.getDeclaredField("fn$upcall0Fd");
        assertEquals("fn$upcall0Fd must be a FunctionDescriptor", java.lang.foreign.FunctionDescriptor.class, fdField.getType());
        assertTrue("fn$upcall0Fd must be static", Modifier.isStatic(fdField.getModifiers()));

        java.lang.reflect.Field mhField = implClass.getDeclaredField("fn$upcall0Mh");
        assertEquals("fn$upcall0Mh must be a MethodHandle", MethodHandle.class, mhField.getType());
        assertTrue("fn$upcall0Mh must be static", Modifier.isStatic(mhField.getModifiers()));

        // The generated method keeps the real Java signature: (IntCallback), not (MemorySegment).
        // Load IntCallback through implClass's own classloader so the Class objects are comparable.
        Class<?> callbackClass = Class.forName("test.IntCallback", false, implClass.getClassLoader());
        java.lang.reflect.Method method = implClass.getMethod("fn", callbackClass);
        assertEquals("fn must return void", void.class, method.getReturnType());

        // The method body must install the stub via LinkerHelper.upcallStub before the downcall.
        Path classFile = result.outputDir().resolve("test/CallbackLib$Impl.class");
        assertTrue("Generated CallbackLib$Impl.class not found", Files.exists(classFile));
        byte[] bytes = Files.readAllBytes(classFile);
        var cm = ClassFile.of().parse(bytes);
        boolean hasUpcallStubCall = cm.methods()
            .stream()
            .filter(m -> m.methodName().equalsString("fn"))
            .flatMap(m -> m.code().stream())
            .flatMap(ca -> ca.elementStream())
            .anyMatch(
                e -> e instanceof InvokeInstruction ii
                    && ii.opcode() == Opcode.INVOKESTATIC
                    && ii.owner().asInternalName().equals("org/elasticsearch/foreign/LinkerHelper")
                    && ii.name().equalsString("upcallStub")
            );
        assertTrue("Generated fn body must invoke LinkerHelper.upcallStub", hasUpcallStubCall);
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
    // real, proving the emitted downcall/upcall/bounds-check bytecode actually works end to end.
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
     * A {@code @Critical} binding with a real fallback adapter links and calls {@code memcmp} correctly.
     */
    public void testCriticalFallbackAdapterCall() throws Throwable {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("test.MemCmpAdapter", """
            package test;
            import java.lang.foreign.MemorySegment;
            import java.lang.invoke.MethodHandle;
            public final class MemCmpAdapter {
                public static int memcmp(MethodHandle mh, MemorySegment a, MemorySegment b, long n) throws Throwable {
                    return (int) mh.invokeExact(a, b, n);
                }
            }
            """);
        sources.put("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Critical;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                @Critical(fallbackAdapter = MemCmpAdapter.class)
                int memcmp(MemorySegment a, MemorySegment b, long n);
            }
            """);

        LoadedLibrary lib = loadLibrary(sources, "test.MemCmpLib");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment a = fill(arena.allocate(4));
            MemorySegment b = fill(arena.allocate(4));
            assertEquals("equal 4-byte segments compare equal", 0, (int) lib.call("memcmp", a, b, 4L));
        }
    }

    /**
     * The {@code Critical.UnsupportedFallback} sentinel behaves differently per JDK, but these tests
     * always run on JDK &gt;= 25 (they compile snippets and run our processor, which requires JDK 24+), so
     * this asserts the JDK 22+ behavior: the binding is a normal critical call with heap segment support,
     * so passing heap-backed {@link MemorySegment}s links and calls {@code memcmp} correctly.
     */
    public void testCriticalUnsupportedFallbackSentinel() throws Throwable {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Critical;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
                int memcmp(MemorySegment a, MemorySegment b, long n);
            }
            """;

        LoadedLibrary lib = loadLibrary("test.MemCmpLib", source);
        MemorySegment a = MemorySegment.ofArray(new byte[] { 0, 1, 2, 3 });
        MemorySegment b = MemorySegment.ofArray(new byte[] { 0, 1, 2, 3 });
        assertEquals("equal 4-byte heap segments compare equal", 0, (int) lib.call("memcmp", a, b, 4L));
    }

    /**
     * Proves the emitted {@code Objects.checkFromIndexSize} call really does throw on an undersized
     * segment, and really does let a correctly-sized call through to the native function.
     */
    public void testVectorSegmentCheckThrowsOnUndersizedSegment() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @VectorSegment(countParam = "n", elementBits = 8) MemorySegment a,
                    @VectorSegment(countParam = "n", elementBits = 8) MemorySegment b,
                    long n);
            }
            """);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment good = arena.allocate(8);
            MemorySegment tooSmall = arena.allocate(4);
            lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", tooSmall, good, 8L);

            MemorySegment a = fill(arena.allocate(8));
            MemorySegment b = fill(arena.allocate(8));
            assertEquals(0, (int) lib.call("memcmp", a, b, 8L));
        }
    }

    /**
     * Proves the {@code aligned} attribute's emitted assert really does fire on a misaligned off-heap
     * segment (the test JVM runs with {@code -ea}), and really doesn't on an aligned one.
     */
    public void testAlignedVectorSegmentThrowsOnMisalignedSegment() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @VectorSegment(countParam = "n", elementBits = 64, aligned = true) MemorySegment a,
                    @VectorSegment(countParam = "n", elementBits = 64) MemorySegment b,
                    long n);
            }
            """);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(16, 8);
            MemorySegment misaligned = buf.asSlice(1, 8);
            MemorySegment aligned = buf.asSlice(0, 8);

            lib.expectThrows(AssertionError.class, "memcmp", misaligned, aligned, 1L);
            lib.call("memcmp", aligned, aligned, 1L);
        }
    }

    /**
     * Same idea as {@link #testVectorSegmentCheckThrowsOnUndersizedSegment}, for {@code @MatrixSegment}.
     * {@code rowsParam}/{@code colsParam} both reference the same parameter {@code n} to describe an n*n
     * "matrix", so the method's real arity stays at {@code (a, b, n)}, matching {@code memcmp}'s
     * actual signature.
     */
    public void testMatrixSegmentCheckThrowsOnUndersizedSegment() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8) MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8) MemorySegment b,
                    long n);
            }
            """);

        // rows = cols = n = 4, so each segment needs n*n = 16 bytes; memcmp itself only reads n=4.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment good = arena.allocate(16);
            MemorySegment tooSmall = arena.allocate(8);
            lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", tooSmall, good, 4L);

            MemorySegment a = fill(arena.allocate(16), 4);
            MemorySegment b = fill(arena.allocate(16), 4);
            assertEquals(0, (int) lib.call("memcmp", a, b, 4L));
        }
    }

    /**
     * Same idea as {@link #testAlignedVectorSegmentThrowsOnMisalignedSegment}, for
     * {@code @MatrixSegment}.
     */
    public void testAlignedMatrixSegmentThrowsOnMisalignedSegment() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 64, aligned = true) MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 64) MemorySegment b,
                    long n);
            }
            """);

        // rows = cols = n = 1 -> requires 1*1*64/8 = 8 bytes (elementBits=64), 8-byte aligned.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(16, 8);
            MemorySegment misaligned = buf.asSlice(1, 8);
            MemorySegment aligned = buf.asSlice(0, 8);

            lib.expectThrows(AssertionError.class, "memcmp", misaligned, aligned, 1L);
            lib.call("memcmp", aligned, aligned, 1L);
        }
    }

    /**
     * The {@code paddingBytesParam} size math. All attributes ({@code rowsParam}/{@code colsParam}/
     * {@code paddingBytesParam}) reference the same parameter {@code n}, so the method's real arity
     * stays at {@code (a, b, n)}, matching {@code memcmp}'s actual signature exactly.
     * With {@code elementBits = 8}, {@code rowBytes = n*8/8 + n = 2n}, so the required size is
     * {@code rows * rowBytes = n * 2n = 2n²} — twice the packed (no-padding) size {@code n²}. This
     * proves the padding is added to each row rather than ignored (a regression here would accept a
     * segment only {@code n²} bytes long).
     */
    public void testMatrixSegmentPaddingBytesAddsToRowSize() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8, paddingBytesParam = "n")
                    MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8, paddingBytesParam = "n")
                    MemorySegment b,
                    long n);
            }
            """);

        // n = 4 -> rowBytes = 2*4 = 8, size = 4*8 = 32 bytes required; the packed size would be only 16.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment good = arena.allocate(32);
            MemorySegment tooSmall = arena.allocate(16);
            lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", tooSmall, good, 4L);

            MemorySegment a = fill(arena.allocate(32), 4);
            MemorySegment b = fill(arena.allocate(32), 4);
            assertEquals(0, (int) lib.call("memcmp", a, b, 4L));
        }
    }

    /**
     * The {@code paddingBytesParam} guard rejects a negative padding value with an
     * {@code IllegalArgumentException}, thrown before any size check or native call. All attributes
     * reference the same {@code n}, so passing {@code n = -1} drives the padding negative and the
     * emitted {@code if (paddingBytes < 0) throw} fires.
     */
    public void testMatrixSegmentNegativePaddingThrows() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8, paddingBytesParam = "n")
                    MemorySegment a,
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 8, paddingBytesParam = "n")
                    MemorySegment b,
                    long n);
            }
            """);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment a = arena.allocate(64);
            MemorySegment b = arena.allocate(64);
            lib.expectThrows(IllegalArgumentException.class, "memcmp", a, b, -1L);
        }
    }

    /**
     * Verifies a sub-byte {@code elementBits} check rounds each row's byte size <em>up</em> to whole
     * bytes: a 2D segment is {@code rows} independently packed vectors, and each vector must have room
     * for all its bits.
     */
    public void testMatrixSegmentSubByteRowSizeRoundsUpToWholeBytes() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.MatrixSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @MatrixSegment(rowsParam = "n", colsParam = "n", elementBits = 4) MemorySegment a,
                    MemorySegment b,
                    long n);
            }
            """);

        // rows = cols = n = 3, elementBits = 4: each row is 3*4 = 12 bits -> ceil(12/8) = 2 bytes, so the
        // required size is rows * 2 = 6 bytes. A 5-byte segment must be rejected.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment b = fill(arena.allocate(6), 3);
            MemorySegment tooSmall = arena.allocate(5);
            lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", tooSmall, b, 3L);

            MemorySegment a = fill(arena.allocate(6), 3);
            assertEquals(0, (int) lib.call("memcmp", a, b, 3L));
        }
    }

    /**
     * Same rounding-up rule as {@link #testMatrixSegmentSubByteRowSizeRoundsUpToWholeBytes}, for a 1D
     * {@code @VectorSegment}: a sub-byte packed vector's byte size is {@code ceil(count*elementBits/8)},
     * so a segment sized to the floored value is rejected.
     */
    public void testVectorSegmentSubByteSizeRoundsUpToWholeBytes() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @VectorSegment(countParam = "n", elementBits = 4) MemorySegment a,
                    MemorySegment b,
                    long n);
            }
            """);

        // n = 3, elementBits = 4: 3*4 = 12 bits -> ceil(12/8) = 2 bytes required. A 1-byte segment must be
        // rejected even though a floored size (12/8 = 1) would accept it. The check throws before memcmp runs.
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment b = fill(arena.allocate(3), 3);
            MemorySegment tooSmall = arena.allocate(1);
            lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", tooSmall, b, 3L);

            MemorySegment a = fill(arena.allocate(3), 3);
            assertEquals(0, (int) lib.call("memcmp", a, b, 3L));
        }
    }

    /**
     * A negative element count must be rejected. Rounding the bit count up to whole bytes with
     * {@code (bits + 7) / 8} would truncate a small negative product toward zero -- for
     * {@code count = -1, elementBits = 8}, {@code (-8 + 7) / 8 == 0} -- and the check would pass a
     * negative count straight through to the native call.
     */
    public void testVectorSegmentRejectsNegativeCount() throws Throwable {
        LoadedLibrary lib = loadLibrary("test.MemCmpLib", """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.VectorSegment;
            @LibrarySpecification
            public interface MemCmpLib {
                @Function("memcmp")
                int memcmp(
                    @VectorSegment(countParam = "n", elementBits = 8) MemorySegment a,
                    MemorySegment b,
                    long n);
            }
            """);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment a = arena.allocate(3);
            MemorySegment b = arena.allocate(3);
            // -1 is the value that survives the (bits + 7) / 8 form; -2 fails under both forms.
            for (long n : new long[] { -1L, -2L }) {
                lib.expectThrows(IndexOutOfBoundsException.class, "memcmp", a, b, n);
            }
        }
    }

    /** Fills the whole segment with ascending byte values and returns it. */
    private static MemorySegment fill(MemorySegment segment) {
        return fill(segment, (int) segment.byteSize());
    }

    /** Fills the first {@code count} bytes of the segment with ascending byte values and returns it. */
    private static MemorySegment fill(MemorySegment segment, int count) {
        for (int i = 0; i < count; i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, (byte) i);
        }
        return segment;
    }
}
