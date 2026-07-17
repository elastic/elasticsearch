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
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tests that {@link ImplClassWriter} generates correct {@code $Impl} class files.
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
     * {@code MemorySegmentUtil.allocateString}, pass the resulting {@code MemorySegment} to
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
        java.lang.reflect.Method method = implClass.getMethod(
            "sandboxInit",
            String.class,
            long.class,
            java.lang.foreign.MemorySegment.class
        );
        assertEquals("sandboxInit must return int", int.class, method.getReturnType());
        assertEquals("first param must be String", String.class, method.getParameterTypes()[0]);
    }

    /**
     * A {@code @CaptureErrno @Function} method must generate a class WITHOUT a per-class
     * {@code errnoState} field — the shared {@code LinkerHelper.ERRNO_STATE} is used instead.
     * Also initializes the class to exercise the emitted {@code Linker.Option.captureCallState}
     * (and {@code firstVariadicArg} in the {@code @Variadic} case) bytecode against the real FFM
     * API, catching descriptor mismatches that {@code loadClassNoInit} would miss.
     *
     * <p>The custom {@link org.elasticsearch.foreign.SymbolResolver} returns a fake non-null
     * address so {@code linker.downcallHandle} succeeds at class-init time without needing a
     * real native symbol on the classpath. Any linkage error from the descriptor construction
     * (e.g. {@code captureCallState} declared as varargs but emitted as a single {@code String})
     * still surfaces here because it fires before {@code downcallHandle} is even called.
     */
    public void testCaptureErrnoAndVariadicInitializeAgainstFfmApi() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.SymbolLookup;
            import org.elasticsearch.foreign.CaptureErrno;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.SymbolResolver;
            import org.elasticsearch.foreign.Variadic;
            @LibrarySpecification(symbolResolver = ErrnoLib.FakeResolver.class)
            public interface ErrnoLib {
                @CaptureErrno
                @Function("foo")
                int foo(int x);

                @CaptureErrno
                @Variadic(firstArg = 1)
                @Function("bar")
                long bar(long a, int b);

                class FakeResolver implements SymbolResolver {
                    public FakeResolver() {}
                    public MemorySegment resolve(String name, SymbolLookup lookup) {
                        // downcallHandle validates the address is non-NULL; any positive value works.
                        return MemorySegment.ofAddress(1L);
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

        // Must NOT have a per-class errnoState field — the shared LinkerHelper.ERRNO_STATE is used.
        try {
            implClass.getDeclaredField("errnoState");
            fail("ErrnoLib$Impl must not have a per-class errnoState field");
        } catch (NoSuchFieldException expected) {
            // expected
        }

        // Both MethodHandle fields must exist.
        assertEquals(MethodHandle.class, implClass.getDeclaredField("foo$mh").getType());
        assertEquals(MethodHandle.class, implClass.getDeclaredField("bar$mh").getType());
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
}
