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
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
        java.lang.reflect.Method method = implClass.getMethod("sandboxInit", String.class, long.class, MemorySegment.class);
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
     * Two overloaded Java methods binding to the same C symbol must generate two distinct
     * {@code MethodHandle} fields using the ordinal-suffix naming strategy:
     * {@code open$0$mh} and {@code open$1$mh}.
     */
    public void testOverloadedMethodsSameCSymbolGetDisambiguatedFields() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import java.lang.foreign.SymbolLookup;
            import org.elasticsearch.foreign.CaptureErrno;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.SymbolResolver;
            import org.elasticsearch.foreign.Variadic;
            @LibrarySpecification(symbolResolver = OpenLib.FakeResolver.class)
            public interface OpenLib {
                @CaptureErrno @Variadic(firstArg = 2) @Function("open")
                int open(String pathname, int flags);

                @CaptureErrno @Variadic(firstArg = 2) @Function("open")
                int open(String pathname, int flags, int mode);

                class FakeResolver implements SymbolResolver {
                    public FakeResolver() {}
                    public MemorySegment resolve(String name, SymbolLookup lookup) {
                        return MemorySegment.ofAddress(1L);
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
}
