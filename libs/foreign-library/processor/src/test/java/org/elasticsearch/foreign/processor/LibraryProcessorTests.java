/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import java.lang.classfile.ClassFile;
import java.lang.reflect.AccessFlag;
import java.nio.file.Files;
import java.nio.file.Path;

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
     * The {@link org.elasticsearch.foreign.Critical.UnsupportedFallback} sentinel satisfies the required
     * {@code fallbackAdapter} without a real adapter class.
     */
    public void testUnsupportedFallbackSentinel() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Critical;
            @LibrarySpecification(name = "testlib")
            public interface GatedLib {
                @Function("native_fn")
                @Critical(fallbackAdapter = Critical.UnsupportedFallback.class)
                long fn(MemorySegment dst, long dstCap);
            }
            """;

        CompilationResult result = compile("test.GatedLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertNotNull(result.loadClassNoInit("test.GatedLib$Impl"));
    }

    /**
     * A {@code @LibrarySpecification} annotation on a non-abstract class should emit an error.
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

    /**
     * A {@code @LibrarySpecification} abstract class with a single {@code public abstract @Function}
     * method must compile successfully and produce a {@code $Impl} class file.
     */
    public void testAbstractClassGeneratesImpl() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib")
            public abstract class AbstractLib {
                @Function("native_add")
                public abstract int add(int a, int b);
            }
            """;

        CompilationResult result = compile("test.AbstractLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertTrue(
            "Expected AbstractLib$Impl.class to be generated",
            Files.exists(result.outputDir().resolve("test/AbstractLib$Impl.class"))
        );
    }

    /**
     * A {@code @LibrarySpecification} abstract class lacking a no-arg constructor must emit a clear
     * compile error.
     */
    public void testAbstractClassWithoutNoArgConstructorEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib")
            public abstract class NoDefaultCtorLib {
                public NoDefaultCtorLib(int x) {}

                @Function("native_fn")
                public abstract int fn(int x);
            }
            """;

        CompilationResult result = compile("test.NoDefaultCtorLib", source);

        assertFalse("Expected compilation to fail due to missing no-arg constructor", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("no-arg constructor"));
        assertTrue("Expected error about missing no-arg constructor but got: " + result.errors(), hasError);
    }

    /**
     * A {@code protected abstract @Function} method on an abstract-class spec must compile
     * successfully and the generated {@code $Impl} must emit the method with {@code ACC_PROTECTED}
     * (not {@code ACC_PUBLIC}). Verified by parsing the bytecode directly, since loading the
     * {@code $Impl} class requires the Step 3 superclass change to be in place.
     */
    public void testProtectedAbstractMethodIsProtectedInImpl() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib")
            public abstract class ProtectedLib {
                @Function("native_fn")
                protected abstract int fn(int x);
            }
            """;

        CompilationResult result = compile("test.ProtectedLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Path classFile = result.outputDir().resolve("test/ProtectedLib$Impl.class");
        assertTrue("Generated ProtectedLib$Impl.class not found", Files.exists(classFile));

        // Parse the bytecode and verify the 'fn' method carries ACC_PROTECTED (not ACC_PUBLIC)
        var cm = ClassFile.of().parse(Files.readAllBytes(classFile));
        var fnMethod = cm.methods().stream().filter(m -> m.methodName().equalsString("fn")).findFirst();
        assertTrue("fn method not found in ProtectedLib$Impl bytecode", fnMethod.isPresent());
        assertTrue("fn method must have ACC_PROTECTED in generated $Impl", fnMethod.get().flags().has(AccessFlag.PROTECTED));
        assertFalse("fn method must not have ACC_PUBLIC in generated $Impl", fnMethod.get().flags().has(AccessFlag.PUBLIC));
    }

    /**
     * A library with {@code unavailableOn} listing a single platform compiles cleanly (no warning).
     */
    public void testUnavailableOnSinglePlatformCompilesClean() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Platform;
            @LibrarySpecification(name = "testlib", unavailableOn = { Platform.WINDOWS_X64 })
            public interface MyLib {
                @Function("native_fn")
                int fn(int x);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        boolean hasAllPlatformsWarning = result.warnings().stream().anyMatch(msg -> msg.contains("never be natively loaded"));
        assertFalse("Unexpected all-platforms warning for single-platform unavailableOn", hasAllPlatformsWarning);
    }

    /**
     * A library with {@code unavailableOn} listing an empty array compiles cleanly (no warning).
     */
    public void testUnavailableOnEmptyCompilesClean() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            @LibrarySpecification(name = "testlib", unavailableOn = {})
            public interface MyLib {
                @Function("native_fn")
                int fn(int x);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        boolean hasAllPlatformsWarning = result.warnings().stream().anyMatch(msg -> msg.contains("never be natively loaded"));
        assertFalse("Unexpected all-platforms warning for empty unavailableOn", hasAllPlatformsWarning);
    }

    /**
     * A library with {@code unavailableOn} listing all five platforms must fail compilation — a library
     * that can never load natively is a mistake, not a valid configuration.
     */
    public void testUnavailableOnAllPlatformsFailsCompilation() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Platform;
            @LibrarySpecification(
                name = "testlib",
                unavailableOn = {
                    Platform.LINUX_X64,
                    Platform.LINUX_AARCH64,
                    Platform.DARWIN_X64,
                    Platform.DARWIN_AARCH64,
                    Platform.WINDOWS_X64
                }
            )
            public interface MyLib {
                @Function("native_fn")
                int fn(int x);
            }
            """;

        CompilationResult result = compile("test.MyLib", source);

        assertFalse("Expected compilation to fail when all platforms are listed in unavailableOn", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("never be natively loaded"));
        assertTrue("Expected error about all platforms listed but got: " + result.errors(), hasError);
    }

    /**
     * A {@code @StructSpecification} interface that does NOT declare {@code extends Addressable}
     * must compile cleanly — the processor no longer requires it.
     */
    public void testStructInterfaceWithoutAddressableCompilesClean() {
        String source = """
            package test;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface CleanStructLib {
                @StructSpecification
                interface RLimit {
                    long rlim_cur();
                    long rlim_max();
                }

                @Function("getrlimit")
                int getrlimit(int resource, RLimit rlimit);
            }
            """;

        CompilationResult result = compile("test.CleanStructLib", source);
        assertTrue("Expected compilation to succeed without extends Addressable but got errors: " + result.errors(), result.success());
        assertTrue("Expected no processor errors", result.errors().isEmpty());
    }

    /**
     * A {@code @StructSpecification} interface with an {@code @InlineArrayField} getter-only method
     * must compile cleanly and produce a model.
     */
    public void testInlineArrayFieldGetterOnlyCompilesClean() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.InlineArrayField;
            @LibrarySpecification
            public interface InlineArrayLib {
                @StructSpecification
                interface SockAddr {
                    short sa_family();
                    @InlineArrayField(length = 14)
                    byte sa_data(int index);
                }
            }
            """;

        CompilationResult result = compile("test.InlineArrayLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertTrue("Expected no processor errors", result.errors().isEmpty());
    }

    /**
     * A {@code @StructSpecification} interface with an {@code @InlineArrayField} getter and setter pair
     * must compile cleanly.
     */
    public void testInlineArrayFieldGetterSetterCompilesClean() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.InlineArrayField;
            @LibrarySpecification
            public interface InlineArrayLib {
                @StructSpecification
                interface SunPath {
                    @InlineArrayField(length = 108)
                    byte sun_path(int index);
                    @InlineArrayField(length = 108)
                    void sun_path(int index, byte value);
                }
            }
            """;

        CompilationResult result = compile("test.InlineArrayLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertTrue("Expected no processor errors", result.errors().isEmpty());
    }

    /**
     * A {@code @StructSpecification} interface with an {@code @InlineStringField} getter-only method
     * must compile cleanly.
     */
    public void testInlineStringFieldGetterOnlyCompilesClean() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.InlineStringField;
            @LibrarySpecification
            public interface InlineStringLib {
                @StructSpecification
                interface UnixAddr {
                    short sa_family();
                    @InlineStringField(length = 108)
                    String sun_path();
                }
            }
            """;

        CompilationResult result = compile("test.InlineStringLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertTrue("Expected no processor errors", result.errors().isEmpty());
    }

    /**
     * A {@code @StructSpecification} interface with an {@code @InlineStringField} getter and setter
     * must compile cleanly.
     */
    public void testInlineStringFieldGetterSetterCompilesClean() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.InlineStringField;
            @LibrarySpecification
            public interface InlineStringLib {
                @StructSpecification
                interface UnixAddr {
                    short sa_family();
                    @InlineStringField(length = 108)
                    String sun_path();
                    @InlineStringField(length = 108)
                    void sun_path(String value);
                }
            }
            """;

        CompilationResult result = compile("test.InlineStringLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        assertTrue("Expected no processor errors", result.errors().isEmpty());
    }

    /**
     * {@code @InlineArrayField} with a non-positive {@code length} must emit an error.
     */
    public void testInlineArrayFieldNonPositiveLengthEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.InlineArrayField;
            @LibrarySpecification
            public interface BadLib {
                @StructSpecification
                interface BadStruct {
                    @InlineArrayField(length = 0)
                    byte data(int index);
                }
            }
            """;

        CompilationResult result = compile("test.BadLib", source);
        assertFalse("Expected compilation to fail with zero length", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("positive length"));
        assertTrue("Expected error about positive length but got: " + result.errors(), hasError);
    }

    /**
     * {@code @InlineArrayField} applied to a non-primitive return type must emit an error.
     */
    public void testInlineArrayFieldNonPrimitiveReturnTypeEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.InlineArrayField;
            @LibrarySpecification
            public interface BadLib {
                @StructSpecification
                interface BadStruct {
                    @InlineArrayField(length = 4)
                    String data(int index);
                }
            }
            """;

        CompilationResult result = compile("test.BadLib", source);
        assertFalse("Expected compilation to fail with non-primitive return type", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("primitive type"));
        assertTrue("Expected error about primitive type but got: " + result.errors(), hasError);
    }

    /**
     * {@code @InlineStringField} with a non-String return type must emit an error.
     */
    public void testInlineStringFieldNonStringReturnTypeEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.InlineStringField;
            @LibrarySpecification
            public interface BadLib {
                @StructSpecification
                interface BadStruct {
                    @InlineStringField(length = 16)
                    int notAString();
                }
            }
            """;

        CompilationResult result = compile("test.BadLib", source);
        assertFalse("Expected compilation to fail with non-String return type", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("must return String"));
        assertTrue("Expected error about String return type but got: " + result.errors(), hasError);
    }

    /**
     * A method with both {@code @InlineArrayField} and {@code @InlineStringField} must emit an error
     * (mixing annotations is forbidden).
     */
    public void testMixingInlineAnnotationsEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.InlineArrayField;
            import org.elasticsearch.foreign.InlineStringField;
            @LibrarySpecification
            public interface BadLib {
                @StructSpecification
                interface BadStruct {
                    @InlineArrayField(length = 4)
                    @InlineStringField(length = 4)
                    byte data(int index);
                }
            }
            """;

        CompilationResult result = compile("test.BadLib", source);
        assertFalse("Expected compilation to fail when mixing inline annotations", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("more than one"));
        assertTrue("Expected error about mixing annotations but got: " + result.errors(), hasError);
    }

    /**
     * An {@code @InlineArrayField} getter/setter pair with mismatched lengths must emit an error.
     */
    public void testInlineArrayFieldMismatchedLengthEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.InlineArrayField;
            @LibrarySpecification
            public interface BadLib {
                @StructSpecification
                interface BadStruct {
                    @InlineArrayField(length = 4)
                    byte data(int index);
                    @InlineArrayField(length = 8)
                    void data(int index, byte value);
                }
            }
            """;

        CompilationResult result = compile("test.BadLib", source);
        assertFalse("Expected compilation to fail with mismatched lengths", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("mismatched lengths"));
        assertTrue("Expected error about mismatched lengths but got: " + result.errors(), hasError);
    }

    /**
     * A getter and setter for the same scalar field that are separated by another field declaration
     * must emit an error. Non-adjacent pairs would silently reorder struct fields, breaking the
     * native memory layout.
     */
    public void testNonAdjacentGetterSetterEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface BadLib {
                @StructSpecification
                interface Mixed {
                    int a();
                    int b();
                    void a(int v);
                }
            }
            """;

        CompilationResult result = compile("test.BadLib", source);
        assertFalse("Expected compilation to fail when getter and setter are not adjacent", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("adjacent"));
        assertTrue("Expected error about non-adjacent getter/setter but got: " + result.errors(), hasError);
    }

    /**
     * The same adjacency rule applies to {@code @InlineArrayField}: getter and setter must be
     * declared next to each other.
     */
    public void testNonAdjacentInlineArrayFieldEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.InlineArrayField;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface BadLib {
                @StructSpecification
                interface Mixed {
                    @InlineArrayField(length = 4)
                    byte data(int index);
                    short other();
                    @InlineArrayField(length = 4)
                    void data(int index, byte value);
                }
            }
            """;

        CompilationResult result = compile("test.BadLib", source);
        assertFalse("Expected compilation to fail when @InlineArrayField getter/setter are not adjacent", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("adjacent"));
        assertTrue("Expected error about non-adjacent @InlineArrayField but got: " + result.errors(), hasError);
    }
}
