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
import org.elasticsearch.foreign.Platform;

import java.lang.foreign.MemoryLayout;

/**
 * Tests for dense (natural-aligned) and sparse ({@code @Offset} + {@code @StructSize}) struct
 * modes: annotation-processor diagnostics for invalid combinations, plus behavioral tests that
 * verify the generated {@code $Impl} class has the correct {@code LAYOUT} size and field offsets at
 * runtime.
 */
@SuppressForbidden(
    reason = "behavioral tests verify static fields of processor-generated classes; getDeclaredField is the only way to access them"
)
public class StructLayoutTests extends ProcessorTestCase {

    /**
     * A sparse struct with {@code @Offset} on every field and {@code @StructSize} should compile.
     */
    public void testSparseWithOffsetAndStructSizeCompiles() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification(sparse = true)
                @StructSize(144)
                interface Stat64 {
                    @Offset(48)
                    long stSize();
                    void stSize(long v);
                    @Offset(56)
                    long stBlocks();
                    void stBlocks(long v);
                }
                @Function("stat64")
                int stat64(MemorySegment path, Stat64 stat);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);

        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
    }

    /**
     * {@code @Offset} on a field in dense mode is a compile error.
     */
    public void testDenseWithOffsetEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification
                interface MyStruct {
                    @Offset(8)
                    long field();
                    void field(long v);
                }
                @Function("fn")
                int fn(MemorySegment p, MyStruct s);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);

        assertFalse("Expected compilation to fail due to @Offset in dense mode", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("@Offset") && msg.contains("dense"));
        assertTrue("Expected error about @Offset in dense mode but got: " + result.errors(), hasError);
    }

    /**
     * {@code @StructSize} on a struct in dense mode is a compile error.
     */
    public void testDenseWithStructSizeEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification
                @StructSize(32)
                interface MyStruct {
                    long field();
                    void field(long v);
                }
                @Function("fn")
                int fn(MemorySegment p, MyStruct s);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);

        assertFalse("Expected compilation to fail due to @StructSize in dense mode", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("@StructSize") && msg.contains("dense"));
        assertTrue("Expected error about @StructSize in dense mode but got: " + result.errors(), hasError);
    }

    /**
     * A sparse struct missing {@code @Offset} on a field should emit a clear error.
     */
    public void testSparseFieldMissingOffsetEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification(sparse = true)
                @StructSize(144)
                interface Stat64 {
                    @Offset(48)
                    long stSize();
                    void stSize(long v);
                    long stBlocks();
                    void stBlocks(long v);
                }
                @Function("stat64")
                int stat64(MemorySegment path, Stat64 stat);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);

        assertFalse("Expected compilation to fail due to missing @Offset", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("stBlocks") && msg.contains("@Offset"));
        assertTrue("Expected error about missing @Offset on 'stBlocks' but got: " + result.errors(), hasError);
    }

    /**
     * A sparse struct missing {@code @StructSize} should emit a clear error.
     */
    public void testSparseMissingStructSizeEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification(sparse = true)
                interface Stat64 {
                    @Offset(48)
                    long stSize();
                    void stSize(long v);
                }
                @Function("stat64")
                int stat64(MemorySegment path, Stat64 stat);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);

        assertFalse("Expected compilation to fail due to missing @StructSize", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("@StructSize"));
        assertTrue("Expected error about missing @StructSize but got: " + result.errors(), hasError);
    }

    /**
     * A sparse struct with {@code @Offset} values declared out of order (second field's offset is
     * before the end of the first field) should emit a clear overlap error.
     */
    public void testSparseOutOfOrderOffsetsEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification(sparse = true)
                @StructSize(144)
                interface Stat64 {
                    @Offset(56)
                    long stBlocks();
                    void stBlocks(long v);
                    @Offset(48)
                    long stSize();
                    void stSize(long v);
                }
                @Function("stat64")
                int stat64(MemorySegment path, Stat64 stat);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);

        assertFalse("Expected compilation to fail due to overlapping @Offset", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("overlaps"));
        assertTrue("Expected overlap error but got: " + result.errors(), hasError);
    }

    /**
     * A sparse struct whose {@code @StructSize} is smaller than the space required by its fields
     * should emit a clear error.
     */
    public void testSparseStructSizeTooSmallEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification(sparse = true)
                @StructSize(60)
                interface Stat64 {
                    @Offset(48)
                    long stSize();
                    void stSize(long v);
                    @Offset(56)
                    long stBlocks();
                    void stBlocks(long v);
                }
                @Function("stat64")
                int stat64(MemorySegment path, Stat64 stat);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);

        assertFalse("Expected compilation to fail due to @StructSize too small", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("@StructSize") && msg.contains("smaller"));
        assertTrue("Expected @StructSize too-small error but got: " + result.errors(), hasError);
    }

    /**
     * A library unavailable on some platforms only needs @Offset to resolve for the supported ones.
     */
    public void testOffsetResolvesForSupportedPlatformsOnly() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Platform;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification(unavailableOn = {Platform.WINDOWS_X64})
            public interface TestLib {
                @StructSpecification(sparse = true)
                @StructSize(144)
                interface Stat64 {
                    @Offset(value = 48, platforms = {Platform.LINUX_X64, Platform.LINUX_AARCH64,
                                                     Platform.DARWIN_X64, Platform.DARWIN_AARCH64})
                    long stSize();
                    void stSize(long v);
                }
                @Function("stat64")
                int stat64(MemorySegment path, Stat64 stat);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);

        assertTrue("Expected compilation to succeed (WINDOWS_X64 not needed) but got errors: " + result.errors(), result.success());
    }

    /**
     * A dense struct with no layout annotations should produce a layout sized by C natural alignment.
     * long a (8 bytes, offset 0) + int b (4 bytes, offset 8, naturally aligned to 4) = 12 bytes.
     */
    public void testDenseNaturalAlignmentLayout() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification
                interface Dense {
                    long a();
                    void a(long v);
                    int b();
                    void b(int v);
                }
                @Function("fn")
                int fn(MemorySegment p, Dense s);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.TestLib$Dense$Impl");
        assertNotNull("Dense$Impl class not found", implClass);

        var layoutField = implClass.getDeclaredField("LAYOUT");
        layoutField.setAccessible(true);
        MemoryLayout layout = (MemoryLayout) layoutField.get(null);
        // long a (8) + int b (4, naturally aligned at offset 8) = 12 bytes
        assertEquals("Natural alignment layout size", 12L, layout.byteSize());
    }

    /**
     * A dense struct inserts C natural-alignment padding automatically: {@code int a} (4 bytes) then
     * {@code long b} places {@code b} at offset 8 (after 4 bytes of padding), for a total of 16 bytes.
     */
    public void testDenseAutoAlignmentInsertsPadding() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification
                interface Dense {
                    int a();
                    void a(int v);
                    long b();
                    void b(long v);
                }
                @Function("fn")
                int fn(MemorySegment p, Dense s);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.TestLib$Dense$Impl");
        assertNotNull("Dense$Impl class not found", implClass);

        var layoutField = implClass.getDeclaredField("LAYOUT");
        layoutField.setAccessible(true);
        MemoryLayout layout = (MemoryLayout) layoutField.get(null);
        // int a (4) + 4 bytes auto natural-alignment padding + long b (8) = 16 bytes
        assertEquals("Auto-aligned dense layout size", 16L, layout.byteSize());
        assertEquals("b must be at offset 8", 8L, layout.byteOffset(MemoryLayout.PathElement.groupElement("b")));
    }

    /**
     * A sparse struct with {@code @Offset} on every field and {@code @StructSize} produces a
     * layout whose {@code byteSize()} equals the declared {@code @StructSize}.
     */
    public void testSparseTotalSizeMatchesStructSize() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification(sparse = true)
                @StructSize(144)
                interface Stat64 {
                    @Offset(48)
                    long stSize();
                    void stSize(long v);
                    @Offset(56)
                    long stBlocks();
                    void stBlocks(long v);
                }
                @Function("stat64")
                int stat64(MemorySegment path, Stat64 stat);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.TestLib$Stat64$Impl");
        assertNotNull("Stat64$Impl class not found", implClass);

        var layoutField = implClass.getDeclaredField("LAYOUT");
        layoutField.setAccessible(true);
        MemoryLayout layout = (MemoryLayout) layoutField.get(null);
        assertEquals("Sparse layout byteSize() must equal @StructSize", 144L, layout.byteSize());
    }

    /**
     * A sparse struct places fields at their declared {@code @Offset} positions. Verify that
     * {@code LAYOUT.byteOffset(groupElement("stSize"))} is 48 and
     * {@code LAYOUT.byteOffset(groupElement("stBlocks"))} is 56.
     */
    public void testSparseFieldsAtDeclaredOffsets() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification(sparse = true)
                @StructSize(144)
                interface Stat64 {
                    @Offset(48)
                    long stSize();
                    void stSize(long v);
                    @Offset(56)
                    long stBlocks();
                    void stBlocks(long v);
                }
                @Function("stat64")
                int stat64(MemorySegment path, Stat64 stat);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.TestLib$Stat64$Impl");
        assertNotNull("Stat64$Impl class not found", implClass);

        var layoutField = implClass.getDeclaredField("LAYOUT");
        layoutField.setAccessible(true);
        MemoryLayout layout = (MemoryLayout) layoutField.get(null);
        assertEquals("stSize must be at offset 48", 48L, layout.byteOffset(MemoryLayout.PathElement.groupElement("stSize")));
        assertEquals("stBlocks must be at offset 56", 56L, layout.byteOffset(MemoryLayout.PathElement.groupElement("stBlocks")));
    }

    /**
     * A sparse struct with per-platform {@code @Offset} annotations produces a {@code LAYOUT} whose
     * {@code byteSize()} and field offsets match those declared for {@code Platform.current()}.
     *
     * <p>Linux platforms use offset 48; all other platforms use offset 96. The struct size is 200
     * on every platform so that only the field-offset switch arm differs between groups.
     */
    public void testSparsePerPlatformLayoutMatchesCurrentPlatform() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Platform;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification(sparse = true)
                @StructSize(200)
                interface Stat {
                    @Offset(value = 48, platforms = {Platform.LINUX_X64, Platform.LINUX_AARCH64})
                    @Offset(96)
                    long stSize();
                    void stSize(long v);
                }
                @Function("stat")
                int stat(MemorySegment path, Stat stat);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.TestLib$Stat$Impl");
        assertNotNull("Stat$Impl class not found", implClass);

        var layoutField = implClass.getDeclaredField("LAYOUT");
        layoutField.setAccessible(true);
        MemoryLayout layout = (MemoryLayout) layoutField.get(null);

        Platform current = Platform.current();
        boolean isLinux = current == Platform.LINUX_X64 || current == Platform.LINUX_AARCH64;
        long expectedOffset = isLinux ? 48L : 96L;

        assertEquals("Sparse per-platform layout byteSize() must equal @StructSize", 200L, layout.byteSize());
        assertEquals(
            "stSize offset must match Platform.current()",
            expectedOffset,
            layout.byteOffset(MemoryLayout.PathElement.groupElement("stSize"))
        );
    }

    /**
     * A sparse struct with the first field at a non-zero offset includes leading padding before that
     * field in the layout. Verify that the total byteSize() matches @StructSize even with leading padding.
     */
    public void testSparseWithLeadingAndTrailingPadding() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification(sparse = true)
                @StructSize(32)
                interface Sparse {
                    @Offset(8)
                    int value();
                    void value(int v);
                }
                @Function("fn")
                int fn(MemorySegment p, Sparse s);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> implClass = result.loadClass("test.TestLib$Sparse$Impl");
        assertNotNull("Sparse$Impl class not found", implClass);

        var layoutField = implClass.getDeclaredField("LAYOUT");
        layoutField.setAccessible(true);
        MemoryLayout layout = (MemoryLayout) layoutField.get(null);
        // Total size = @StructSize = 32
        assertEquals("Sparse layout byteSize() must equal @StructSize even with padding", 32L, layout.byteSize());
        // Field at declared offset
        assertEquals("value must be at offset 8", 8L, layout.byteOffset(MemoryLayout.PathElement.groupElement("value")));
    }

    /**
     * A sparse {@code @StructSpecification} record honours {@code @Offset} and {@code @StructSize}
     * exactly as an interface does: the {@code $Pack} LAYOUT has {@code byteSize()} equal to
     * {@code @StructSize} and places each field at its declared offset.
     */
    public void testSparseRecordWithOffsetAndStructSize() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.ArrayField;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            import org.elasticsearch.foreign.Function;
            import java.lang.foreign.MemorySegment;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification(sparse = true)
                @StructSize(144)
                record Elem(@Offset(48) long a, @Offset(56) long b) {}
                @StructSpecification
                interface Buf {
                    @ArrayField(lengthField = "len")
                    Elem at(int i);
                    int len();
                    void len(int v);
                }
                @Function("fn")
                int fn(MemorySegment p, Buf b);
            }
            """;

        CompilationResult result = compile("test.TestLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> packClass = result.loadClass("test.TestLib$Elem$Pack");
        assertNotNull("Elem$Pack class not found", packClass);

        var layoutField = packClass.getDeclaredField("LAYOUT");
        layoutField.setAccessible(true);
        MemoryLayout layout = (MemoryLayout) layoutField.get(null);
        assertEquals("Sparse record layout byteSize() must equal @StructSize", 144L, layout.byteSize());
        assertEquals("a must be at offset 48", 48L, layout.byteOffset(MemoryLayout.PathElement.groupElement("a")));
        assertEquals("b must be at offset 56", 56L, layout.byteOffset(MemoryLayout.PathElement.groupElement("b")));
    }

    /**
     * A {@code @Sizeof} method on a dense struct returns a compile-time constant equal to the
     * struct's natural-aligned byte size.
     */
    public void testSizeofDenseStructReturnsConstant() throws Exception {
        String libSource = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Sizeof;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface SizeofLib {
                @StructSpecification
                interface Dense {
                    @Sizeof
                    int sizeof();
                    long a();
                    void a(long v);
                    int b();
                    void b(int v);
                }
            }
            """;
        String driverSource = """
            package test;
            public final class SizeofDriver {
                public static int size() {
                    return new SizeofLib$Dense$Impl().sizeof();
                }
            }
            """;

        var sources = new java.util.LinkedHashMap<String, String>();
        sources.put("test.SizeofLib", libSource);
        sources.put("test.SizeofDriver", driverSource);
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> driver = result.loadClass("test.SizeofDriver");
        int size = (int) driver.getMethod("size").invoke(null);
        // long a (8) + int b (4, naturally aligned at offset 8) = 12 bytes
        assertEquals("sizeof() must equal dense LAYOUT.byteSize()", 12, size);
    }

    /**
     * A {@code @Sizeof} method on a sparse struct whose {@code @StructSize} differs per platform
     * returns the size resolved for {@code Platform.current()} at runtime.
     */
    public void testSizeofSparsePerPlatformStructReturnsCurrentPlatformSize() throws Exception {
        if (System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).startsWith("windows")) {
            return; // struct sizes are declared only for Linux/macOS; Windows is excluded via unavailableOn
        }
        String libSource = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Offset;
            import org.elasticsearch.foreign.Platform;
            import org.elasticsearch.foreign.Sizeof;
            import org.elasticsearch.foreign.StructSize;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification(unavailableOn = {Platform.WINDOWS_X64})
            public interface SizeofLib {
                @StructSpecification(sparse = true)
                @StructSize(value = 144, platforms = {Platform.LINUX_X64, Platform.LINUX_AARCH64})
                @StructSize(value = 152, platforms = {Platform.DARWIN_X64, Platform.DARWIN_AARCH64})
                interface Stat {
                    @Sizeof
                    int sizeof();
                    @Offset(48)
                    long stSize();
                    void stSize(long v);
                }
            }
            """;
        String driverSource = """
            package test;
            public final class SizeofDriver {
                public static int size() {
                    return new SizeofLib$Stat$Impl().sizeof();
                }
            }
            """;

        var sources = new java.util.LinkedHashMap<String, String>();
        sources.put("test.SizeofLib", libSource);
        sources.put("test.SizeofDriver", driverSource);
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> driver = result.loadClass("test.SizeofDriver");
        int size = (int) driver.getMethod("size").invoke(null);

        Platform current = Platform.current();
        boolean isLinux = current == Platform.LINUX_X64 || current == Platform.LINUX_AARCH64;
        int expectedSize = isLinux ? 144 : 152;
        assertEquals("sizeof() must equal @StructSize resolved for Platform.current()", expectedSize, size);
    }

    /**
     * A {@code @Sizeof} method that takes parameters is a compile error.
     */
    public void testSizeofMethodWithParametersEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Sizeof;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification
                interface MyStruct {
                    @Sizeof
                    int sizeof(int extra);
                    long field();
                    void field(long v);
                }
            }
            """;

        CompilationResult result = compile("test.TestLib", source);

        assertFalse("Expected compilation to fail due to @Sizeof method with parameters", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("@Sizeof") && msg.contains("no parameters"));
        assertTrue("Expected error about @Sizeof method parameters but got: " + result.errors(), hasError);
    }

    /**
     * A {@code @Sizeof} method that does not return {@code int} is a compile error.
     */
    public void testSizeofMethodNonIntReturnEmitsError() {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Sizeof;
            import org.elasticsearch.foreign.StructSpecification;
            @LibrarySpecification
            public interface TestLib {
                @StructSpecification
                interface MyStruct {
                    @Sizeof
                    long sizeof();
                    long field();
                    void field(long v);
                }
            }
            """;

        CompilationResult result = compile("test.TestLib", source);

        assertFalse("Expected compilation to fail due to @Sizeof method not returning int", result.success());
        boolean hasError = result.errors().stream().anyMatch(msg -> msg.contains("@Sizeof") && msg.contains("must return int"));
        assertTrue("Expected error about @Sizeof return type but got: " + result.errors(), hasError);
    }
}
