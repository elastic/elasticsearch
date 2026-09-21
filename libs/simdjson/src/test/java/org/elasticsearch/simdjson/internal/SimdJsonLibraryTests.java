/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.simdjson.SimdJsonTestCase;
import org.elasticsearch.simdjson.internal.parsers.BitIndexes;
import org.elasticsearch.simdvec.GuardPageAllocator;
import org.junit.Before;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Low-level FFI tests for {@link SimdJsonLibrary} (direct native {@code stage1} calls).
 *
 * <p>Many cases here overlap upstream simdjson's C++ stage-1 tests: structural indexing,
 * UTF-8 validation, and whitespace handling are already exercised by simdjson itself.
 * We keep a small smoke set at this layer to catch regressions in {@code es_simdjson}
 * and the FFM binding, not to re-prove simdjson correctness.
 *
 * <p>Tests that are ES-specific and not covered upstream or at walker level:
 * <ul>
 *   <li>{@code create}/{@code destroy} lifecycle and null handling</li>
 *   <li>Generated {@code @SlicedSegment}/{@code @VectorSegment} bounds checks</li>
 *   <li>Guard-page verification via {@link GuardPageAllocator} (parameterized with
 *       {@link Arena#ofConfined()})</li>
 *   <li>{@link SimdJsonLibrary#errorMessage(int)} mapping for wrapper error codes</li>
 * </ul>
 *
 * <p>End-to-end document parsing is covered by {@link org.elasticsearch.simdjson.SimdJsonDirectWalkerTests}
 * and {@link org.elasticsearch.simdjson.SimdJsonJacksonComparisonTests}. The Java {@code byte[]}
 * API layer is covered by {@link StructuralIndexerTests}.
 */
public class SimdJsonLibraryTests extends SimdJsonTestCase {

    private final Supplier<Arena> arenaSupplier;

    private SimdJsonLibrary lib;

    public SimdJsonLibraryTests(Supplier<Arena> arenaSupplier) {
        this.arenaSupplier = arenaSupplier;
    }

    @Override
    protected NativeRequirement nativeRequirement() {
        return NativeRequirement.LIBRARY;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[] { named("Arena.ofConfined", Arena::ofConfined) });
        if (GuardPageAllocator.isSupported()) {
            params.add(new Object[] { named("GuardPageAllocator.ofConfined", GuardPageAllocator::ofConfined) });
        }
        return params;
    }

    @Before
    public void initLibrary() {
        lib = SimdJsonNativeSupport.library();
    }

    // ---- Basic functionality ----

    // Verify that create returns a non-null context and destroy does not crash.
    public void testCreateAndDestroy() {
        MemorySegment ctx = lib.create(256);
        assertNotEquals("create should return non-null context", MemorySegment.NULL, ctx);
        lib.destroy(ctx);
    }

    // Destroying a NULL segment should be a safe no-op.
    public void testDestroyNullSegmentIsNoOp() {
        lib.destroy(MemorySegment.NULL);
    }

    // Passing Java null (not MemorySegment.NULL) should throw.
    public void testDestroyJavaNullThrows() {
        // The generated $Impl has an assertion on the parameter; with -ea the AssertionError
        // fires before the NullPointerException from the actual downcall.
        expectThrows(AssertionError.class, () -> lib.destroy(null));
    }

    // Parse {"a":1} with varying whitespace and verify identical structurals.
    public void testStage1SimpleDocument() {
        String[] variants = { "{\"a\":1}", "{ \"a\" : 1 }", "{  \"a\"  :  1  }", "{\n\"a\"\n:\n1\n}", "{\t\"a\"\t:\t1\t}" };
        List<Character> expected = List.of('{', '"', ':', '1', '}');
        for (String json : variants) {
            byte[] buffer = json.getBytes(UTF_8);
            MemorySegment ctx = lib.create(buffer.length);
            try (Arena arena = arenaSupplier.get()) {
                BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
                List<Character> chars = drainStructurals(buffer, bi);
                assertEquals("structural mismatch for: " + json, expected, chars);
            } finally {
                lib.destroy(ctx);
            }
        }
    }

    // Parse JSON placed at a non-zero offset and verify indices are absolute buffer positions.
    public void testStage1WithOffset() {
        String[] variants = { "{\"x\":42}", "{ \"x\" : 42 }", "{  \"x\"  :  42  }", "{\n\"x\"\n:\n42\n}" };
        List<Character> expected = List.of('{', '"', ':', '4', '}');
        int offset = 50;
        for (String json : variants) {
            byte[] raw = json.getBytes(UTF_8);
            byte[] buffer = new byte[offset + raw.length];
            System.arraycopy(raw, 0, buffer, offset, raw.length);

            MemorySegment ctx = lib.create(buffer.length);
            try (Arena arena = arenaSupplier.get()) {
                BitIndexes bi = runStage1(ctx, arena, buffer, offset, raw.length);
                List<Character> chars = drainStructurals(buffer, bi);
                assertEquals("structural mismatch for: " + json, expected, chars);
                // verify indices are absolute positions in the buffer, not relative to the offset
                bi.setReadWindow(0, bi.writeCount());
                int firstIdx = bi.getAndAdvance();
                assertEquals("first index should be at offset for: " + json, offset, firstIdx);
            } finally {
                lib.destroy(ctx);
            }
        }
    }

    // Empty objects with varying whitespace should produce only { and } structurals.
    public void testStage1EmptyObject() {
        String[] variants = { "{}", " {}", "{ }", "{  }", "{\n}", "{\t}" };
        List<Character> expected = List.of('{', '}');
        for (String json : variants) {
            byte[] buffer = json.getBytes(UTF_8);
            MemorySegment ctx = lib.create(buffer.length);
            try (Arena arena = arenaSupplier.get()) {
                BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
                List<Character> chars = drainStructurals(buffer, bi);
                assertEquals("structural mismatch for: " + json, expected, chars);
            } finally {
                lib.destroy(ctx);
            }
        }
    }

    // A single context can be reused to parse multiple different documents.
    public void testContextReuse() {
        MemorySegment ctx = lib.create(256);
        try (Arena arena = arenaSupplier.get()) {
            byte[] buf1 = "{\"a\":1}".getBytes(UTF_8);
            BitIndexes bi1 = runStage1(ctx, arena, buf1, 0, buf1.length);
            assertEquals(List.of('{', '"', ':', '1', '}'), drainStructurals(buf1, bi1));

            byte[] buf2 = "{\"b\":2,\"c\":3}".getBytes(UTF_8);
            BitIndexes bi2 = runStage1(ctx, arena, buf2, 0, buf2.length);
            assertEquals(List.of('{', '"', ':', '2', ',', '"', ':', '3', '}'), drainStructurals(buf2, bi2));
        } finally {
            lib.destroy(ctx);
        }
    }

    // Invalid UTF-8 byte (0xFF) inside a string should return a UTF8_ERROR.
    public void testStage1InvalidUtf8ReturnsError() {
        byte[] buffer = new byte[7];
        buffer[0] = '{';
        buffer[1] = '"';
        buffer[2] = (byte) 0xFF;
        buffer[3] = '"';
        buffer[4] = ':';
        buffer[5] = '1';
        buffer[6] = '}';

        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            int[] out = new int[buffer.length + 1];
            MemorySegment outCount = arena.allocate(ValueLayout.JAVA_INT);
            int err = lib.stage1(ctx, MemorySegment.ofArray(buffer), 0, buffer.length, MemorySegment.ofArray(out), out.length, outCount);
            assertNotEquals("should return non-zero error for invalid UTF-8", 0, err);
            String msg = lib.errorMessage(err);
            assertThat(msg, is("UTF8_ERROR: The input is not valid UTF-8"));
        } finally {
            lib.destroy(ctx);
        }
    }

    // A string that is opened but never closed should return an UNCLOSED_STRING error.
    public void testStage1UnclosedStringReturnsError() {
        String[] variants = { "{\"x:42}", "{ \"x\" : \"42 }", "{  x\"  :  42  }", "{\n\"x\"\n:\n42\"\n}" };
        for (String json : variants) {
            byte[] buffer = json.getBytes(UTF_8);
            MemorySegment ctx = lib.create(buffer.length);
            try (Arena arena = arenaSupplier.get()) {
                int[] out = new int[buffer.length + 1];
                MemorySegment outCount = arena.allocate(ValueLayout.JAVA_INT);
                int err = lib.stage1(
                    ctx,
                    MemorySegment.ofArray(buffer),
                    0,
                    buffer.length,
                    MemorySegment.ofArray(out),
                    out.length,
                    outCount
                );
                assertNotEquals("should return non-zero error for unclosed string", 0, err);
                String msg = lib.errorMessage(err);
                assertThat(msg, is("UNCLOSED_STRING: A string is opened, but never closed."));
            } finally {
                lib.destroy(ctx);
            }
        }
    }

    // Control characters (0x00–0x1F) inside a string must be escaped; stage1 should reject them.
    public void testStage1UnescapedCharsReturnsError() {
        // control characters (0x00–0x1F) inside a JSON string must be escaped
        byte[][] variants = {
            { '{', '"', 'a', 0x00, '"', ':', '1', '}' },
            { '{', '"', 'a', 0x0A, '"', ':', '1', '}' },
            { '{', '"', 'a', 0x09, '"', ':', '1', '}' }, };
        for (byte[] buffer : variants) {
            MemorySegment ctx = lib.create(buffer.length);
            try (Arena arena = arenaSupplier.get()) {
                int[] out = new int[buffer.length + 1];
                MemorySegment outCount = arena.allocate(ValueLayout.JAVA_INT);
                int err = lib.stage1(
                    ctx,
                    MemorySegment.ofArray(buffer),
                    0,
                    buffer.length,
                    MemorySegment.ofArray(out),
                    out.length,
                    outCount
                );
                assertNotEquals("should return non-zero error for unescaped chars", 0, err);
                String msg = lib.errorMessage(err);
                assertThat(msg, is("UNESCAPED_CHARS: Within strings, some characters must be escaped, we found unescaped characters"));
            } finally {
                lib.destroy(ctx);
            }
        }
    }

    // ---- Capacity growth ----

    // A context created with a tiny capacity should auto-grow when parsing a larger document.
    public void testSmallInitialCapacityGrows() {
        MemorySegment ctx = lib.create(2);
        try (Arena arena = arenaSupplier.get()) {
            byte[] buffer = "{\"key\":\"value\"}".getBytes(UTF_8);
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            List<Character> chars = drainStructurals(buffer, bi);
            assertEquals(List.of('{', '"', ':', '"', '}'), chars);
        } finally {
            lib.destroy(ctx);
        }
    }

    // ---- Arrays ----

    // Empty arrays with varying whitespace should produce only [ and ] structurals.
    public void testStage1EmptyArray() {
        String[] variants = { "[]", "[ ]", "[\n]", "[\t]" };
        List<Character> expected = List.of('[', ']');
        for (String json : variants) {
            byte[] buffer = json.getBytes(UTF_8);
            MemorySegment ctx = lib.create(buffer.length);
            try (Arena arena = arenaSupplier.get()) {
                BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
                assertEquals("structural mismatch for: " + json, expected, drainStructurals(buffer, bi));
            } finally {
                lib.destroy(ctx);
            }
        }
    }

    // An array of integers should produce structurals for brackets, values, and commas.
    public void testStage1ArrayOfValues() {
        String json = "[1,2,3]";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            assertEquals(List.of('[', '1', ',', '2', ',', '3', ']'), drainStructurals(buffer, bi));
        } finally {
            lib.destroy(ctx);
        }
    }

    // Nested arrays should produce structurals for all bracket levels.
    public void testStage1NestedArrays() {
        String json = "[[1],[2,3]]";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            assertEquals(List.of('[', '[', '1', ']', ',', '[', '2', ',', '3', ']', ']'), drainStructurals(buffer, bi));
        } finally {
            lib.destroy(ctx);
        }
    }

    // An object containing an array value should produce structurals for both containers.
    public void testStage1MixedObjectAndArray() {
        String json = "{\"a\":[1,2]}";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            assertEquals(List.of('{', '"', ':', '[', '1', ',', '2', ']', '}'), drainStructurals(buffer, bi));
        } finally {
            lib.destroy(ctx);
        }
    }

    // ---- Nested objects ----

    // Nested objects should produce structurals for both { } levels.
    public void testStage1NestedObjects() {
        String json = "{\"a\":{\"b\":1}}";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            assertEquals(List.of('{', '"', ':', '{', '"', ':', '1', '}', '}'), drainStructurals(buffer, bi));
        } finally {
            lib.destroy(ctx);
        }
    }

    // ---- Value types ----

    // Boolean values true/false should appear as pseudo-structural starts 't' and 'f'.
    public void testStage1BooleanValues() {
        String json = "{\"a\":true,\"b\":false}";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            assertEquals(List.of('{', '"', ':', 't', ',', '"', ':', 'f', '}'), drainStructurals(buffer, bi));
        } finally {
            lib.destroy(ctx);
        }
    }

    // Null value should appear as pseudo-structural start 'n'.
    public void testStage1NullValue() {
        String json = "{\"a\":null}";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            assertEquals(List.of('{', '"', ':', 'n', '}'), drainStructurals(buffer, bi));
        } finally {
            lib.destroy(ctx);
        }
    }

    // Negative numbers should appear as pseudo-structural start '-'.
    public void testStage1NegativeNumber() {
        String json = "{\"a\":-42}";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            assertEquals(List.of('{', '"', ':', '-', '}'), drainStructurals(buffer, bi));
        } finally {
            lib.destroy(ctx);
        }
    }

    // A string value should appear as pseudo-structural start '"'.
    public void testStage1StringValue() {
        String json = "{\"a\":\"hello\"}";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            assertEquals(List.of('{', '"', ':', '"', '}'), drainStructurals(buffer, bi));
        } finally {
            lib.destroy(ctx);
        }
    }

    // ---- Escaped strings ----

    // An escaped quote inside a string value should not produce an extra structural.
    public void testStage1EscapedStrings() {
        String json = "{\"a\":\"hello\\\"world\"}";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            // the escaped quote inside the string is not a structural
            assertEquals(List.of('{', '"', ':', '"', '}'), drainStructurals(buffer, bi));
        } finally {
            lib.destroy(ctx);
        }
    }

    // An escaped backslash should not cause the following quote to be treated as escaped.
    public void testStage1EscapedBackslash() {
        String json = "{\"a\":\"path\\\\dir\"}";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            assertEquals(List.of('{', '"', ':', '"', '}'), drainStructurals(buffer, bi));
        } finally {
            lib.destroy(ctx);
        }
    }

    // ---- Error message ----

    // Error code 0 (success) should return a valid "SUCCESS" message.
    public void testErrorMessageSuccess() {
        assertThat(lib.errorMessage(0), is("SUCCESS: No error"));
    }

    // An out-of-range error code should return "UNEXPECTED_ERROR".
    public void testErrorMessageUnknownCode() {
        assertThat(lib.errorMessage(9999), is("UNEXPECTED_ERROR"));
    }

    // ---- Large document ----

    // A 100-field document exercises multiple SIMD chunks and verifies the structural count.
    public void testStage1LargeDocument() {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < 100; i++) {
            if (i > 0) sb.append(',');
            sb.append("\"field_").append(i).append("\":").append(i);
        }
        sb.append('}');
        byte[] buffer = sb.toString().getBytes(UTF_8);
        assertTrue("document should span multiple SIMD chunks", buffer.length > 64);

        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            List<Character> chars = drainStructurals(buffer, bi);
            assertEquals('{', chars.getFirst().charValue());
            assertEquals('}', chars.getLast().charValue());
            // 100 fields: each contributes "key":value plus a comma separator (except last)
            // structurals per field: " : value_start = 3, plus comma = 1 (except last)
            // total = { + 100*3 + 99 commas + } = 401
            assertEquals(401, chars.size());
        } finally {
            lib.destroy(ctx);
        }
    }

    // ---- Multiple documents in a single buffer ----

    // Multiple JSON documents concatenated in one buffer should produce structurals for all of them.
    public void testStage1MultipleDocumentsInBuffer() {
        String json = "{\"a\":1}{\"b\":2}{\"c\":3}";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            List<Character> chars = drainStructurals(buffer, bi);
            assertEquals(List.of('{', '"', ':', '1', '}', '{', '"', ':', '2', '}', '{', '"', ':', '3', '}'), chars);
        } finally {
            lib.destroy(ctx);
        }
    }

    // Multiple documents separated by newlines (NDJSON-style) should also work.
    public void testStage1NdjsonStyleBuffer() {
        String json = "{\"a\":1}\n{\"b\":2}\n{\"c\":3}";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            List<Character> chars = drainStructurals(buffer, bi);
            assertEquals(List.of('{', '"', ':', '1', '}', '{', '"', ':', '2', '}', '{', '"', ':', '3', '}'), chars);
        } finally {
            lib.destroy(ctx);
        }
    }

    // ---- Partial documents ----

    // Stage 1 indexes structurals without validating completeness, so a truncated document succeeds.
    public void testStage1PartialDocument() {
        String json = "{\"a\":1,\"b\"";
        byte[] buffer = json.getBytes(UTF_8);
        MemorySegment ctx = lib.create(buffer.length);
        try (Arena arena = arenaSupplier.get()) {
            BitIndexes bi = runStage1(ctx, arena, buffer, 0, buffer.length);
            List<Character> chars = drainStructurals(buffer, bi);
            assertEquals(List.of('{', '"', ':', '1', ',', '"'), chars);
        } finally {
            lib.destroy(ctx);
        }
    }

    // ---- Zero-length input ----

    // Zero-length input should either error without crashing.
    public void testStage1ZeroLengthInput() {
        MemorySegment ctx = lib.create(1);
        try (Arena arena = arenaSupplier.get()) {
            byte[] buffer = new byte[1];
            int[] out = new int[2];
            MemorySegment outCount = arena.allocate(ValueLayout.JAVA_INT);
            for (int offset : List.of(0, 1)) {
                int err = lib.stage1(ctx, MemorySegment.ofArray(buffer), offset, 0, MemorySegment.ofArray(out), out.length, outCount);
                assertNotEquals("should return non-zero error for empty", 0, err);
                assertThat(lib.errorMessage(err), is("EMPTY: no JSON found"));
            }
        } finally {
            lib.destroy(ctx);
        }
    }

    static final Class<IndexOutOfBoundsException> IOOBE = IndexOutOfBoundsException.class;

    // ---- Bounds check tests ----
    // Verify that the generated @SlicedSegment and @VectorSegment bounds checks on
    // stage1 throw IndexOutOfBoundsException when segments are too small.

    // Input buffer smaller than declared length should throw IndexOutOfBoundsException.
    public void testBoundsCheckBufTooSmall() {
        String json = "{\"a\":1}";
        byte[] jsonBytes = json.getBytes(UTF_8);
        int len = jsonBytes.length;

        MemorySegment ctx = lib.create(len);
        try (Arena arena = arenaSupplier.get()) {
            MemorySegment okBuf = arena.allocate(len);
            MemorySegment.copy(jsonBytes, 0, okBuf, ValueLayout.JAVA_BYTE, 0, len);

            int[] out = new int[len + 1];
            MemorySegment outCount = arena.allocate(ValueLayout.JAVA_INT);
            int err = lib.stage1(ctx, okBuf, 0, len, MemorySegment.ofArray(out), out.length, outCount);
            assertEquals("should succeed with exact-size buf", 0, err);

            MemorySegment tooSmallBuf = arena.allocate(len - 1);
            expectThrows(IOOBE, () -> lib.stage1(ctx, tooSmallBuf, 0, len, MemorySegment.ofArray(out), out.length, outCount));
        } finally {
            lib.destroy(ctx);
        }
    }

    // Offset + length exceeding buffer size should throw IndexOutOfBoundsException.
    public void testBoundsCheckOffsetExceedsBuf() {
        int len = 2;
        MemorySegment ctx = lib.create(256);
        try (Arena arena = arenaSupplier.get()) {
            MemorySegment buf = arena.allocate(10);
            int[] out = new int[64];
            MemorySegment outCount = arena.allocate(ValueLayout.JAVA_INT);
            expectThrows(IOOBE, () -> lib.stage1(ctx, buf, 9, len, MemorySegment.ofArray(out), out.length, outCount));
        } finally {
            lib.destroy(ctx);
        }
    }

    // Output buffer too small for the expected structural count should throw IndexOutOfBoundsException.
    public void testBoundsCheckOutBufTooSmall() {
        String json = "{\"a\":1}";
        byte[] jsonBytes = json.getBytes(UTF_8);
        int len = jsonBytes.length;

        MemorySegment ctx = lib.create(len);
        try (Arena arena = arenaSupplier.get()) {
            MemorySegment tooSmallOut = arena.allocate(3 * Integer.BYTES);
            MemorySegment outCount = arena.allocate(ValueLayout.JAVA_INT);
            expectThrows(IOOBE, () -> lib.stage1(ctx, MemorySegment.ofArray(jsonBytes), 0, len, tooSmallOut, 4, outCount));
        } finally {
            lib.destroy(ctx);
        }
    }

    // Random byte arrays of varying lengths should not crash or corrupt memory.
    public void testStage1RandomDataDoesNotCrash() {
        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            int len = between(1, 4096);
            byte[] buffer = new byte[len];
            random().nextBytes(buffer);

            MemorySegment ctx = lib.create(len);
            try (Arena arena = arenaSupplier.get()) {
                int[] out = new int[len + 1];
                MemorySegment outCount = arena.allocate(ValueLayout.JAVA_INT);
                int err = lib.stage1(ctx, MemorySegment.ofArray(buffer), 0, len, MemorySegment.ofArray(out), out.length, outCount);
                if (err != 0) {
                    // stage1 may return an error for random bytes (e.g. invalid UTF-8) — that's fine,
                    // but it there is an error then error handling must succeed
                    assertThat(lib.errorMessage(err), is(not(emptyOrNullString())));
                }
            } finally {
                lib.destroy(ctx);
            }
        }
    }

    // ---- Helpers ----

    // Returns the characters at each structural/pseudo-structural index
    // produced by stage 1: { } [ ] : , and value starts (" digits t f n).
    private static List<Character> drainStructurals(byte[] buffer, BitIndexes bi) {
        bi.setReadWindow(0, bi.writeCount());
        List<Character> chars = new ArrayList<>();
        while (!bi.isEnd()) {
            int idx = bi.getAndAdvance();
            chars.add((char) buffer[idx]);
        }
        return chars;
    }

    private BitIndexes runStage1(MemorySegment ctx, Arena arena, byte[] buffer, int offset, int len) {
        int[] out = new int[len + 1];
        MemorySegment outCount = arena.allocate(ValueLayout.JAVA_INT);
        int err = lib.stage1(ctx, MemorySegment.ofArray(buffer), offset, len, MemorySegment.ofArray(out), out.length, outCount);
        assertEquals("stage 1 should succeed", 0, err);

        int count = outCount.get(ValueLayout.JAVA_INT, 0);
        BitIndexes bi = new BitIndexes(buffer.length);
        bi.ensureCapacity(count);
        System.arraycopy(out, 0, bi.rawIndexes(), 0, count);
        bi.setWriteIdx(count);
        return bi;
    }

    private static Supplier<Arena> named(String name, Supplier<Arena> delegate) {
        return new Supplier<>() {
            @Override
            public Arena get() {
                return delegate.get();
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }
}
