/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal;

import org.elasticsearch.simdjson.JsonParsingException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for the {@link StructuralIndexer} Java API layer. Low-level native FFI
 * correctness (structurals, whitespace, escaping, error codes, guard pages, etc.) is
 * covered by {@link org.elasticsearch.simdjson.internal.SimdJsonLibraryTests SimdJsonLibraryTests}; these tests focus on the Java-level concerns:
 * the {@code byte[]}-based API, bounds checking, error wrapping, context reuse, and
 * {@link AutoCloseable} lifecycle.
 */
public class StructuralIndexerTests extends ESTestCase {

    @Before
    public void requireNativeLibrary() {
        assumeTrue("Native simdjson library not available", StructuralIndexer.available());
    }

    // ---- Basic API ----

    // Smoke test: the byte[]-based API produces correct structurals.
    public void testSimpleDocument() {
        byte[] buffer = "{\"a\":1}".getBytes(UTF_8);
        try (StructuralIndexer indexer = new StructuralIndexer(buffer.length)) {
            BitIndexes bi = new BitIndexes(buffer.length);
            indexer.index(buffer, buffer.length, bi);
            assertEquals(List.of('{', '"', ':', '1', '}'), drainStructurals(buffer, bi));
        }
    }

    // The offset variant produces absolute indices starting at the offset.
    public void testWithOffset() {
        String json = "{\"x\":42}";
        byte[] raw = json.getBytes(UTF_8);
        int offset = 100;
        byte[] buffer = new byte[offset + raw.length];
        System.arraycopy(raw, 0, buffer, offset, raw.length);

        try (StructuralIndexer indexer = new StructuralIndexer(buffer.length)) {
            BitIndexes bi = new BitIndexes(buffer.length);
            indexer.index(buffer, offset, raw.length, bi);

            bi.setReadWindow(0, bi.writeCount());
            int firstIdx = bi.getAndAdvance();
            assertEquals(offset, firstIdx);
            assertEquals('{', buffer[firstIdx]);
        }
    }

    // A single indexer instance can parse multiple documents sequentially.
    public void testReuse() {
        try (StructuralIndexer indexer = new StructuralIndexer(64)) {
            byte[] buf1 = "{\"a\":1}".getBytes(UTF_8);
            BitIndexes bi = new BitIndexes(64);
            indexer.index(buf1, buf1.length, bi);
            assertEquals(List.of('{', '"', ':', '1', '}'), drainStructurals(buf1, bi));

            byte[] buf2 = "{\"b\":2,\"c\":3}".getBytes(UTF_8);
            indexer.index(buf2, buf2.length, bi);
            assertEquals(List.of('{', '"', ':', '2', ',', '"', ':', '3', '}'), drainStructurals(buf2, bi));
        }
    }

    // ---- Error wrapping ----

    // Native errors are wrapped as JsonParsingException with a human-readable message.
    public void testInvalidUtf8Throws() {
        byte[] buffer = new byte[] { '{', '"', (byte) 0xFF, '"', ':', '1', '}' };
        try (StructuralIndexer indexer = new StructuralIndexer(buffer.length)) {
            BitIndexes bi = new BitIndexes(buffer.length);
            var e = expectThrows(JsonParsingException.class, () -> indexer.index(buffer, buffer.length, bi));
            assertThat(e.getMessage(), containsString("UTF-8"));
        }
    }

    // ---- Bounds checks (Java-level, before native call) ----

    static final Class<IndexOutOfBoundsException> IOOBE = IndexOutOfBoundsException.class;

    public void testBoundsCheckLenExceedsBuffer() {
        byte[] buffer = "{\"a\":1}".getBytes(UTF_8);
        try (StructuralIndexer indexer = new StructuralIndexer(256)) {
            BitIndexes bi = new BitIndexes(256);
            expectThrows(IOOBE, () -> indexer.index(buffer, buffer.length + 1, bi));
        }
    }

    public void testBoundsCheckOffsetPlusLenExceedsBuffer() {
        byte[] buffer = new byte[10];
        try (StructuralIndexer indexer = new StructuralIndexer(256)) {
            BitIndexes bi = new BitIndexes(256);
            expectThrows(IOOBE, () -> indexer.index(buffer, 5, 6, bi));
        }
    }

    public void testBoundsCheckNegativeOffset() {
        byte[] buffer = "{\"a\":1}".getBytes(UTF_8);
        try (StructuralIndexer indexer = new StructuralIndexer(256)) {
            BitIndexes bi = new BitIndexes(256);
            expectThrows(IOOBE, () -> indexer.index(buffer, -1, buffer.length, bi));
        }
    }

    public void testBoundsCheckNegativeLen() {
        byte[] buffer = "{\"a\":1}".getBytes(UTF_8);
        try (StructuralIndexer indexer = new StructuralIndexer(256)) {
            BitIndexes bi = new BitIndexes(256);
            expectThrows(IOOBE, () -> indexer.index(buffer, 0, -1, bi));
        }
    }

    // ---- Lifecycle ----

    // Closing twice should not crash.
    public void testDoubleCloseIsNoOp() {
        StructuralIndexer indexer = new StructuralIndexer(64);
        indexer.close();
        indexer.close();
    }

    private static List<Character> drainStructurals(byte[] buffer, BitIndexes bi) {
        bi.setReadWindow(0, bi.writeCount());
        List<Character> chars = new ArrayList<>();
        while (bi.isEnd() == false) {
            chars.add((char) buffer[bi.getAndAdvance()]);
        }
        return chars;
    }
}
