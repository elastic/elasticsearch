/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.adapter;

import org.elasticsearch.test.ESTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * Tests the charset-aware {@code allocateString}/{@code getString}/{@code setString} overloads of
 * {@link MemorySegmentAdapter} that hold on every JDK.
 *
 * <p>This task runs on the runtime JDK, so it exercises whichever implementation the JVM selects
 * from the multi-release jar: the manual UTF-16LE encode/decode in {@code src/main} on JDK 21, or
 * the direct FFM passthrough in {@code src/main22} on JDK 22+. {@link #testAllocateStringUnsupportedCharsetThrows}
 * is the one behavior that differs between the two branches and is guarded accordingly.
 */
public class MemorySegmentAdapterTests extends ESTestCase {

    public void testAllocateStringUtf16LERoundTrip() {
        try (Arena arena = Arena.ofConfined()) {
            for (String s : new String[] { "héllo, world", "", "😀" }) {
                MemorySegment segment = MemorySegmentAdapter.allocateString(arena, s, StandardCharsets.UTF_16LE);
                assertEquals(s, MemorySegmentAdapter.getString(segment, 0, StandardCharsets.UTF_16LE));
            }
        }
    }

    public void testSetStringUtf16LERoundTrip() {
        try (Arena arena = Arena.ofConfined()) {
            String s = "héllo";
            MemorySegment segment = arena.allocate((s.length() + 1) * 2L);
            MemorySegmentAdapter.setString(segment, 0, s, StandardCharsets.UTF_16LE);
            assertEquals(s, MemorySegmentAdapter.getString(segment, 0, StandardCharsets.UTF_16LE));

            // Supplementary character: U+1F600 is a surrogate pair (2 Java chars = 4 UTF-16LE bytes).
            // s.length() returns 2 here, so (2+1)*2 = 6 bytes, which is correct.
            String emoji = "😀";
            MemorySegment emojiSeg = arena.allocate((emoji.length() + 1) * 2L);
            MemorySegmentAdapter.setString(emojiSeg, 0, emoji, StandardCharsets.UTF_16LE);
            assertEquals(emoji, MemorySegmentAdapter.getString(emojiSeg, 0, StandardCharsets.UTF_16LE));
        }
    }

    public void testAllocateStringUtf8CharsetOverloadMatchesLegacy() {
        try (Arena arena = Arena.ofConfined()) {
            String s = "héllo, world";
            MemorySegment legacy = MemorySegmentAdapter.allocateString(arena, s);
            MemorySegment overload = MemorySegmentAdapter.allocateString(arena, s, StandardCharsets.UTF_8);
            assertEquals(legacy.byteSize(), overload.byteSize());
            for (long i = 0; i < legacy.byteSize(); i++) {
                assertEquals(legacy.get(ValueLayout.JAVA_BYTE, i), overload.get(ValueLayout.JAVA_BYTE, i));
            }
        }
    }

    public void testAllocateStringUnsupportedCharsetThrows() {
        assumeTrue("JDK 22+ FFM passthrough accepts any charset the JDK knows", Runtime.version().feature() < 22);
        try (Arena arena = Arena.ofConfined()) {
            expectThrows(
                UnsupportedOperationException.class,
                () -> MemorySegmentAdapter.allocateString(arena, "hello", StandardCharsets.US_ASCII)
            );
        }
    }
}
