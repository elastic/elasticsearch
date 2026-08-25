/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.adapter;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Adapts MemorySegment APIs that changed between JDK 21 and 22+.
 */
public final class MemorySegmentAdapter {

    public static String getString(MemorySegment segment, long offset) {
        return segment.getUtf8String(offset);
    }

    public static void setString(MemorySegment segment, long offset, String value) {
        segment.setUtf8String(offset, value);
    }

    public static MemorySegment allocateString(Arena arena, String s) {
        return arena.allocateUtf8String(s);
    }

    /**
     * FFM added charset-aware string access in JDK 22 ({@code Arena.allocateFrom(String, Charset)},
     * {@code MemorySegment.getString(long, Charset)}, {@code MemorySegment.setString(long, String, Charset)}).
     * JDK 21 has no equivalent, so this branch backports it manually. Only {@link StandardCharsets#UTF_8}
     * and {@link StandardCharsets#UTF_16LE} are supported: those are the only encodings any current caller
     * needs (UTF-8 bindings and Windows {@code *W}-suffixed APIs), and constraining the surface keeps this
     * manual encode/decode logic small and reviewable. This whole method disappears once the minimum
     * runtime moves off JDK 21 and the two adapter branches collapse into the JDK 22+ passthrough.
     *
     * @throws UnsupportedOperationException if charset is neither {@link StandardCharsets#UTF_8} nor
     *         {@link StandardCharsets#UTF_16LE}
     */
    private static void requireSupportedCharset(Charset charset) {
        if (charset.equals(StandardCharsets.UTF_8) == false && charset.equals(StandardCharsets.UTF_16LE) == false) {
            throw new UnsupportedOperationException("Unsupported charset: " + charset.name());
        }
    }

    private static int terminatorSize(Charset charset) {
        return charset.equals(StandardCharsets.UTF_16LE) ? 2 : 1;
    }

    /**
     * Charset-aware counterpart to {@link #allocateString(Arena, String)}, for encodings other than
     * UTF-8 such as the UTF-16LE used by Windows {@code *W}-suffixed APIs.
     *
     * @throws UnsupportedOperationException if charset is neither {@link StandardCharsets#UTF_8} nor
     *         {@link StandardCharsets#UTF_16LE}
     */
    public static MemorySegment allocateString(Arena arena, String s, Charset charset) {
        requireSupportedCharset(charset);
        byte[] bytes = s.getBytes(charset);
        int terminatorSize = terminatorSize(charset);
        MemorySegment segment = arena.allocate(bytes.length + terminatorSize);
        MemorySegment.copy(bytes, 0, segment, ValueLayout.JAVA_BYTE, 0, bytes.length);
        return segment;
    }

    /**
     * Charset-aware counterpart to {@link #getString(MemorySegment, long)}, for encodings other than
     * UTF-8 such as the UTF-16LE used by Windows {@code *W}-suffixed APIs.
     *
     * @throws UnsupportedOperationException if charset is neither {@link StandardCharsets#UTF_8} nor
     *         {@link StandardCharsets#UTF_16LE}
     */
    public static String getString(MemorySegment segment, long offset, Charset charset) {
        requireSupportedCharset(charset);
        long end = offset;
        if (charset.equals(StandardCharsets.UTF_16LE)) {
            while (segment.get(ValueLayout.JAVA_SHORT_UNALIGNED, end) != 0) {
                end += 2;
            }
        } else {
            while (segment.get(ValueLayout.JAVA_BYTE, end) != 0) {
                end += 1;
            }
        }
        byte[] bytes = new byte[(int) (end - offset)];
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, offset, bytes, 0, bytes.length);
        return new String(bytes, charset);
    }

    /**
     * Charset-aware counterpart to {@link #setString(MemorySegment, long, String)}, for encodings
     * other than UTF-8 such as the UTF-16LE used by Windows {@code *W}-suffixed APIs. The caller must
     * supply a segment large enough to hold the encoded bytes plus the NUL terminator, starting at
     * {@code offset}.
     *
     * @throws UnsupportedOperationException if charset is neither {@link StandardCharsets#UTF_8} nor
     *         {@link StandardCharsets#UTF_16LE}
     */
    public static void setString(MemorySegment segment, long offset, String value, Charset charset) {
        requireSupportedCharset(charset);
        byte[] bytes = value.getBytes(charset);
        MemorySegment.copy(bytes, 0, segment, ValueLayout.JAVA_BYTE, offset, bytes.length);
        int terminatorSize = terminatorSize(charset);
        for (int i = 0; i < terminatorSize; i++) {
            segment.set(ValueLayout.JAVA_BYTE, offset + bytes.length + i, (byte) 0);
        }
    }

    /**
     * Return a {@link VarHandle} to access an element within the given memory layout.
     *
     * Returns the VarHandle directly; in Java 21, a single-element path VarHandle does not need an
     * offset coordinate inserted (unlike the Java 22 variant, which inserts a fixed {@code 0L} at
     * coordinate position 1).
     *
     * @param layout The layout of a struct to access
     * @param element The element within the struct to access
     * @return A {@link VarHandle} that accesses the element with a fixed offset of 0
     */
    public static VarHandle varHandleWithoutOffset(MemoryLayout layout, MemoryLayout.PathElement element) {
        return layout.varHandle(element);
    }

    /**
     * Return a {@link VarHandle} to access a sequence element within the given memory layout,
     * using a two-element path: {@code groupElement(name)} then {@code sequenceElement()}.
     *
     * Returns the VarHandle directly; in Java 21, group+sequence path VarHandles do not need an
     * offset coordinate inserted (unlike the Java 22 variant, which inserts a fixed {@code 0L} at
     * coordinate position 1).
     *
     * @param layout The layout of a struct to access
     * @param group The group element path element (e.g. {@code groupElement("fieldName")})
     * @param seq The sequence element path element (i.e. {@code sequenceElement()})
     * @return A {@link VarHandle} that accesses indexed sequence elements with a fixed offset of 0
     */
    public static VarHandle varHandleSequenceWithoutOffset(
        MemoryLayout layout,
        MemoryLayout.PathElement group,
        MemoryLayout.PathElement seq
    ) {
        return layout.varHandle(group, seq);
    }

    private MemorySegmentAdapter() {}
}
