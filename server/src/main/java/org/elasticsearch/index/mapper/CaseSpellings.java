/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.UnicodeUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The ways a value can spell a term whose characters it lowers onto. An analyzer that lowers a token leaves it
 * spelled by whichever characters lower onto its own, so a value holding that token holds one of those spellings
 * and no others — which is what lets a reader of the value's bytes look for the term without analyzing it.
 *
 * <p>Spelling a character is not one byte: lowering a character can spell it shorter ({@code U+212A} becomes
 * {@code k}, three bytes becoming one), and the bytes of its upper and lower case differ by no single rule. So a
 * term is not folded into one pattern but spelled out into the patterns a value could hold, each a search of its
 * own, and the comparison of the ASCII letters without their case takes care of the rest.
 */
final class CaseSpellings {

    private CaseSpellings() {}

    /**
     * The other ways to spell a character, by the characters that lower onto it, held only for the characters
     * spelled some way their own bytes do not cover under the comparison of ASCII case. Built once, by asking of
     * every character what it lowers onto, so that a change in what Unicode says cannot leave this behind.
     */
    private static final Map<Integer, byte[][]> OTHER_SPELLINGS;

    /** {@link #OTHER_SPELLINGS} for the ASCII characters, which most terms are made of. */
    private static final byte[][][] OTHER_ASCII_SPELLINGS = new byte[128][][];

    static {
        final Map<Integer, List<Integer>> sources = new TreeMap<>();
        for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
            if (Character.isDefined(codePoint) == false) {
                continue;
            }
            final int lower = Character.toLowerCase(codePoint);
            if (lower != codePoint) {
                sources.computeIfAbsent(lower, k -> new ArrayList<>()).add(codePoint);
            }
        }
        final Map<Integer, byte[][]> built = new HashMap<>();
        sources.forEach((lower, from) -> {
            final Set<BytesRef> apart = new HashSet<>();
            apart.add(fold(new BytesRef(utf8(lower))));
            final List<byte[]> others = new ArrayList<>();
            for (int codePoint : from) {
                final byte[] other = utf8(codePoint);
                if (apart.add(fold(new BytesRef(other)))) {
                    others.add(other);
                }
            }
            if (others.isEmpty() == false) {
                built.put(lower, others.toArray(byte[][]::new));
            }
        });
        built.forEach((lower, others) -> {
            if (lower < OTHER_ASCII_SPELLINGS.length) {
                OTHER_ASCII_SPELLINGS[lower] = others;
            }
        });
        OTHER_SPELLINGS = Map.copyOf(built);
    }

    private static byte[] utf8(int codePoint) {
        return new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Every way a value could spell {@code term}, as patterns to look for, or null where there are more of them
     * than {@code most}, or where {@code term} is not UTF-8. Spellings of one character that the comparison of
     * ASCII case takes for the same are one, so a term of ASCII comes to one pattern. A character spelled one way
     * is appended to the spellings in place, and only one spelled more ways copies them, which can happen no more
     * than {@code most} times over, so the work stays linear in the length of the term.
     */
    static List<BytesRef> of(BytesRef term, int most) {
        final byte[] bytes = term.bytes;
        final int end = term.offset + term.length;
        List<BytesRefBuilder> spellings = new ArrayList<>(List.of(new BytesRefBuilder()));
        final UnicodeUtil.UTF8CodePoint codePoint = new UnicodeUtil.UTF8CodePoint();
        for (int at = term.offset; at < end; at += codePoint.numBytes) {
            final byte[][] others;
            final int lead = bytes[at] & 0xFF;
            if (lead < 0x80) {
                codePoint.numBytes = 1;
                others = OTHER_ASCII_SPELLINGS[lead];
            } else {
                final int length = lead >= 0xF0 ? 4 : lead >= 0xE0 ? 3 : lead >= 0xC0 ? 2 : 0;
                if (length == 0 || lead > 0xF4 || at + length > end) {
                    return null;
                }
                UnicodeUtil.codePointAt(bytes, at, codePoint);
                others = OTHER_SPELLINGS.get(codePoint.codePoint);
            }
            if (others == null) {
                for (BytesRefBuilder spelling : spellings) {
                    spelling.append(bytes, at, codePoint.numBytes);
                }
                continue;
            }
            if ((long) spellings.size() * (1 + others.length) > most) {
                return null;
            }
            final List<BytesRefBuilder> grown = new ArrayList<>(spellings.size() * (1 + others.length));
            for (BytesRefBuilder spelling : spellings) {
                final BytesRefBuilder copy = new BytesRefBuilder();
                copy.copyBytes(spelling);
                copy.append(bytes, at, codePoint.numBytes);
                grown.add(copy);
            }
            for (byte[] other : others) {
                for (BytesRefBuilder spelling : spellings) {
                    final BytesRefBuilder copy = new BytesRefBuilder();
                    copy.copyBytes(spelling);
                    copy.append(other, 0, other.length);
                    grown.add(copy);
                }
            }
            spellings = grown;
        }
        final List<BytesRef> patterns = new ArrayList<>(spellings.size());
        final Set<BytesRef> apart = new HashSet<>();
        for (BytesRefBuilder spelling : spellings) {
            final BytesRef pattern = spelling.toBytesRef();
            if (apart.add(fold(pattern))) {
                patterns.add(pattern);
            }
        }
        return patterns;
    }

    /** A pattern as the comparison of ASCII case reads it, which is how two spellings are told apart. */
    private static BytesRef fold(BytesRef bytes) {
        final byte[] folded = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            folded[i] = (byte) (bytes.bytes[bytes.offset + i] | 0x20);
        }
        return new BytesRef(folded);
    }
}
