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
 * Lists the byte sequences that lowercase to a given term. A lowercasing analyzer produces the term from any
 * character that lowercases to one of its characters, so a value holding the token contains one of these spellings.
 *
 * <p>Case mapping does not preserve the UTF-8 length ({@code U+212A KELVIN SIGN} is three bytes and lowercases to the
 * one-byte {@code k}), and upper- and lowercase bytes are not related by a single rule outside ASCII. So instead of a
 * single folded pattern, a term expands to every spelling, each searched for separately. Spellings that differ only
 * in ASCII case are merged, since the search already ignores ASCII case.
 */
final class CaseSpellings {

    private CaseSpellings() {}

    /**
     * For each lowercase code point, the UTF-8 encodings of the other code points that lowercase to it, excluding
     * those that only differ in ASCII case. Built from {@link Character#toLowerCase(int)} over every code point, so it
     * follows the JDK's Unicode version.
     */
    private static final Map<Integer, byte[][]> OTHER_SPELLINGS;

    /** {@link #OTHER_SPELLINGS} for ASCII, as an array lookup. */
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
     * Every spelling of {@code term}, or null if there are more than {@code most} or if {@code term} is not valid
     * UTF-8. An ASCII term has a single spelling. Characters with a single spelling are appended in place; only those
     * with several copy the spellings built so far, which the {@code most} limit bounds, so the cost is linear in the
     * length of the term.
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

    /** Folds a spelling the way the needle search does, to merge spellings it does not tell apart. */
    private static BytesRef fold(BytesRef bytes) {
        final byte[] folded = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            folded[i] = (byte) (bytes.bytes[bytes.offset + i] | 0x20);
        }
        return new BytesRef(folded);
    }
}
