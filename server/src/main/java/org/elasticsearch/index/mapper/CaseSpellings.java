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

import java.util.ArrayList;
import java.util.LinkedHashSet;
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
     * Characters that lower onto a character, held only for the characters that more than one lowers onto. Built
     * once, by asking of every character what it lowers onto, so that a change in what Unicode says cannot leave
     * this behind.
     */
    private static final Map<Integer, int[]> LOWERS_ONTO = loweringMap();

    private static Map<Integer, int[]> loweringMap() {
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
        final Map<Integer, int[]> built = new TreeMap<>();
        sources.forEach((lower, from) -> {
            final int[] all = new int[from.size()];
            for (int i = 0; i < all.length; i++) {
                all[i] = from.get(i);
            }
            built.put(lower, all);
        });
        return Map.copyOf(built);
    }

    /**
     * Every way a value could spell {@code term}, as patterns to look for, or null where there are more of them
     * than {@code most}. Spellings the comparison of ASCII case takes for the same are one pattern, and are made
     * one as each character is added rather than at the end: a term of ASCII would otherwise be counted in its
     * thousands on the way to the one pattern it comes to.
     */
    static List<BytesRef> of(BytesRef term, int most) {
        final String spelled = term.utf8ToString();
        List<String> spellings = List.of("");
        for (int offset = 0; offset < spelled.length();) {
            final int codePoint = spelled.codePointAt(offset);
            offset += Character.charCount(codePoint);
            final int[] lowersOnto = LOWERS_ONTO.get(codePoint);
            final List<String> grown = new ArrayList<>(spellings.size());
            final Set<BytesRef> apart = new LinkedHashSet<>();
            for (String spelling : spellings) {
                addIfApart(grown, apart, spelling + new String(Character.toChars(codePoint)));
            }
            if (lowersOnto != null) {
                for (String spelling : spellings) {
                    for (int from : lowersOnto) {
                        addIfApart(grown, apart, spelling + new String(Character.toChars(from)));
                    }
                }
            }
            if (grown.size() > most) {
                return null;
            }
            spellings = grown;
        }
        final List<BytesRef> patterns = new ArrayList<>(spellings.size());
        for (String spelling : spellings) {
            patterns.add(new BytesRef(spelling));
        }
        return patterns;
    }

    /** Keeps {@code spelling} where the comparison of ASCII case tells it from the ones kept already. */
    private static void addIfApart(List<String> kept, Set<BytesRef> apart, String spelling) {
        if (apart.add(fold(new BytesRef(spelling)))) {
            kept.add(spelling);
        }
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
