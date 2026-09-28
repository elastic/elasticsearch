/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomUnicodeOfCodepointLengthBetween;

/**
 * Generates individual keyword literals for a duel corpus. This class owns value content only; document
 * shapes (single, multi, sparse, nulls, and so on) are the responsibility of {@link KeywordScenario}. All
 * randomness flows through {@link org.elasticsearch.test.ESTestCase} so values reproduce from a test seed.
 */
public final class KeywordValues {

    /**
     * A small fixed vocabulary. A low cardinality is deliberate: reusing the same values across many
     * documents builds the dense and sparse doc-values blocks that keyword codecs encode differently.
     */
    public static final List<String> THEMED = List.of(
        "alpha",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliet"
    );

    private static final List<String> PUNCTUATION = List.of(" ", ",", ".", "-", "_", ":", "/", "\t");

    private KeywordValues() {}

    /**
     * @return a value drawn from the fixed low-cardinality vocabulary.
     */
    public static String themed() {
        return randomFrom(THEMED);
    }

    /**
     * @return a short ASCII value with a randomized length.
     */
    public static String shortAscii() {
        return randomAlphaOfLengthBetween(1, 8);
    }

    /**
     * @return a value that embeds punctuation and whitespace, which a keyword field stores verbatim.
     */
    public static String punctuated() {
        return themed() + randomFrom(PUNCTUATION) + shortAscii();
    }

    /**
     * @return a value made of unicode code points outside the ASCII range.
     */
    public static String unicode() {
        return randomUnicodeOfCodepointLengthBetween(1, 8);
    }

    /**
     * @return a long value, exercising the large-value path of a keyword codec. Capped below 1000 bytes so it
     *         stays within the term-query limit and can be probed by term and terms queries.
     */
    public static String longValue() {
        return randomAlphaOfLengthBetween(256, 900);
    }

    /**
     * @param ordinal a caller-supplied ordinal
     * @return a value unique to {@code ordinal}, used to build high-cardinality corpora.
     */
    public static String unique(long ordinal) {
        return "hc-" + ordinal + "-" + randomAlphaOfLengthBetween(1, 4);
    }

    /**
     * @return an empty string. A term query on {@code ""} rewrites to {@code BinaryDocValuesLengthQuery}, a path
     *         that is distinct from all non-empty terms. Scenarios that include empty strings alongside nulls
     *         verify the codec keeps them apart: {@code [null]} and {@code [""]} must match disjoint doc sets.
     */
    public static String emptyString() {
        return "";
    }

    /**
     * @return a value chosen from any of the generators above, mixing shapes within one corpus.
     */
    public static String any() {
        return switch (randomInt(4)) {
            case 0 -> themed();
            case 1 -> shortAscii();
            case 2 -> punctuated();
            case 3 -> unicode();
            case 4 -> longValue();
            default -> throw new AssertionError("unreachable");
        };
    }
}
