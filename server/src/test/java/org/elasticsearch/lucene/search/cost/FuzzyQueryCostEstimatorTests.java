/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Locale;
import java.util.Random;

import static org.hamcrest.Matchers.greaterThan;

public class FuzzyQueryCostEstimatorTests extends ESTestCase {

    public void testZeroEditsReturnsZero() {
        assertEquals(0L, new FuzzyQueryCostEstimator(0, 0, 0, 0).estimate());
        assertEquals(0L, new FuzzyQueryCostEstimator(50, 1, 0, 0).estimate());
        assertEquals(0L, new FuzzyQueryCostEstimator(50, 1, 0, 5).estimate());
    }

    public void testEstimateIsMonotonicInTermLength() {
        long shorter = new FuzzyQueryCostEstimator(10, 10, 2, 0).estimate();
        long longer = new FuzzyQueryCostEstimator(50, 10, 2, 0).estimate();
        long longest = new FuzzyQueryCostEstimator(200, 10, 2, 0).estimate();
        assertTrue(shorter < longer);
        assertTrue(longer < longest);
    }

    public void testEstimateIsMonotonicInMaxEdits() {
        long e1 = new FuzzyQueryCostEstimator(20, 20, 1, 0).estimate();
        long e2 = new FuzzyQueryCostEstimator(20, 20, 2, 0).estimate();
        assertTrue("expected estimate to grow with maxEdits", e1 < e2);
    }

    public void testWideAlphabetEstimateIsHigherThanNarrow() {
        long narrow = new FuzzyQueryCostEstimator(20, 20, 2, 0).estimate();
        long wide = new FuzzyQueryCostEstimator(20, 200, 2, 0).estimate();
        assertTrue("wide alphabet (above WIDE_ALPHABET_THRESHOLD) must charge more than ASCII-shaped input", narrow < wide);
    }

    public void testNarrowAlphabetIsFlat() {
        long bd1 = new FuzzyQueryCostEstimator(20, 1, 2, 0).estimate();
        long bd26 = new FuzzyQueryCostEstimator(20, 26, 2, 0).estimate();
        long bd64 = new FuzzyQueryCostEstimator(20, 64, 2, 0).estimate();
        assertEquals(bd1, bd26);
        assertEquals(bd1, bd64);
    }

    public void testPrefixLengthShrinksEstimate() {
        long noPrefix = new FuzzyQueryCostEstimator(60, 60, 2, 0).estimate();
        long withPrefix = new FuzzyQueryCostEstimator(60, 60, 2, 5).estimate();
        assertTrue("prefix should reduce the suffix-driven cost", withPrefix < noPrefix);
    }

    public void testPrefixLongerThanTermIsClampedNotNegative() {
        long allPrefix = new FuzzyQueryCostEstimator(5, 5, 2, 100).estimate();
        assertEquals(FuzzyQueryCostEstimator.BASE_BYTES, allPrefix);
    }

    public void testConstructorRejectsNegativeArguments() {
        expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryCostEstimator(-1, 0, 1, 0));
        expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryCostEstimator(10, -1, 1, 0));
        expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryCostEstimator(10, 10, -1, 0));
        expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryCostEstimator(10, 10, 1, -1));
    }

    public void testConstructorRejectsMaxEditsAboveLuceneLimit() {
        expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryCostEstimator(10, 10, 99, 0));
    }

    public void testEstimateIsCeilingOnMeasuredAutomataRam() {
        int[] termLengths = { 5, 20, 50, 200, 600 };
        int[] maxEditsValues = { 1, 2 };
        int[] prefixLengths = { 0, 3 };
        Alphabet[] alphabets = Alphabet.values();
        boolean[] transpositionsValues = { true, false };

        long worstRatioMicros = 0;
        long bestRatioMicros = Long.MAX_VALUE;

        for (int termLength : termLengths) {
            for (int maxEdits : maxEditsValues) {
                for (int prefix : prefixLengths) {
                    for (Alphabet alphabet : alphabets) {
                        for (boolean transpositions : transpositionsValues) {

                            Random rnd = new Random(0xC0FFEEL ^ termLength ^ alphabet.ordinal());
                            String term = alphabet.generate(termLength, rnd);
                            byte[] utf8 = term.getBytes(StandardCharsets.UTF_8);
                            int distinctUtf8Bytes = countDistinctUtf8Bytes(utf8);

                            long estimated = new FuzzyQueryCostEstimator(utf8.length, distinctUtf8Bytes, maxEdits, prefix).estimate();
                            long measured = sumCompiledAutomataRamBytes(term, maxEdits, prefix, transpositions);

                            double ratio = measured == 0L ? Double.POSITIVE_INFINITY : (double) estimated / (double) measured;
                            long ratioMicros = measured == 0L ? Long.MAX_VALUE : Math.round(ratio * 1_000_000.0);
                            worstRatioMicros = Math.max(worstRatioMicros, ratioMicros);
                            bestRatioMicros = Math.min(bestRatioMicros, ratioMicros);

                            assertThat(
                                String.format(
                                    Locale.ROOT,
                                    "estimate must be a ceiling on measured ramBytesUsed "
                                        + "[termLen=%d, maxEdits=%d, prefix=%d, alphabet=%s, transpositions=%s, "
                                        + "estimated=%d, measured=%d, ratio=%.3f]",
                                    termLength,
                                    maxEdits,
                                    prefix,
                                    alphabet,
                                    transpositions,
                                    estimated,
                                    measured,
                                    ratio
                                ),
                                estimated,
                                greaterThan(measured)
                            );
                        }
                    }
                }
            }
        }
    }

    private static long sumCompiledAutomataRamBytes(String term, int maxEdits, int prefixLength, boolean transpositions) {
        long sum = 0L;
        for (int e = 0; e <= maxEdits; e++) {
            CompiledAutomaton ca = FuzzyQuery.getFuzzyAutomaton(term, e, prefixLength, transpositions);
            sum += ca.ramBytesUsed();
        }
        return sum;
    }

    private static int countDistinctUtf8Bytes(byte[] utf8) {
        BitSet seen = new BitSet(256);
        for (byte b : utf8) {
            seen.set(b & 0xff);
        }
        return seen.cardinality();
    }

    private enum Alphabet {
        SINGLE_CHAR {
            @Override
            String generate(int n, Random r) {
                return "a".repeat(n);
            }
        },
        ASCII_LETTERS {
            @Override
            String generate(int n, Random r) {
                StringBuilder sb = new StringBuilder(n);
                for (int i = 0; i < n; i++) {
                    sb.append((char) ('a' + r.nextInt(26)));
                }
                return sb.toString();
            }
        },
        UNICODE_BMP {
            @Override
            String generate(int n, Random r) {
                StringBuilder sb = new StringBuilder(n);
                for (int i = 0; i < n; i++) {
                    int cp;
                    do {
                        cp = r.nextInt(0xD800);
                    } while (Character.isISOControl(cp) || Character.isWhitespace(cp));
                    sb.appendCodePoint(cp);
                }
                return sb.toString();
            }
        };

        abstract String generate(int n, Random r);
    }
}
