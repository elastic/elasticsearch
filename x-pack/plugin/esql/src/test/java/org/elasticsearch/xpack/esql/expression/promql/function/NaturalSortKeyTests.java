/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Encoding tests for {@link NaturalSortKey}. Byte order of encoded keys is checked against a
 * port of {@code facette/natsort.Compare}, skipping the pairs where that comparator is not a
 * total order (numerically equal but textually different digit runs, and the empty string).
 */
public class NaturalSortKeyTests extends ESTestCase {

    private static final Pattern CHUNKIFY = Pattern.compile("(\\d+|\\D+)");

    public void testPod2BeforePod10() {
        assertTrue(compareKeys("pod2", "pod10") < 0);
    }

    public void testA1BeforeAb() {
        assertTrue(compareKeys("a1", "ab") < 0);
    }

    public void testDigitBeforeLetter() {
        assertTrue(compareKeys("1", "a") < 0);
    }

    public void testPunctuationBeforeDigit() {
        assertTrue(compareKeys("!", "1") < 0);
    }

    public void testLeadingZerosTieNumericallyEqualRuns() {
        assertEquals(key("007"), key("7"));
        assertEquals(0, compareKeys("007", "7"));
    }

    public void testNumberBeforeNumberThenText() {
        assertTrue(compareKeys("1", "1a") < 0);
    }

    public void testEmptyStringSortsFirst() {
        assertTrue(compareKeys("", "a") < 0);
        assertTrue(compareKeys("", "1") < 0);
        assertTrue(compareKeys("", "!") < 0);
        assertEquals(key(""), new BytesRef(""));
    }

    public void testLeadingZerosShorterNumberStillFirst() {
        assertTrue(compareKeys("09", "10") < 0);
        assertTrue(compareKeys("001", "2") < 0);
    }

    public void testMultiByteUtf8CopiedVerbatim() {
        assertTrue(compareKeys("a1", "aé") < 0);
        assertTrue(compareKeys("café", "cafë") < 0);
        assertEquals(key("日本語"), new BytesRef("日本語"));
    }

    public void testLongDigitRunOrdersByNumericMagnitude() {
        String smaller = "1" + "0".repeat(20);
        String larger = "1" + "0".repeat(21);
        assertTrue(compareKeys(smaller, larger) < 0);
        assertTrue(compareKeys("pod" + "0".repeat(25) + "2", "pod" + "0".repeat(25) + "10") < 0);
    }

    public void testNoDigitFastPathReturnsInput() {
        BytesRef input = new BytesRef("hello");
        try (BreakingBytesRefBuilder scratch = scratch()) {
            assertSame(input, NaturalSortKey.encode(input, scratch));
        }
    }

    public void testAgreesWithNatsortOnNonDegeneratePairs() {
        int needed = atLeast(200);
        int compared = 0;
        int attempts = 0;
        while (compared < needed) {
            attempts++;
            if (attempts > needed * 50) {
                fail("could not collect " + needed + " non-degenerate pairs after " + attempts + " attempts");
            }
            String a = randomLabel();
            String b = randomLabel();
            boolean natsortAB = natsortCompare(a, b);
            boolean natsortBA = natsortCompare(b, a);
            if (natsortAB == natsortBA) {
                continue;
            }
            int cmp = compareKeys(a, b);
            if (natsortAB) {
                assertTrue("key(" + a + ") should precede key(" + b + ")", cmp < 0);
            } else {
                assertTrue("key(" + b + ") should precede key(" + a + ")", cmp > 0);
            }
            compared++;
        }
    }

    private static int compareKeys(String a, String b) {
        return key(a).compareTo(key(b));
    }

    private static BytesRef key(String s) {
        BytesRef input = new BytesRef(s);
        try (BreakingBytesRefBuilder scratch = scratch()) {
            return BytesRef.deepCopyOf(NaturalSortKey.encode(input, scratch));
        }
    }

    private static BreakingBytesRefBuilder scratch() {
        return new BreakingBytesRefBuilder(new NoopCircuitBreaker(CircuitBreaker.REQUEST), "natural_sort_key");
    }

    /**
     * Verbatim port of {@code facette/natsort.Compare}: chunkify with {@code (\d+|\D+)},
     * {@code Atoi} both chunks, equal integers continue, last-chunk length rules, else string
     * less-than. Digit-run overflow of {@code long} is treated as text, matching Go {@code Atoi}.
     */
    static boolean natsortCompare(String a, String b) {
        List<String> chunksA = chunkify(a);
        List<String> chunksB = chunkify(b);
        int nChunksA = chunksA.size();
        int nChunksB = chunksB.size();
        for (int i = 0; i < nChunksA; i++) {
            if (i >= nChunksB) {
                return false;
            }
            Long aInt = parseLongOrNull(chunksA.get(i));
            Long bInt = parseLongOrNull(chunksB.get(i));
            if (aInt != null && bInt != null) {
                if (aInt.longValue() == bInt.longValue()) {
                    if (i == nChunksA - 1) {
                        return true;
                    } else if (i == nChunksB - 1) {
                        return false;
                    }
                    continue;
                }
                return aInt < bInt;
            }
            if (chunksA.get(i).equals(chunksB.get(i))) {
                if (i == nChunksA - 1) {
                    return true;
                } else if (i == nChunksB - 1) {
                    return false;
                }
                continue;
            }
            return utf8Less(chunksA.get(i), chunksB.get(i));
        }
        return false;
    }

    private static List<String> chunkify(String s) {
        Matcher matcher = CHUNKIFY.matcher(s);
        List<String> chunks = new ArrayList<>();
        while (matcher.find()) {
            chunks.add(matcher.group());
        }
        return chunks;
    }

    private static Long parseLongOrNull(String chunk) {
        try {
            return Long.parseLong(chunk);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static boolean utf8Less(String a, String b) {
        return new BytesRef(a.getBytes(StandardCharsets.UTF_8)).compareTo(new BytesRef(b.getBytes(StandardCharsets.UTF_8))) < 0;
    }

    private static String randomLabel() {
        int chunks = randomIntBetween(0, 5);
        if (chunks == 0) {
            return randomBoolean() ? "" : randomAlphaOfLength(randomIntBetween(1, 8));
        }
        StringBuilder sb = new StringBuilder();
        boolean digit = randomBoolean();
        for (int i = 0; i < chunks; i++) {
            if (digit) {
                if (randomBoolean()) {
                    sb.append("0".repeat(randomIntBetween(0, 3)));
                }
                int len = randomIntBetween(1, 18);
                for (int d = 0; d < len; d++) {
                    sb.append((char) ('0' + randomIntBetween(0, 9)));
                }
            } else {
                int len = randomIntBetween(1, 6);
                for (int t = 0; t < len; t++) {
                    sb.append(randomTextChar());
                }
            }
            digit = digit == false;
        }
        return sb.toString();
    }

    private static char randomTextChar() {
        return switch (randomIntBetween(0, 4)) {
            case 0 -> randomAlphaOfLength(1).charAt(0);
            case 1 -> (char) randomIntBetween(32, 47);
            case 2 -> (char) randomIntBetween(58, 126);
            case 3 -> (char) randomIntBetween(0xA0, 0xFF);
            case 4 -> (char) randomIntBetween(0x100, 0x24F);
            default -> throw new AssertionError("unexpected random branch");
        };
    }
}
