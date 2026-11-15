/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;
import java.time.Instant;

/** Uses independent unbounded arithmetic to check ordering and span boundaries throughout the ordinary-date range. */
public class SequenceOrdinalTests extends ESTestCase {
    public void testOrderingAgainstUnboundedArithmetic() {
        for (int iteration = 0; iteration < 10_000; iteration++) {
            SequenceOrdinal left = ordinal();
            SequenceOrdinal right = randomBoolean()
                ? ordinal()
                : new SequenceOrdinal(left.epochMillis(), left.nanoOfMilli(), randomNonNegativeLong());
            int expected = timestamp(left).compareTo(timestamp(right));
            if (expected == 0) {
                expected = Long.compare(left.shardDoc(), right.shardDoc());
            }
            assertEquals(Integer.signum(expected), Integer.signum(left.compareTo(right)));
        }
    }

    public void testMaxspanAgainstUnboundedArithmetic() {
        for (int iteration = 0; iteration < 10_000; iteration++) {
            SequenceOrdinal start = ordinal();
            SequenceOrdinal end = randomBoolean()
                ? ordinal()
                : new SequenceOrdinal(start.epochMillis(), between(0, 999_999), randomNonNegativeLong());
            if (timestamp(start).compareTo(timestamp(end)) > 0) {
                SequenceOrdinal temporary = start;
                start = end;
                end = temporary;
            }
            long span = randomNonNegativeLong();
            boolean expected = timestamp(end).subtract(timestamp(start)).compareTo(BigInteger.valueOf(span)) > 0;
            assertEquals(expected, end.exceedsSpan(start, span));
        }
    }

    public void testSpanIgnoresTheDocumentTieBreaker() {
        SequenceOrdinal start = new SequenceOrdinal(-1, 999_999, 0);
        SequenceOrdinal end = new SequenceOrdinal(0, 0, Long.MAX_VALUE);
        assertFalse(end.exceedsSpan(start, 1));
        assertTrue(end.exceedsSpan(start, 0));
        assertFalse(new SequenceOrdinal(start.epochMillis(), start.nanoOfMilli(), Long.MAX_VALUE).exceedsSpan(start, 0));
    }

    public void testUpperTimeBoundAgainstUnboundedArithmetic() {
        long[] millis = { Long.MIN_VALUE, Long.MIN_VALUE + 1, -1_000, -2, -1, 0, 1, 1_000, Long.MAX_VALUE - 1, Long.MAX_VALUE };
        int[] fractions = { 0, 1, 499_999, 999_998, 999_999 };
        long[] spans = { 1, 2, 999_999, 1_000_000, 1_000_001, Long.MAX_VALUE };
        for (long timestamp : millis) {
            for (int fraction : fractions) {
                for (long span : spans) {
                    assertUpperTimeBound(new SequenceOrdinal(timestamp, fraction, randomNonNegativeLong()), span);
                }
            }
        }
        for (int iteration = 0; iteration < 10_000; iteration++) {
            long span = randomNonNegativeLong();
            assertUpperTimeBound(ordinal(), span == 0 ? 1 : span);
        }
    }

    public void testUpperTimeBoundIgnoresTheDocumentTieBreaker() {
        SequenceOrdinal start = new SequenceOrdinal(-1, 999_999, 0);
        assertEquals(Instant.EPOCH, start.upperTimeBound(1));
        assertEquals(start.upperTimeBound(1), new SequenceOrdinal(-1, 999_999, Long.MAX_VALUE).upperTimeBound(1));
    }

    private static void assertUpperTimeBound(SequenceOrdinal start, long span) {
        BigInteger expected = timestamp(start).add(BigInteger.valueOf(span));
        BigInteger maximum = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(1_000_000)).add(BigInteger.valueOf(999_999));
        Instant actual = start.upperTimeBound(span);
        if (expected.compareTo(maximum) > 0) {
            assertNull("ordinary date overflow must leave the upper range open", actual);
        } else {
            assertNotNull(actual);
            BigInteger actualNanos = BigInteger.valueOf(actual.getEpochSecond())
                .multiply(BigInteger.valueOf(1_000_000_000))
                .add(BigInteger.valueOf(actual.getNano()));
            assertEquals(expected, actualNanos);
        }
    }

    private static SequenceOrdinal ordinal() {
        return new SequenceOrdinal(randomLong(), randomIntBetween(0, 999_999), randomNonNegativeLong());
    }

    private static BigInteger timestamp(SequenceOrdinal ordinal) {
        return BigInteger.valueOf(ordinal.epochMillis())
            .multiply(BigInteger.valueOf(1_000_000))
            .add(BigInteger.valueOf(ordinal.nanoOfMilli()));
    }
}
