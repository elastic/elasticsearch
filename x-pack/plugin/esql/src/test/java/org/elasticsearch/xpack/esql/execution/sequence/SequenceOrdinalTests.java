/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;

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

    private static SequenceOrdinal ordinal() {
        return new SequenceOrdinal(randomLong(), randomIntBetween(0, 999_999), randomNonNegativeLong());
    }

    private static BigInteger timestamp(SequenceOrdinal ordinal) {
        return BigInteger.valueOf(ordinal.epochMillis())
            .multiply(BigInteger.valueOf(1_000_000))
            .add(BigInteger.valueOf(ordinal.nanoOfMilli()));
    }
}
