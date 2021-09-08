/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.function.scalar;

import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.function.Function;

public class NullSafeSortTests extends ESTestCase {

    public void testLongToOrderPreservingString() {
        assertEquals("1               0", NullSafeSort.longToOrderPreservingString(0));
        assertEquals("1               1", NullSafeSort.longToOrderPreservingString(1));
        assertEquals("17fffffffffffffff", NullSafeSort.longToOrderPreservingString(Long.MAX_VALUE));
        assertEquals("07fffffffffffffff", NullSafeSort.longToOrderPreservingString(-1));
        assertEquals("0               0", NullSafeSort.longToOrderPreservingString(Long.MIN_VALUE));
    }

    public void testLongToOrderPreservingStringPreservesOrder() {
        for (int i = 0; i < 100; i++) {
            long a = randomInterestingLong();
            long b = randomInterestingLong();

            String sa = NullSafeSort.longToOrderPreservingString(a);
            String sb = NullSafeSort.longToOrderPreservingString(b);

            String msg = String.format(Locale.ROOT, "a = %s, sa = [%s], b = %s, sb = [%s]", a, sa, b, sb);

            assertEquals(msg, a < b, sa.compareTo(sb) < 0);
            assertEquals(msg, a > b, sa.compareTo(sb) > 0);
        }
    }

    private long randomInterestingLong() {
        return randomFrom(Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L, 2L, randomLong());
    }

    public void testDoubleToOrderPreservingString() {
        assertEquals("107ffffc011               0", NullSafeSort.doubleToOrderPreservingString(0));
        assertEquals("11       01               0", NullSafeSort.doubleToOrderPreservingString(1));
        assertEquals("11     3ff1   fffffffffffff", NullSafeSort.doubleToOrderPreservingString(Double.MAX_VALUE));
        assertEquals("01       01               0", NullSafeSort.doubleToOrderPreservingString(-1));
        assertEquals("107ffffc011               1", NullSafeSort.doubleToOrderPreservingString(Double.MIN_VALUE));
    }

    public void testDoubleToOrderPreservingStringPreservesOrder() {
        for (int i = 0; i < 1000; i++) {
            double a = randomInterestingDouble();
            double b = randomInterestingDouble();

            String sa = NullSafeSort.doubleToOrderPreservingString(a);
            String sb = NullSafeSort.doubleToOrderPreservingString(b);

            String msg = String.format(Locale.ROOT, "a = %s, sa = [%s], b = %s, sb = [%s]", a, sa, b, sb);

            assertEquals(msg, a < b, sa.compareTo(sb) < 0);
            assertEquals(msg, a > b, sa.compareTo(sb) > 0);
        }
    }

    private double randomInterestingDouble() {
        double d = randomFrom(
            Double.MAX_VALUE,
            -Double.MAX_VALUE,
            Double.MIN_VALUE,
            -Double.MIN_VALUE,
            bitwiseNeighbour(-1d, -1),
            -1d,
            bitwiseNeighbour(-1d, 1),
            bitwiseNeighbour(0d, -1),
            -0d,
            0d,
            bitwiseNeighbour(0d, 1),
            bitwiseNeighbour(1d, -1),
            1d,
            bitwiseNeighbour(1d, 1),
            2d,
            Math.pow(2, randomInt()),
            Double.longBitsToDouble(randomLong())
        );

        // ES does not store NaN, and thankfully we do not have to consider it here.
        return Double.isNaN(d) ? randomInterestingDouble() : d;
    }

    private double bitwiseNeighbour(double d, long steps) {
        return Double.longBitsToDouble(Double.doubleToRawLongBits(d) + steps);
    }

    public void testInstantToOrderPreservingStringPreservesOrder() {
        for (int i = 0; i < 10; i++) {
            Instant a = Instant.ofEpochMilli(randomInterestingLong());
            Instant b = randomFrom(Instant.ofEpochMilli(randomInterestingLong()), a, a.plusNanos(1));

            assertOrder(a, b, NullSafeSort::instantToOrderPreservingString);
        }
    }

    public void testZonedDateTimeToOrderPreservingStringPreservesOrder() {
        for (int i = 0; i < 100; i++) {
            ZonedDateTime a = Instant.ofEpochMilli(randomInterestingLong()).atZone(randomZone());
            ZonedDateTime b = randomFrom(
                Instant.ofEpochMilli(randomInterestingLong()).atZone(randomZone()),
                a.withZoneSameLocal(randomZone()),
                a.withZoneSameInstant(randomZone())
            );

            assertOrder(a, b, NullSafeSort::zonedDateTimeToOrderPreservingString);
        }
    }

    public void testBooleanToOrderPreservingStringPreservesOrder() {
        for (int i = 0; i < 10; i++) {
            Boolean a = randomBoolean();
            Boolean b = randomBoolean();

            assertOrder(a, b, NullSafeSort::boolToOrderPreservingString);
        }
    }

    public void testNullableToOrderPreservingStringWithNullsLeast() {
        String s = randomAlphaOfLength(5);

        assertTrue(
            NullSafeSort.nullableToOrderPreservingString(null, true).compareTo(NullSafeSort.nullableToOrderPreservingString(s, true)) < 0
        );
        assertEquals(
            0,
            NullSafeSort.nullableToOrderPreservingString(null, true).compareTo(NullSafeSort.nullableToOrderPreservingString(null, true))
        );
        assertEquals(
            0,
            NullSafeSort.nullableToOrderPreservingString(s, true).compareTo(NullSafeSort.nullableToOrderPreservingString(s, true))
        );
    }

    public void testNullableToOrderPreservingStringWithoutNullsLeast() {
        String s = randomAlphaOfLength(5);

        assertTrue(
            NullSafeSort.nullableToOrderPreservingString(null, false).compareTo(NullSafeSort.nullableToOrderPreservingString(s, false)) > 0
        );
        assertEquals(
            0,
            NullSafeSort.nullableToOrderPreservingString(null, false).compareTo(NullSafeSort.nullableToOrderPreservingString(null, false))
        );
        assertEquals(
            0,
            NullSafeSort.nullableToOrderPreservingString(s, false).compareTo(NullSafeSort.nullableToOrderPreservingString(s, false))
        );
    }

    @SuppressWarnings("unchecked")
    private <U, T extends Comparable<U>> void assertOrder(T a, T b, Function<T, String> toOrderPreservingString) {
        String sa = toOrderPreservingString.apply(a);
        String sb = toOrderPreservingString.apply(b);

        String msg = String.format(Locale.ROOT, "a = %s, sa = [%s], b = %s, sb = [%s]", a, sa, b, sb);

        assertEquals(msg, a.compareTo((U) b) < 0, sa.compareTo(sb) < 0);
    }

}
