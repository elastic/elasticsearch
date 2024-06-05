/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LongKeyedBucketOrdsTests extends ESTestCase {
    private final MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

    public void testExplicitCollectsFromSingleBucket() {
        collectsFromSingleBucketCase(LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE));
    }

    public void testSurpriseCollectsFromSingleBucket() {
        collectsFromSingleBucketCase(LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY));
    }

    public void testCollectsFromManyBuckets() {
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY)) {
            assertCollectsFromManyBuckets(ords, scaledRandomIntBetween(1, 10000), Long.MIN_VALUE, Long.MAX_VALUE);
        }
    }

    public void testCollectsFromManyBucketsSmall() {
        int owningBucketOrds = scaledRandomIntBetween(2, 10000);
        long maxValue = randomLongBetween(10000 / owningBucketOrds, 2 << (16 * 3));
        CardinalityUpperBound cardinality = CardinalityUpperBound.ONE.multiply(owningBucketOrds);
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.buildForValueRange(bigArrays, cardinality, 0, maxValue)) {
            assertCollectsFromManyBuckets(ords, owningBucketOrds, 0, maxValue);
        }
    }

    private void collectsFromSingleBucketCase(LongKeyedBucketOrds ords) {
        try {
            // Test a few explicit values
            assertThat(ords.add(0, 0), equalTo(0L));
            assertThat(ords.add(0, 1000), equalTo(1L));
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(0, 1000), equalTo(-2L));
            assertThat(ords.find(0, 0), equalTo(0L));
            assertThat(ords.find(0, 1000), equalTo(1L));

            // And some random values
            Set<Long> seen = new HashSet<>();
            seen.add(0L);
            seen.add(1000L);
            assertThat(ords.size(), equalTo(2L));
            long[] values = new long[scaledRandomIntBetween(1, 10000)];
            for (int i = 0; i < values.length; i++) {
                values[i] = randomValueOtherThanMany(seen::contains, ESTestCase::randomLong);
                seen.add(values[i]);
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.find(0, values[i]), equalTo(-1L));
                assertThat(ords.add(0, values[i]), equalTo(i + 2L));
                assertThat(ords.find(0, values[i]), equalTo(i + 2L));
                assertThat(ords.size(), equalTo(i + 3L));
                if (randomBoolean()) {
                    assertThat(ords.add(0, 0), equalTo(-1L));
                }
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.add(0, values[i]), equalTo(-1 - (i + 2L)));
            }

            // And the explicit values are still ok
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(0, 1000), equalTo(-2L));

            // Check counting values
            assertThat(ords.bucketsInOrd(0), equalTo(values.length + 2L));

            // Check iteration
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(0);
            assertTrue(ordsEnum.next());
            assertThat(ordsEnum.ord(), equalTo(0L));
            assertThat(ordsEnum.value(), equalTo(0L));
            assertTrue(ordsEnum.next());
            assertThat(ordsEnum.ord(), equalTo(1L));
            assertThat(ordsEnum.value(), equalTo(1000L));
            for (int i = 0; i < values.length; i++) {
                assertTrue(ordsEnum.next());
                assertThat(ordsEnum.ord(), equalTo(i + 2L));
                assertThat(ordsEnum.value(), equalTo(values[i]));
            }
            assertFalse(ordsEnum.next());

            assertThat(ords.maxOwningBucketOrd(), equalTo(0L));
        } finally {
            ords.close();
        }
    }

    private void assertCollectsFromManyBuckets(LongKeyedBucketOrds ords, int maxAllowedOwningBucketOrd, long minValue, long maxValue) {
        // Test a few explicit values
        assertThat(ords.add(0, 0), equalTo(0L));
        assertThat(ords.add(1, 0), equalTo(1L));
        assertThat(ords.add(0, 0), equalTo(-1L));
        assertThat(ords.add(1, 0), equalTo(-2L));
        assertThat(ords.size(), equalTo(2L));
        assertThat(ords.find(0, 0), equalTo(0L));
        assertThat(ords.find(1, 0), equalTo(1L));

        // And some random values
        Set<OwningBucketOrdAndValue> seen = new HashSet<>();
        seen.add(new OwningBucketOrdAndValue(0, 0));
        seen.add(new OwningBucketOrdAndValue(1, 0));
        OwningBucketOrdAndValue[] values = new OwningBucketOrdAndValue[scaledRandomIntBetween(1, 10000)];
        long maxOwningBucketOrd = Long.MIN_VALUE;
        for (int i = 0; i < values.length; i++) {
            values[i] = randomValueOtherThanMany(
                seen::contains,
                () -> new OwningBucketOrdAndValue(randomLongBetween(0, maxAllowedOwningBucketOrd), randomLongBetween(minValue, maxValue))
            );
            seen.add(values[i]);
            maxOwningBucketOrd = Math.max(maxOwningBucketOrd, values[i].owningBucketOrd);
        }
        for (int i = 0; i < values.length; i++) {
            assertThat(ords.find(values[i].owningBucketOrd, values[i].value), equalTo(-1L));
            assertThat(ords.add(values[i].owningBucketOrd, values[i].value), equalTo(i + 2L));
            assertThat(ords.find(values[i].owningBucketOrd, values[i].value), equalTo(i + 2L));
            assertThat(ords.size(), equalTo(i + 3L));
            if (randomBoolean()) {
                assertThat(ords.add(0, 0), equalTo(-1L));
            }
        }
        for (int i = 0; i < values.length; i++) {
            assertThat(ords.add(values[i].owningBucketOrd, values[i].value), equalTo(-1 - (i + 2L)));
        }

        // And the explicit values are still ok
        assertThat(ords.add(0, 0), equalTo(-1L));
        assertThat(ords.add(1, 0), equalTo(-2L));

        for (long owningBucketOrd = 0; owningBucketOrd <= maxAllowedOwningBucketOrd; owningBucketOrd++) {
            long expectedCount = 0;
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(owningBucketOrd);
            if (owningBucketOrd <= 1) {
                expectedCount++;
                assertTrue(ordsEnum.next());
                assertThat(ordsEnum.ord(), equalTo(owningBucketOrd));
                assertThat(ordsEnum.value(), equalTo(0L));
            }
            for (int i = 0; i < values.length; i++) {
                if (values[i].owningBucketOrd == owningBucketOrd) {
                    expectedCount++;
                    assertTrue(ordsEnum.next());
                    assertThat(ordsEnum.ord(), equalTo(i + 2L));
                    assertThat(ordsEnum.value(), equalTo(values[i].value));
                }
            }
            assertFalse(ordsEnum.next());

            assertThat(ords.bucketsInOrd(owningBucketOrd), equalTo(expectedCount));
        }
        assertFalse(ords.ordsEnum(randomLongBetween(maxOwningBucketOrd + 1, Long.MAX_VALUE)).next());
        assertThat(ords.bucketsInOrd(randomLongBetween(maxOwningBucketOrd + 1, Long.MAX_VALUE)), equalTo(0L));

        assertThat(ords.maxOwningBucketOrd(), equalTo(maxOwningBucketOrd));
    }

    public void testKeyIteratorSingleValue() {
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE)) {
            // Test a few explicit values
            assertThat(ords.add(0, 0), equalTo(0L));
            assertThat(ords.add(0, 1000), equalTo(1L));
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(0, 1000), equalTo(-2L));
            assertThat(ords.find(0, 0), equalTo(0L));
            assertThat(ords.find(0, 1000), equalTo(1L));

            // And some random values
            Set<Long> seen = new TreeSet<>();
            seen.add(0L);
            seen.add(1000L);
            assertThat(ords.size(), equalTo(2L));
            long[] values = new long[scaledRandomIntBetween(1, 10000)];
            for (int i = 0; i < values.length; i++) {
                values[i] = randomValueOtherThanMany(seen::contains, ESTestCase::randomLong);
                seen.add(values[i]);
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.find(0, values[i]), equalTo(-1L));
                assertThat(ords.add(0, values[i]), equalTo(i + 2L));
                assertThat(ords.find(0, values[i]), equalTo(i + 2L));
                assertThat(ords.size(), equalTo(i + 3L));
            }

            // For the single value case, the sorted iterator should exactly equal the values tree set iterator
            Iterator<Long> expected = seen.iterator();
            Iterator<Long> actual = ords.keyOrderedIterator(0);
            assertNotSame(expected, actual);
            while (expected.hasNext()) {
                assertThat(actual.hasNext(), is(true));
                long actualNext = actual.next();
                long expectedNext = expected.next();
                assertThat(actualNext, equalTo(expectedNext));
            }
            assertThat(actual.hasNext(), is(false));
        }
    }

    public void testKeyIteratormanyBuckets() {
        long maxAllowedOwningBucketOrd = scaledRandomIntBetween(1, 10000);
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY)) {
            Map<Long, TreeSet<Long>> expected = new HashMap<>();
            // Test a few explicit values
            assertThat(ords.add(0, 0), equalTo(0L));
            assertThat(ords.add(1, 0), equalTo(1L));
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(1, 0), equalTo(-2L));
            assertThat(ords.size(), equalTo(2L));
            assertThat(ords.find(0, 0), equalTo(0L));
            assertThat(ords.find(1, 0), equalTo(1L));

            Set<OwningBucketOrdAndValue> seen = new HashSet<>();
            seen.add(new OwningBucketOrdAndValue(0, 0));
            seen.add(new OwningBucketOrdAndValue(1, 0));

            expected.put(0L, new TreeSet<>());
            expected.get(0L).add(0L);
            expected.put(1L, new TreeSet<>());
            expected.get(1L).add(0L);

            OwningBucketOrdAndValue[] values = new OwningBucketOrdAndValue[scaledRandomIntBetween(1, 10000)];
            for (int i = 0; i < values.length; i++) {
                values[i] = randomValueOtherThanMany(
                    seen::contains,
                    () -> new OwningBucketOrdAndValue(
                        randomLongBetween(0, maxAllowedOwningBucketOrd),
                        randomLongBetween(minValue, maxValue)
                    )
                );
                seen.add(values[i]);
                if (expected.containsKey(values[i].owningBucketOrd) == false) {
                    expected.put(values[i].owningBucketOrd, new TreeSet<>());
                }
                expected.get(values[i].owningBucketOrd).add(values[i].value);
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.find(values[i].owningBucketOrd, values[i].value), equalTo(-1L));
                assertThat(ords.add(values[i].owningBucketOrd, values[i].value), equalTo(i + 2L));
                assertThat(ords.find(values[i].owningBucketOrd, values[i].value), equalTo(i + 2L));
                assertThat(ords.size(), equalTo(i + 3L));
            }

            for (Long owningBucketOrd : expected.keySet()) {
                Iterator<Long> expectedIterator = expected.get(owningBucketOrd).iterator();
                Iterator<Long> actualIterator = ords.keyOrderedIterator(owningBucketOrd);

                assertNotSame(expectedIterator, actualIterator);
                while (expectedIterator.hasNext()) {
                    assertThat(actualIterator.hasNext(), is(true));
                    long actualNext = actualIterator.next();
                    long expectedNext = expectedIterator.next();
                    assertThat(actualNext, equalTo(expectedNext));
                }
                assertThat(actualIterator.hasNext(), is(false));
            }
        }
    }

    public void testKeyIteratorManyBucketsSmall() {
        int maxAllowedOwningBucketOrd = scaledRandomIntBetween(2, 10000);
        long minValue = 0;
        long maxValue = randomLongBetween(10000 / maxAllowedOwningBucketOrd, 2 << (16 * 3));
        CardinalityUpperBound cardinality = CardinalityUpperBound.ONE.multiply(maxAllowedOwningBucketOrd);
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.buildForValueRange(bigArrays, cardinality, minValue, maxValue)) {
            assertTrue(ords instanceof LongKeyedBucketOrds.FromManySmall);
            Map<Long, TreeSet<Long>> expected = new HashMap<>();
            // Test a few explicit values
            assertThat(ords.add(0, 0), equalTo(0L));
            assertThat(ords.add(1, 0), equalTo(1L));
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(1, 0), equalTo(-2L));
            assertThat(ords.size(), equalTo(2L));
            assertThat(ords.find(0, 0), equalTo(0L));
            assertThat(ords.find(1, 0), equalTo(1L));

            Set<OwningBucketOrdAndValue> seen = new HashSet<>();
            seen.add(new OwningBucketOrdAndValue(0, 0));
            seen.add(new OwningBucketOrdAndValue(1, 0));

            expected.put(0L, new TreeSet<>());
            expected.get(0L).add(0L);
            expected.put(1L, new TreeSet<>());
            expected.get(1L).add(0L);

            OwningBucketOrdAndValue[] values = new OwningBucketOrdAndValue[scaledRandomIntBetween(1, 10000)];
            for (int i = 0; i < values.length; i++) {
                values[i] = randomValueOtherThanMany(
                    seen::contains,
                    () -> new OwningBucketOrdAndValue(
                        randomLongBetween(0, maxAllowedOwningBucketOrd),
                        randomLongBetween(minValue, maxValue)
                    )
                );
                seen.add(values[i]);
                if (expected.containsKey(values[i].owningBucketOrd) == false) {
                    expected.put(values[i].owningBucketOrd, new TreeSet<>());
                }
                expected.get(values[i].owningBucketOrd).add(values[i].value);
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.find(values[i].owningBucketOrd, values[i].value), equalTo(-1L));
                assertThat(ords.add(values[i].owningBucketOrd, values[i].value), equalTo(i + 2L));
                assertThat(ords.find(values[i].owningBucketOrd, values[i].value), equalTo(i + 2L));
                assertThat(ords.size(), equalTo(i + 3L));
            }

            for (Long owningBucketOrd : expected.keySet()) {
                Iterator<Long> expectedIterator = expected.get(owningBucketOrd).iterator();
                Iterator<Long> actualIterator = ords.keyOrderedIterator(owningBucketOrd);

                assertNotSame(expectedIterator, actualIterator);
                while (expectedIterator.hasNext()) {
                    assertThat(actualIterator.hasNext(), is(true));
                    long actualNext = actualIterator.next();
                    long expectedNext = expectedIterator.next();
                    assertThat(actualNext, equalTo(expectedNext));
                }
                assertThat(actualIterator.hasNext(), is(false));
            }
        }
    }

    private class OwningBucketOrdAndValue {
        private final long owningBucketOrd;
        private final long value;

        OwningBucketOrdAndValue(long owningBucketOrd, long value) {
            this.owningBucketOrd = owningBucketOrd;
            this.value = value;
        }

        @Override
        public String toString() {
            return owningBucketOrd + "/" + value;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            OwningBucketOrdAndValue other = (OwningBucketOrdAndValue) obj;
            return owningBucketOrd == other.owningBucketOrd && value == other.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(owningBucketOrd, value);
        }
    }
}
