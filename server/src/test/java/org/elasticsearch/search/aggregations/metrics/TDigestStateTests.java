/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class TDigestStateTests extends ESTestCase {

    public void testMoreThan4BValues() {
        // Regression test for #19528
        // See https://github.com/tdunning/t-digest/pull/70/files#diff-4487072cee29b939694825647928f742R439
        try (TDigestState digest = TDigestState.create(breaker(), 100)) {
            for (int i = 0; i < 1000; ++i) {
                digest.add(randomDouble());
            }
            final int count = 1 << 29;
            for (int i = 0; i < 10; ++i) {
                digest.add(randomDouble(), count);
            }
            assertEquals(1000 + 10L * (1 << 29), digest.size());
            assertTrue(digest.size() > 2L * Integer.MAX_VALUE);
            final double[] quantiles = new double[] { 0, 0.1, 0.5, 0.9, 1, randomDouble() };
            Arrays.sort(quantiles);
            double prev = Double.NEGATIVE_INFINITY;
            for (double q : quantiles) {
                final double v = digest.quantile(q);
                logger.trace("q=" + q + ", v=" + v);
                assertThat(v, Matchers.either(Matchers.closeTo(prev, 0.0000001D)).or(Matchers.greaterThan(prev)));
                assertTrue("Unexpectedly low value: " + v, v >= 0.0);
                assertTrue("Unexpectedly high value: " + v, v <= 1.0);
                prev = v;
            }
        }
    }

    public void testEqualsHashCode() {
        try (
            TDigestState empty1 = new EmptyTDigestState();
            TDigestState empty2 = new EmptyTDigestState();
            TDigestState a = TDigestState.create(breaker(), 200);
            TDigestState b = TDigestState.create(breaker(), 100);
            TDigestState c = TDigestState.create(breaker(), 100);
        ) {

            assertEquals(empty1, empty2);
            assertEquals(empty1.hashCode(), empty2.hashCode());

            assertNotEquals(a, b);
            assertNotEquals(a.hashCode(), b.hashCode());

            assertNotEquals(a, c);
            assertNotEquals(a.hashCode(), c.hashCode());

            assertEquals(b, c);
            assertEquals(b.hashCode(), c.hashCode());

            for (int i = 0; i < 100; i++) {
                double value = randomDouble();
                a.add(value);
                b.add(value);
                c.add(value);
            }

            assertNotEquals(a, b);
            assertNotEquals(a.hashCode(), b.hashCode());

            assertNotEquals(a, c);
            assertNotEquals(a.hashCode(), c.hashCode());

            assertEquals(b, c);
            assertEquals(b.hashCode(), c.hashCode());

            b.add(randomDouble());
            c.add(randomDouble());

            assertNotEquals(b, c);
            assertNotEquals(b.hashCode(), c.hashCode());
        }
    }

    public void testHash() {
        final HashMap<String, TDigestState> map = new HashMap<>();
        final Set<TDigestState> set = new HashSet<>();
        try (
            TDigestState empty1 = new EmptyTDigestState();
            TDigestState empty2 = new EmptyTDigestState();
            TDigestState a = TDigestState.create(breaker(), 200);
            TDigestState b = TDigestState.create(breaker(), 100);
            TDigestState c = TDigestState.create(breaker(), 100);
        ) {

            a.add(randomDouble());
            b.add(randomDouble());
            c.add(randomDouble());
            expectThrows(UnsupportedOperationException.class, () -> empty1.add(randomDouble()));
            expectThrows(UnsupportedOperationException.class, () -> empty2.add(a));

            map.put("empty1", empty1);
            map.put("empty2", empty2);
            map.put("a", a);
            map.put("b", b);
            map.put("c", c);
            set.add(empty1);
            set.add(empty2);
            set.add(a);
            set.add(b);
            set.add(c);

            assertEquals(5, map.size());
            assertEquals(4, set.size());

            assertEquals(empty1, map.get("empty1"));
            assertEquals(empty2, map.get("empty2"));
            assertEquals(a, map.get("a"));
            assertEquals(b, map.get("b"));
            assertEquals(c, map.get("c"));

            assertTrue(set.stream().anyMatch(digest -> digest.equals(a)));
            assertTrue(set.stream().anyMatch(digest -> digest.equals(b)));
            assertTrue(set.stream().anyMatch(digest -> digest.equals(c)));
            assertTrue(set.stream().anyMatch(digest -> digest.equals(empty1)));
            assertTrue(set.stream().anyMatch(digest -> digest.equals(empty2)));
        }
    }

    public void testFactoryMethods() {
        try (
            TDigestState fast = TDigestState.create(breaker(), 100);
            TDigestState anotherFast = TDigestState.create(breaker(), 100);
            TDigestState accurate = TDigestState.createOptimizedForAccuracy(breaker(), 100);
            TDigestState anotherAccurate = TDigestState.createUsingParamsFrom(accurate);
        ) {

            for (int i = 0; i < 100; i++) {
                fast.add(i);
                anotherFast.add(i);
                accurate.add(i);
                anotherAccurate.add(i);
            }

            for (double p : new double[] { 0.1, 1, 10, 25, 50, 75, 90, 99, 99.9 }) {
                double q = p / 100;
                assertEquals(fast.quantile(q), accurate.quantile(q), 0.5);
                assertEquals(fast.quantile(q), anotherFast.quantile(q), 1e-5);
                assertEquals(accurate.quantile(q), anotherAccurate.quantile(q), 1e-5);
            }

            assertEquals(fast, anotherFast);
            assertEquals(accurate, anotherAccurate);
            assertNotEquals(fast, accurate);
            assertNotEquals(anotherFast, anotherAccurate);
        }
    }

    public void testUniqueCentroids() {
        assertUniqueCentroids(new double[0], new long[0], new HashMap<>(), 100);
        assertUniqueCentroids(new double[] { 4 }, new long[] { 6 }, Map.of(4.0, 6L), 100);
        assertUniqueCentroids(new double[] { -2.0, -1.0, -1.0, 0.0 }, new long[] { 3, 3, 2, 5 }, Map.of(-2.0, 3L, -1.0, 5L, 0.0, 5L), 100);
        assertUniqueCentroids(new double[] { 1, 1, 1, 1 }, new long[] { 1, 1, 1, 1 }, Map.of(1.0, 4L), 100);
        assertUniqueCentroids(new double[] { 1, 1, 1, 2 }, new long[] { 1, 1, 1, 1 }, Map.of(1.0, 3L, 2.0, 1L), 100);
        assertUniqueCentroids(new double[] { 1, 2, 2, 2 }, new long[] { 1, 1, 1, 1 }, Map.of(1.0, 1L, 2.0, 3L), 100);
        assertUniqueCentroids(new double[] { 1, 2, 3, 4 }, new long[] { 4, 3, 2, 1 }, Map.of(1.0, 4L, 2.0, 3L, 3.0, 2L, 4.0, 1L), 100);
        // We keep this low to avoid losing accuracy
        int maxRandomSize = randomIntBetween(10, 1000);
        SortedMap<Double, Long> expected = new TreeMap<>();
        double[] means = new double[maxRandomSize];
        long[] counts = new long[maxRandomSize];
        long totalCount = 0;
        for (int i = 0; i < maxRandomSize; ++i) {
            // We choose ints to be able to allow for more duplicates
            means[i] = randomIntBetween(0, maxRandomSize / 2);
            counts[i] = randomLongBetween(1, 100);
            totalCount += counts[i];
            if (expected.containsKey(means[i])) {
                expected.put(means[i], expected.get(means[i]) + counts[i]);
            } else {
                expected.put(means[i], counts[i]);
            }
        }
        // We use compression greater than our samples to ensure predictable centroids
        assertUniqueCentroids(means, counts, expected, totalCount + 1);
    }

    private void assertUniqueCentroids(double[] mean, long[] count, Map<Double, Long> expected, long compression) {
        try (TDigestState digest = TDigestState.create(breaker(), compression)) {
            for (int i = 0; i < mean.length; ++i) {
                digest.add(mean[i], count[i]);
            }
            Set<Double> seen = new HashSet<>();
            if (digest.size() == 0) {
                assertThat(digest.uniqueCentroids().hasNext(), equalTo(false));
            } else {
                Double previous = null;
                for (Iterator<Centroid> it = digest.uniqueCentroids(); it.hasNext();) {
                    Centroid centroid = it.next();
                    assertThat(seen.contains(centroid.mean()), equalTo(false));
                    assertThat(centroid.count(), equalTo(expected.get(centroid.mean())));
                    if (previous != null) {
                        assertThat(centroid.mean(), greaterThan(previous));
                    }
                    previous = centroid.mean();
                    seen.add(centroid.mean());
                }
                assertThat(seen.size(), equalTo(expected.size()));
            }
        }
    }

    private CircuitBreaker breaker() {
        return newLimitedBreaker(ByteSizeValue.ofMb(100));
    }
}
