/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.search.aggregations.metrics.MemoryTrackingTDigestArrays;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.tdigest.TDigest;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.Iterator;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class BreakingTDigestHolderTests extends ESTestCase {

    public void testSetFromHolderCopiesData() {
        TDigestHolder source;
        do {
            source = randomStandaloneTDigestHolder();
        } while (source.centroidCount() == 0);
        Collection<Centroid> sourceCentroids = source.centroids();
        double min = source.getMin();
        double max = source.getMax();
        double sum = source.getSum();

        try (BreakingTDigestHolder copy = BreakingTDigestHolder.create(new NoopCircuitBreaker("test-breaker"))) {
            copy.set(source);
            assertThat(copy.accessor(), equalTo(source));

            // Mutating the source after set should not alter the copy.
            source.getEncodedDigest().bytes[source.getEncodedDigest().offset] = 42;
            source.reset(source.getEncodedDigest(), min + 1, max + 1, sum + 1, source.centroidCount() + 1);

            Collection<Centroid> copyCentroids = copy.accessor().centroids();
            assertThat(copyCentroids.size(), equalTo(sourceCentroids.size()));
            Iterator<Centroid> copyIt = copyCentroids.iterator();
            Iterator<Centroid> sourceIt = sourceCentroids.iterator();
            while (copyIt.hasNext() && sourceIt.hasNext()) {
                Centroid copyCentroid = copyIt.next();
                Centroid sourceCentroid = sourceIt.next();
                assertThat(copyCentroid.mean(), equalTo(sourceCentroid.mean()));
                assertThat(copyCentroid.count(), equalTo(sourceCentroid.count()));
            }

            assertThat(copy.accessor().getMin(), equalTo(min));
            assertThat(copy.accessor().getMax(), equalTo(max));
            assertThat(copy.accessor().getSum(), equalTo(sum));
        }
    }

    public void testSetFromTDigestReadView() {
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("test-breaker");
        MemoryTrackingTDigestArrays arrays = new MemoryTrackingTDigestArrays(breaker);
        try (
            TDigest digest = TDigest.createMergingDigest(arrays, 100.0);
            BreakingTDigestHolder holder = BreakingTDigestHolder.create(breaker)
        ) {
            long expectedCount = 0L;
            double expectedSum = randomDouble();
            double expectedMin = randomDouble();
            double expectedMax = randomDouble();

            int values = between(0, 50);
            for (int i = 0; i < values; i++) {
                double v = randomDoubleBetween(-1_000, 1_000, true);
                int count = between(1, 20);
                digest.add(v, count);
                expectedCount += count;
            }

            holder.set(digest, expectedSum, expectedMin, expectedMax);
            TDigestHolder view = holder.accessor();
            assertThat(view.size(), equalTo(expectedCount));
            assertThat(view.getSum(), equalTo(expectedSum));
            assertThat(view.getMin(), equalTo(expectedMin));
            assertThat(view.getMax(), equalTo(expectedMax));
        }
    }

    public void testCloseReleasesBreakerAccounting() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(4));
        assertThat(breaker.getUsed(), equalTo(0L));
        try (BreakingTDigestHolder holder = BreakingTDigestHolder.create(breaker)) {
            TDigestHolder source = randomStandaloneTDigestHolder();
            holder.set(source);
            assertThat(breaker.getUsed(), greaterThan(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testCrankyCircuitBreaker() {
        CircuitBreaker breaker = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        assertThrows(CircuitBreakingException.class, () -> {
            while (true) {
                // Loop until we throw
                try (BreakingTDigestHolder holder = BreakingTDigestHolder.create(breaker)) {
                    holder.set(randomStandaloneTDigestHolder());
                }
            }
        });
    }

    private TDigestHolder randomStandaloneTDigestHolder() {
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("random-holder");
        MemoryTrackingTDigestArrays arrays = new MemoryTrackingTDigestArrays(breaker);
        try (
            TDigest digest = TDigest.createMergingDigest(arrays, 100.0);
            BreakingTDigestHolder holder = BreakingTDigestHolder.create(breaker)
        ) {
            int values = between(5, 20);
            double sum = 0.0;
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < values; i++) {
                double v = randomDoubleBetween(-1_000, 1_000, true);
                int count = between(1, 10);
                digest.add(v, count);
                sum += v * count;
                min = Math.min(min, v);
                max = Math.max(max, v);
            }
            holder.set(digest, sum, min, max);
            TDigestHolder copy = new TDigestHolder();
            TDigestHolder view = holder.accessor();
            copy.reset(BytesRef.deepCopyOf(view.getEncodedDigest()), view.getMin(), view.getMax(), view.getSum(), view.size());
            return copy;
        }
    }
}
