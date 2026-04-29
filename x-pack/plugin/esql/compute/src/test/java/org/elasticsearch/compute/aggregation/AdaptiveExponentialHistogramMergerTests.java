/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AdaptiveExponentialHistogramMergerTests extends ESTestCase {

    private static ExponentialHistogramCircuitBreaker histoBreaker(CircuitBreaker breaker) {
        return bytesAllocated -> {
            if (bytesAllocated < 0) {
                breaker.addWithoutBreaking(bytesAllocated);
            } else {
                breaker.addEstimateBytesAndMaybeBreak(bytesAllocated, "adaptive-histo-test");
            }
        };
    }

    private static ExponentialHistogram createHistogramWithBuckets(int bucketCount) {
        ExponentialHistogramBuilder builder = ExponentialHistogram.builder(10, ExponentialHistogramCircuitBreaker.noop());
        for (int i = 1; i <= bucketCount; i++) {
            builder.setPositiveBucket(i, i);
        }
        return builder.build();
    }

    /**
     * Verifies basic delegation: create, add, get, close all work without memory pressure.
     */
    public void testBasicDelegation() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(10));
        AtomicInteger reductionCount = new AtomicInteger();

        try (
            var factory = new AdaptiveExponentialHistogramMerger.Factory(
                breaker,
                histoBreaker(breaker),
                ExponentialHistogramMerger.DEFAULT_MAX_HISTOGRAM_BUCKETS,
                20,
                0.90,
                reductionCount::incrementAndGet
            )
        ) {
            ExponentialHistogram input = createHistogramWithBuckets(10);
            try (var merger = factory.createMerger()) {
                merger.add(input);
                ExponentialHistogram result = merger.get();
                assertThat(result.positiveBuckets().valueCount(), equalTo(input.positiveBuckets().valueCount()));
            }
        }
        assertThat(reductionCount.get(), equalTo(0));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Under memory pressure, the factory should reduce the bucket limit and invoke the callback exactly once.
     */
    public void testReducesBucketLimitUnderMemoryPressure() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(10));
        AtomicInteger reductionCount = new AtomicInteger();
        int startingBucketLimit = ExponentialHistogramMerger.DEFAULT_MAX_HISTOGRAM_BUCKETS;
        int minimumBucketLimit = 20;

        ExponentialHistogram largeHistogram = createHistogramWithBuckets(100);
        ExponentialHistogram reduced = ExponentialHistogram.merge(
            minimumBucketLimit,
            ExponentialHistogramCircuitBreaker.noop(),
            largeHistogram
        );
        int numGroups = 10_000;

        try (
            var factory = new AdaptiveExponentialHistogramMerger.Factory(
                breaker,
                histoBreaker(breaker),
                startingBucketLimit,
                minimumBucketLimit,
                0.90,
                reductionCount::incrementAndGet
            )
        ) {
            AdaptiveExponentialHistogramMerger[] mergers = new AdaptiveExponentialHistogramMerger[numGroups];
            try {
                for (int i = 0; i < numGroups; i++) {
                    mergers[i] = factory.createMerger();
                    mergers[i].add(largeHistogram);
                }
                for (int i = 0; i < numGroups; i++) {
                    assertThat(mergers[i].get(), equalTo(reduced));
                }
            } finally {
                for (AdaptiveExponentialHistogramMerger m : mergers) {
                    if (m != null) {
                        m.close();
                    }
                }
            }
        }
        assertThat(reductionCount.get(), equalTo(1));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * With a noop breaker (limit is non-positive), no reduction should occur regardless of usage.
     */
    public void testNoReductionWithNoopBreaker() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
        AtomicInteger reductionCount = new AtomicInteger();

        try (
            var factory = new AdaptiveExponentialHistogramMerger.Factory(
                breaker,
                histoBreaker(breaker),
                ExponentialHistogramMerger.DEFAULT_MAX_HISTOGRAM_BUCKETS,
                20,
                0.90,
                reductionCount::incrementAndGet
            )
        ) {
            ExponentialHistogram input = createHistogramWithBuckets(50);
            try (var merger = factory.createMerger()) {
                merger.add(input);
            }
        }
        assertThat(reductionCount.get(), equalTo(0));
    }

    /**
     * Tests that closing mergers in the middle of the linked list correctly unlinks them.
     */
    public void testPartialCloseAndReduction() {
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(10));
        AtomicInteger reductionCount = new AtomicInteger();
        int minimumBucketLimit = 20;

        ExponentialHistogram largeHistogram = createHistogramWithBuckets(100);
        ExponentialHistogram reduced = ExponentialHistogram.merge(
            minimumBucketLimit,
            ExponentialHistogramCircuitBreaker.noop(),
            largeHistogram
        );

        try (
            var factory = new AdaptiveExponentialHistogramMerger.Factory(
                breaker,
                histoBreaker(breaker),
                ExponentialHistogramMerger.DEFAULT_MAX_HISTOGRAM_BUCKETS,
                minimumBucketLimit,
                0.90,
                reductionCount::incrementAndGet
            )
        ) {
            var m1 = factory.createMerger();
            var m2 = factory.createMerger();
            var m3 = factory.createMerger();
            m1.add(largeHistogram);
            m2.add(largeHistogram);
            m3.add(largeHistogram);

            // close the middle one
            m2.close();

            // fill up to trigger reduction — m1 and m3 should be replaced
            AdaptiveExponentialHistogramMerger[] more = new AdaptiveExponentialHistogramMerger[10_000];
            try {
                for (int i = 0; i < more.length; i++) {
                    more[i] = factory.createMerger();
                    more[i].add(largeHistogram);
                }
                assertThat(m1.get(), equalTo(reduced));
                assertThat(m3.get(), equalTo(reduced));
            } finally {
                m1.close();
                m3.close();
                for (AdaptiveExponentialHistogramMerger m : more) {
                    if (m != null) {
                        m.close();
                    }
                }
            }
        }
        assertThat(reductionCount.get(), equalTo(1));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * The bucket limit should never go below the configured minimum even under extreme pressure.
     */
    public void testBucketLimitNeverGoesBelowMinimum() {
        int minimumBucketLimit = 40;
        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(5));
        AtomicInteger reductionCount = new AtomicInteger();

        ExponentialHistogram largeHistogram = createHistogramWithBuckets(100);

        try (
            var factory = new AdaptiveExponentialHistogramMerger.Factory(
                breaker,
                histoBreaker(breaker),
                ExponentialHistogramMerger.DEFAULT_MAX_HISTOGRAM_BUCKETS,
                minimumBucketLimit,
                0.90,
                reductionCount::incrementAndGet
            )
        ) {
            AdaptiveExponentialHistogramMerger[] mergers = new AdaptiveExponentialHistogramMerger[10_000];
            int created = 0;
            try {
                for (int i = 0; i < mergers.length; i++) {
                    mergers[i] = factory.createMerger();
                    mergers[i].add(largeHistogram);
                    created++;
                }
            } catch (CircuitBreakingException e) {
                // expected — we're using a small breaker
            } finally {
                for (int i = 0; i < created; i++) {
                    if (mergers[i] != null) {
                        mergers[i].close();
                    }
                }
            }
            assertThat(created, greaterThan(0));
        }
        assertThat(reductionCount.get(), lessThanOrEqualTo(1));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * With a cranky breaker that randomly trips, we should never leak memory.
     */
    public void testNoMemoryLeakWithCrankyBreaker() {
        CircuitBreaker breaker = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        ExponentialHistogram input = createHistogramWithBuckets(50);

        assertThrows(CircuitBreakingException.class, () -> {
            while (true) {
                try (
                    var factory = new AdaptiveExponentialHistogramMerger.Factory(
                        breaker,
                        histoBreaker(breaker),
                        ExponentialHistogramMerger.DEFAULT_MAX_HISTOGRAM_BUCKETS,
                        20,
                        0.90,
                        () -> {}
                    )
                ) {
                    for (int i = 0; i < 1_000; i++) {
                        try (var merger = factory.createMerger()) {
                            merger.add(input);
                        }
                    }
                }
            }
        });
        assertThat(breaker.getUsed(), equalTo(0L));
    }
}
