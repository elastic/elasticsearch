/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;

/**
 * A wrapper around {@link ExponentialHistogramMerger} that automatically reduces the bucket limit
 * when memory pressure exceeds a configured threshold. The bucket limit is halved repeatedly
 * until pressure drops below the threshold or the minimum bucket limit is reached.
 * <p>
 * Instances are tracked via an intrusive doubly-linked list anchored in the {@link Factory},
 * so that when the bucket limit is reduced, all live mergers can be replaced with ones using
 * the new, smaller limit.
 */
public final class AdaptiveExponentialHistogramMerger implements Accountable, Releasable {

    private ExponentialHistogramMerger delegate;
    private final Factory factory;

    // intrusive doubly-linked list pointers, guarded by the factory
    AdaptiveExponentialHistogramMerger prev;
    AdaptiveExponentialHistogramMerger next;

    private AdaptiveExponentialHistogramMerger(ExponentialHistogramMerger delegate, Factory factory) {
        this.delegate = delegate;
        this.factory = factory;
    }

    /**
     * Merges the given histogram into this merger. Checks memory pressure before the merge
     * and reduces the bucket limit if needed.
     */
    public void add(ExponentialHistogram histogram) {
        factory.checkMemoryPressureAndReduceAccuracyIfNeeded();
        delegate.add(histogram);
    }

    public ExponentialHistogram get() {
        return delegate.get();
    }

    public ReleasableExponentialHistogram getAndClear() {
        return delegate.getAndClear();
    }

    @Override
    public long ramBytesUsed() {
        return delegate.ramBytesUsed();
    }

    @Override
    public void close() {
        factory.unlink(this);
        Releasables.close(delegate);
    }

    /**
     * Factory for {@link AdaptiveExponentialHistogramMerger} instances that automatically reduces
     * the histogram bucket limit when memory pressure exceeds a configured threshold.
     * <p>
     * The bucket limit starts at {@code startingBucketLimit} and is halved each time pressure
     * is detected, down to {@code minimumBucketLimit}. When a reduction first occurs, the
     * provided {@code onFirstReduction} callback is invoked (e.g. to emit a warning).
     * <p>
     * All live mergers created by this factory are tracked in a linked list so their underlying
     * delegates can be replaced when the bucket limit changes.
     */
    public static final class Factory implements Accountable, Releasable {

        private ExponentialHistogramMerger.Factory delegateFactory;
        private final CircuitBreaker circuitBreaker;
        private final ExponentialHistogramCircuitBreaker histogramCircuitBreaker;
        private final int minimumBucketLimit;
        private final double memoryPressureThreshold;
        private final Runnable onFirstReduction;
        private boolean reduced;

        /**
         * Sentinel node for the doubly-linked list of live mergers. {@code sentinel.next} is the
         * first real node; {@code sentinel.prev} is the last.
         */
        private final AdaptiveExponentialHistogramMerger sentinel;

        /**
         * @param circuitBreaker the circuit breaker used to check memory pressure via
         *                       {@link CircuitBreaker#getUsed()} / {@link CircuitBreaker#getLimit()}
         * @param histogramCircuitBreaker the circuit breaker adapter passed to the underlying
         *                                {@link ExponentialHistogramMerger.Factory}
         * @param startingBucketLimit the initial maximum number of buckets per histogram
         * @param minimumBucketLimit the lowest bucket limit to reduce to
         * @param memoryPressureThreshold the fraction of the circuit breaker limit (0.0–1.0) above which
         *                                accuracy reduction is triggered
         * @param onFirstReduction callback invoked the first time accuracy is reduced (e.g. to emit a warning)
         */
        public Factory(
            CircuitBreaker circuitBreaker,
            ExponentialHistogramCircuitBreaker histogramCircuitBreaker,
            int startingBucketLimit,
            int minimumBucketLimit,
            double memoryPressureThreshold,
            Runnable onFirstReduction
        ) {
            this.circuitBreaker = circuitBreaker;
            this.histogramCircuitBreaker = histogramCircuitBreaker;
            this.minimumBucketLimit = minimumBucketLimit;
            this.memoryPressureThreshold = memoryPressureThreshold;
            this.onFirstReduction = onFirstReduction;
            this.delegateFactory = ExponentialHistogramMerger.createFactory(startingBucketLimit, histogramCircuitBreaker);
            this.sentinel = new AdaptiveExponentialHistogramMerger(null, this);
            this.sentinel.prev = this.sentinel;
            this.sentinel.next = this.sentinel;
        }

        /**
         * Creates a new merger, checking memory pressure first.
         */
        public AdaptiveExponentialHistogramMerger createMerger() {
            checkMemoryPressureAndReduceAccuracyIfNeeded();
            ExponentialHistogramMerger inner = delegateFactory.createMerger();
            AdaptiveExponentialHistogramMerger merger = new AdaptiveExponentialHistogramMerger(inner, this);
            link(merger);
            return merger;
        }

        /**
         * Checks if memory pressure exceeds the threshold and reduces accuracy in a loop
         * until pressure drops or the minimum bucket limit is reached.
         */
        void checkMemoryPressureAndReduceAccuracyIfNeeded() {
            while (delegateFactory.bucketLimit() > minimumBucketLimit) {
                long limit = circuitBreaker.getLimit();
                if (limit <= 0) {
                    return;
                }
                long used = circuitBreaker.getUsed();
                double pressure = (double) used / limit;
                if (pressure < memoryPressureThreshold) {
                    break;
                }
                reduceAccuracy();
            }
        }

        /**
         * Halves the bucket limit and re-merges all live merger contents into new delegates
         * created from a new underlying factory. Old delegates are closed one-at-a-time
         * to minimize peak memory.
         */
        private void reduceAccuracy() {
            int currentBucketLimit = delegateFactory.bucketLimit();
            int newBucketLimit = Math.max(minimumBucketLimit, currentBucketLimit / 2);
            if (newBucketLimit == currentBucketLimit) {
                return;
            }

            ExponentialHistogramMerger.Factory newFactory = ExponentialHistogramMerger.createFactory(
                newBucketLimit,
                histogramCircuitBreaker
            );
            ExponentialHistogramMerger.Factory oldFactory = this.delegateFactory;
            this.delegateFactory = newFactory;
            try {
                AdaptiveExponentialHistogramMerger current = sentinel.next;
                while (current != sentinel) {
                    ExponentialHistogramMerger newMerger = newFactory.createMerger();
                    try {
                        newMerger.add(current.delegate.get());
                        ExponentialHistogramMerger old = current.replaceDelegate(newMerger);
                        newMerger = old;
                    } finally {
                        Releasables.close(newMerger);
                    }
                    current = current.next;
                }
                newFactory = oldFactory;
            } finally {
                Releasables.close(newFactory);
            }

            if (reduced == false) {
                reduced = true;
                onFirstReduction.run();
            }
        }

        private void link(AdaptiveExponentialHistogramMerger merger) {
            merger.prev = sentinel.prev;
            merger.next = sentinel;
            sentinel.prev.next = merger;
            sentinel.prev = merger;
        }

        void unlink(AdaptiveExponentialHistogramMerger merger) {
            merger.prev.next = merger.next;
            merger.next.prev = merger.prev;
            merger.prev = null;
            merger.next = null;
        }

        @Override
        public long ramBytesUsed() {
            return delegateFactory.ramBytesUsed();
        }

        @Override
        public void close() {
            Releasables.close(delegateFactory);
        }
    }

    /**
     * Replaces the underlying merger delegate. The caller is responsible for ensuring
     * that the new delegate already contains the data from the old one.
     *
     * @return the old delegate (caller must close it)
     */
    ExponentialHistogramMerger replaceDelegate(ExponentialHistogramMerger newDelegate) {
        ExponentialHistogramMerger old = this.delegate;
        this.delegate = newDelegate;
        return old;
    }
}
