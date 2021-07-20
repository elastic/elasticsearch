/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.search.aggregations.bucket.terms;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(1_000_000)
@State(Scope.Benchmark)
public class QuickLongKeyedBucketOrdsBenchmark {
    private static final long LIMIT = 1_000_000;
    /**
     * The number of distinct values to add to the buckets.
     */
    private static final long DISTINCT_VALUES = 210;
    /**
     * The number of buckets to create in the {@link #multiBucket} case.
     * <p>
     * If this is not relatively prime to {@link #DISTINCT_VALUES_IN_BUCKETS}
     * then the values won't be scattered evenly across the buckets.
     */
    private static final long DISTINCT_BUCKETS = 21;
    /**
     * Number of distinct values to add to values within buckets.
     */
    private static final long DISTINCT_VALUES_IN_BUCKETS = 10;

    private final PageCacheRecycler recycler = new PageCacheRecycler(Settings.EMPTY);
    private final BigArrays bigArrays = new BigArrays(recycler, null, "REQUEST");

    /**
     * Force loading all of the implementations just for extra paranoia's sake.
     * We really don't want the JVM to be able to eliminate one of them just
     * because we don't use it in the particular benchmark. That is totally a
     * thing it'd do. It is sneaky.
     */
    @Setup
    public void forceLoadClasses(Blackhole bh) {
        bh.consume(LongKeyedBucketOrds.FromSingle.class);
        bh.consume(LongKeyedBucketOrds.FromMany.class);
        bh.consume(LongKeyedBucketOrds.FromManySmall.class);
    }

    /**
     * Emulates an aggregation that collects from many buckets with a known
     * bounds on the values.
     */
    @Benchmark
    public void multiBucketManySmall(Blackhole bh) {
        try (LongKeyedBucketOrds ords = new LongKeyedBucketOrds.FromManySmall(bigArrays, 5)) {
            multiBucket(bh, ords);
        }
    }

    private void multiBucket(Blackhole bh, LongKeyedBucketOrds ords) {
        for (long i = 0; i < LIMIT; i++) {
            ords.add(i % DISTINCT_BUCKETS, i % DISTINCT_VALUES_IN_BUCKETS);
        }
        if (ords.size() != DISTINCT_VALUES) {
            throw new IllegalArgumentException("Expected [" + DISTINCT_VALUES + "] but found [" + ords.size() + "]");
        }
        bh.consume(ords);
    }
}
