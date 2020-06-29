/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

public class TTestStatsBuilder implements Releasable {

    private LongArray counts;
    private DoubleArray sums;
    private DoubleArray compensations;
    private DoubleArray sumOfSqrs;
    private DoubleArray sumOfSqrCompensations;

    TTestStatsBuilder(BigArrays bigArrays) {
        counts = bigArrays.newLongArray(1, true);
        sums = bigArrays.newDoubleArray(1, true);
        compensations = bigArrays.newDoubleArray(1, true);
        sumOfSqrs = bigArrays.newDoubleArray(1, true);
        sumOfSqrCompensations = bigArrays.newDoubleArray(1, true);
    }

    public TTestStats get(long bucket) {
        return new TTestStats(counts.get(bucket), sums.get(bucket), sumOfSqrs.get(bucket));
    }

    public long build(long bucket) {
        return counts.get(bucket);
    }

    public long getSize() {
        return counts.size();
    }

    public void grow(BigArrays bigArrays, long buckets) {
        if (buckets >= counts.size()) {
            long overSize = BigArrays.overSize(buckets);
            counts = bigArrays.resize(counts, overSize);
            sums = bigArrays.resize(sums, overSize);
            compensations = bigArrays.resize(compensations, overSize);
            sumOfSqrs = bigArrays.resize(sumOfSqrs, overSize);
            sumOfSqrCompensations = bigArrays.resize(sumOfSqrCompensations, overSize);
        }
    }

    public void addValue(CompensatedSum compSum, CompensatedSum compSumOfSqr, long bucket, double val) {
        counts.increment(bucket, 1);
        double sum = sums.get(bucket);
        double compensation = compensations.get(bucket);
        compSum.reset(sum, compensation);

        double sumOfSqr = sumOfSqrs.get(bucket);
        double sumOfSqrCompensation = sumOfSqrCompensations.get(bucket);
        compSumOfSqr.reset(sumOfSqr, sumOfSqrCompensation);

        compSum.add(val);
        compSumOfSqr.add(val * val);

        sums.set(bucket, compSum.value());
        compensations.set(bucket, compSum.delta());
        sumOfSqrs.set(bucket, compSumOfSqr.value());
        sumOfSqrCompensations.set(bucket, compSumOfSqr.delta());
    }

    @Override
    public void close() {
        Releasables.close(counts, sums, compensations, sumOfSqrs, sumOfSqrCompensations);
    }
}
