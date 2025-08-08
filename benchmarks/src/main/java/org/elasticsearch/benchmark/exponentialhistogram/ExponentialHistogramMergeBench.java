/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.exponentialhistogram;

import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramGenerator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramMerger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Thread)
public class ExponentialHistogramMergeBench {

    @Param({ "1000", "2000", "5000" })
    int bucketCount;

    @Param({ "0.01", "0.1", "0.25", "0.5", "1.0", "2.0" })
    double mergedHistoSizeFactor;

    Random random;
    ExponentialHistogramMerger histoMerger;

    ExponentialHistogram[] toMerge = new ExponentialHistogram[10_000];

    int index;

    @Setup
    public void setUp() {
        random = ThreadLocalRandom.current();
        histoMerger = new ExponentialHistogramMerger(bucketCount);

        ExponentialHistogramGenerator initial = new ExponentialHistogramGenerator(bucketCount);
        for (int j = 0; j < bucketCount; j++) {
            initial.add(Math.pow(1.001, j));
        }
        ExponentialHistogram initialHisto = initial.get();
        int cnt = getBucketCount(initialHisto);
        if (cnt < bucketCount) {
            throw new IllegalArgumentException("Expected bucket count to be " + bucketCount + ", but was " + cnt);
        }
        histoMerger.add(initialHisto);

        int dataPointSize = (int) Math.round(bucketCount * mergedHistoSizeFactor);

        for (int i = 0; i < toMerge.length; i++) {
            ExponentialHistogramGenerator generator = new ExponentialHistogramGenerator(dataPointSize);

            int bucketIndex = 0;
            for (int j = 0; j < dataPointSize; j++) {
                bucketIndex += 1 + random.nextInt(bucketCount) % (Math.max(1, bucketCount / dataPointSize));
                generator.add(Math.pow(1.001, bucketIndex));
            }
            toMerge[i] = generator.get();
            cnt = getBucketCount(toMerge[i]);
            if (cnt < dataPointSize) {
                throw new IllegalArgumentException("Expected bucket count to be " + dataPointSize + ", but was " + cnt);
            }
        }

        index = 0;
    }

    private static int getBucketCount(ExponentialHistogram histo) {
        int cnt = 0;
        for (BucketIterator it : List.of(histo.negativeBuckets().iterator(), histo.positiveBuckets().iterator())) {
            while (it.hasNext()) {
                cnt++;
                it.advance();
            }
        }
        return cnt;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void add() {
        if (index >= toMerge.length) {
            index = 0;
        }
        histoMerger.add(toMerge[index++]);
    }
}
