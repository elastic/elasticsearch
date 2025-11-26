/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.exponentialhistogram;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
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

import java.io.IOException;
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

    @Param({ "array-backed", "compressed" })
    String histoImplementation;

    Random random;
    ExponentialHistogramMerger histoMerger;

    ExponentialHistogram[] toMerge = new ExponentialHistogram[10_000];

    int index;

    @Setup
    public void setUp() {
        random = ThreadLocalRandom.current();
        ExponentialHistogramCircuitBreaker breaker = ExponentialHistogramCircuitBreaker.noop();
        histoMerger = ExponentialHistogramMerger.create(bucketCount, breaker);

        ExponentialHistogramGenerator initialGenerator = ExponentialHistogramGenerator.create(bucketCount, breaker);
        for (int j = 0; j < bucketCount; j++) {
            initialGenerator.add(Math.pow(1.001, j));
        }
        ExponentialHistogram initialHisto = initialGenerator.getAndClear();
        int cnt = getBucketCount(initialHisto);
        if (cnt < bucketCount) {
            throw new IllegalArgumentException("Expected bucket count to be " + bucketCount + ", but was " + cnt);
        }
        histoMerger.add(initialHisto);

        int dataPointSize = (int) Math.round(bucketCount * mergedHistoSizeFactor);

        for (int i = 0; i < toMerge.length; i++) {
            ExponentialHistogramGenerator generator = ExponentialHistogramGenerator.create(dataPointSize, breaker);

            int bucketIndex = 0;
            for (int j = 0; j < dataPointSize; j++) {
                bucketIndex += 1 + random.nextInt(bucketCount) % (Math.max(1, bucketCount / dataPointSize));
                generator.add(Math.pow(1.001, bucketIndex));
            }
            ExponentialHistogram histogram = generator.getAndClear();
            cnt = getBucketCount(histogram);
            if (cnt < dataPointSize) {
                throw new IllegalStateException("Expected bucket count to be " + dataPointSize + ", but was " + cnt);
            }

            if ("array-backed".equals(histoImplementation)) {
                toMerge[i] = histogram;
            } else if ("compressed".equals(histoImplementation)) {
                toMerge[i] = asCompressedHistogram(histogram);
            } else {
                throw new IllegalArgumentException("Unknown implementation: " + histoImplementation);
            }
        }

        index = 0;
    }

    private ExponentialHistogram asCompressedHistogram(ExponentialHistogram histogram) {
        BytesStreamOutput histoBytes = new BytesStreamOutput();
        try {
            CompressedExponentialHistogram.writeHistogramBytes(
                histoBytes,
                histogram.scale(),
                histogram.negativeBuckets().iterator(),
                histogram.positiveBuckets().iterator()
            );
            CompressedExponentialHistogram result = new CompressedExponentialHistogram();
            BytesRef data = histoBytes.bytes().toBytesRef();
            result.reset(
                histogram.zeroBucket().zeroThreshold(),
                histogram.valueCount(),
                histogram.sum(),
                histogram.min(),
                histogram.max(),
                data
            );
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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
