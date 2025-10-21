/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.benchmark.tdigest;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.search.aggregations.metrics.MemoryTrackingTDigestArrays;
import org.elasticsearch.tdigest.Sort;
import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestDoubleArray;
import org.elasticsearch.tdigest.arrays.TDigestIntArray;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

/** Explores the performance of Sort on pathological input data. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Thread)
public class SortBench {
    private final int size = 100000;
    private final TDigestArrays arrays = new MemoryTrackingTDigestArrays(new NoopCircuitBreaker("default-wrapper-tdigest-arrays"));
    private final TDigestDoubleArray values = arrays.newDoubleArray(size);

    @Param({ "0", "1", "-1" })
    public int sortDirection;

    @Setup
    public void setup() {
        Random prng = new Random(999983);
        for (int i = 0; i < size; i++) {
            values.set(i, prng.nextDouble());
        }
        if (sortDirection > 0) {
            values.sort();
        } else if (sortDirection < 0) {
            values.sort();
            Sort.reverse(values, 0, values.size());
        }
    }

    @Benchmark
    public void stableSort() {
        TDigestIntArray order = arrays.newIntArray(size);
        for (int i = 0; i < size; i++) {
            order.set(i, i);
        }
        Sort.stableSort(order, values, values.size());
    }
}
