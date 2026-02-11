/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.elasticsearch.benchmark.index.codec.tsdb.internal.DecodeBenchmark;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.NumericDataSupplierRegistry;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.ThroughputMetrics;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

// NOTE: Decoding benchmark using NumericDataGenerators long data sources.
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class DecodeLongGeneratorsBenchmark {

    @Param(
        {
            "timestamp",
            "counter",
            "gauge",
            "gcd",
            "constant",
            "random",
            "decreasing-timestamp",
            "boundary",
            "small",
            "timestamp-with-jitter" }
    )
    private String datasetName;

    @Param({ "42" })
    private String seed;

    @Param({ "10" })
    private int blocksPerInvocation;

    private final DecodeBenchmark decode = new DecodeBenchmark();

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        final Map<String, Supplier<long[]>> registry = NumericDataSupplierRegistry.toMap(
            NumericDataSupplierRegistry.longSuppliers(decode.getBlockSize(), Long.parseLong(seed))
        );
        final Supplier<long[]> supplier = registry.get(datasetName);
        if (supplier == null) {
            throw new IllegalArgumentException("Unknown long dataset: [" + datasetName + "]. Available: " + registry.keySet());
        }
        decode.setupTrial(supplier);
        decode.setBlocksPerInvocation(blocksPerInvocation);
    }

    @Benchmark
    public void throughput(Blackhole bh, ThroughputMetrics metrics) throws IOException {
        decode.benchmark(bh);
        metrics.recordOperation(decode.getBlockSize() * blocksPerInvocation, decode.getEncodedSize() * blocksPerInvocation);
    }
}
