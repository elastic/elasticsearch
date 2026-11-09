/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.PredictLinearDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.TimeSeriesAggregationOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Measures full aggregation, including grouping, state allocation and finalization. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector", "-Xms1g", "-Xmx1g" })
public class PredictLinearBenchmark {
    static {
        BenchmarkLogging.configure();
    }

    @Param({ "1", "100", "1000" })
    public int groups;

    @Param({ "65536", "1048576" })
    public int rows;

    private static final long EPOCH = 1_730_000_040_000L;
    private final List<Page> pages = new ArrayList<>();
    private DriverContext context;
    private TimeSeriesAggregationOperator.Factory factory;

    @Setup
    public void setup() {
        var blocks = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("benchmark")).build();
        context = new DriverContext(blocks.bigArrays(), blocks, null);
        factory = new TimeSeriesAggregationOperator.Factory(
            Rounding.builder(TimeValue.timeValueMinutes(1)).build().prepareForUnknown(),
            false,
            List.of(
                new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, null),
                new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
            ),
            AggregatorMode.SINGLE,
            List.of(
                new PredictLinearDoubleAggregatorFunctionSupplier(3600).groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(2, 3))
            ),
            Integer.MAX_VALUE
        );
        for (int start = 0; start < rows; start += 4096) {
            int size = Math.min(4096, rows - start);
            try (
                var ids = blocks.newBytesRefVectorBuilder(size);
                var values = blocks.newDoubleVectorFixedBuilder(size);
                var timestamps = blocks.newLongVectorFixedBuilder(size)
            ) {
                for (int p = 0; p < size; p++) {
                    int row = start + p;
                    int seconds = (row / groups) % 60;
                    ids.appendBytesRef(new BytesRef(Integer.toString(row % groups)));
                    values.appendDouble(10 + seconds / 4.0);
                    timestamps.appendLong(EPOCH + seconds * 1000L);
                }
                pages.add(
                    new Page(
                        ids.build().asBlock(),
                        blocks.newConstantLongBlockWith(EPOCH, size),
                        values.build().asBlock(),
                        timestamps.build().asBlock()
                    )
                );
            }
        }
        // Self-test every parameter combination before timing. The generating line is y = 10 + t / 4.
        double expected = groups * 925.0;
        double actual = aggregate();
        if (Math.abs(expected - actual) > expected * 1e-10) {
            throw new AssertionError("expected " + expected + " but was " + actual);
        }
    }

    @Benchmark
    public double aggregate() {
        double sum = 0;
        try (var operator = factory.get(context)) {
            for (Page page : pages) {
                operator.addInput(page.shallowCopy());
            }
            operator.finish();
            while (operator.isFinished() == false) {
                try (Page result = operator.getOutput()) {
                    if (result != null) {
                        DoubleBlock values = result.getBlock(2);
                        for (int p = 0; p < result.getPositionCount(); p++) {
                            sum += values.getDouble(p);
                        }
                    }
                }
            }
        }
        return sum;
    }

    @TearDown
    public void tearDown() {
        Releasables.close(pages);
        pages.clear();
        context.finish();
    }
}
