/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.aggregation;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.CountDistinctFloatAggregatorFunction;
import org.elasticsearch.compute.aggregation.CountDistinctFloatAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.arrow.FloatArrowBufVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmark for {@link CountDistinctFloatAggregatorFunction}'s ungrouped vector path, {@code addRawVector}, an
 * aggregator with an <b>expensive</b> {@code combine}: a HyperLogLog add (a hash plus a register update, tens of ns),
 * reached through a static call. Because that {@code combine} dominates the loop, the per-element receiver dispatch in
 * front of it is only a small fraction of the cost. This is the high-cost end of the range that
 * {@link SumIntAggregatorBenchmark} (trivial {@code combine}, an {@code int} add) and {@link TopIntAggregatorBenchmark}
 * (moderate {@code combine}, a bounded top-k insert) bracket.
 *
 * <p>{@code addRawVector} reads each value with {@code FloatVector.getFloat(i)} over several concrete receivers
 * ({@code FloatArrayVector}, {@code FloatArrowBufVector}, {@code ConstantFloatVector}). Each {@code vec_*} cell drives a
 * single concrete type so that call site is monomorphic; {@code vec_megamorphic} rotates through all of them within one
 * fork so the call site goes megamorphic and the JIT falls back to an interface-table lookup. Comparing the cells
 * isolates the dispatch cost from the {@code combine} cost.
 *
 * <p>Vector cells only: this exercises {@code addRawVector}, not the multi-valued/nullable {@code addRawBlock} path.
 *
 * <p>Note: {@link #selfTest()} runs every cell at small scale before measurement, which deliberately poisons the
 * dispatch to behave like production rather than an artificially monomorphic microbenchmark (per
 * {@code benchmarks/AGENTS.md}).
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgs = { "--add-opens=java.base/java.nio=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED" })
public class CountDistinctFloatAggregatorBenchmark {

    static final int BLOCK_LENGTH = 8 * 1024;
    private static final int OP_COUNT = 1024;
    private static final int PRECISION = 40000;

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("bench"))
        .build();

    private static final BufferAllocator arrowAllocator = new RootAllocator(Long.MAX_VALUE);

    private static final String VEC_ARRAY = "vec_array";
    private static final String VEC_ARROW = "vec_arrow";
    private static final String VEC_CONSTANT = "vec_constant";
    private static final String VEC_MEGAMORPHIC = "vec_megamorphic";

    static {
        if ("true".equals(System.getProperty("skipSelfTest")) == false) {
            selfTest();
        }
    }

    static void selfTest() {
        for (String input : Utils.possibleValues(CountDistinctFloatAggregatorBenchmark.class, "input")) {
            run(input, 10);
        }
    }

    @Param({ VEC_ARRAY, VEC_ARROW, VEC_CONSTANT, VEC_MEGAMORPHIC })
    public String input;

    @Benchmark
    @OperationsPerInvocation(OP_COUNT * BLOCK_LENGTH)
    public long run() {
        return run(input, OP_COUNT);
    }

    private static long run(String input, int opCount) {
        FloatBlock[] blocks = buildBlocks(input);
        // Distinct values are {1..7} for the non-constant cells and {7} for the constant cell; their union is still
        // {1..7}. HyperLogLog is exact at these cardinalities (linear counting), so we can assert an exact result.
        long expected = VEC_CONSTANT.equals(input) ? 1 : 7;
        BooleanVector mask = blockFactory.newConstantBooleanVector(true, BLOCK_LENGTH);
        DriverContext ctx = driverContext();
        long distinct;
        try (
            CountDistinctFloatAggregatorFunction agg = new CountDistinctFloatAggregatorFunctionSupplier(PRECISION).aggregator(
                ctx,
                List.of(0)
            )
        ) {
            for (int i = 0; i < opCount; i++) {
                FloatBlock b = blocks[i % blocks.length];
                b.incRef();
                agg.addRawInput(new Page(b), mask);
            }
            Block[] out = new Block[1];
            agg.evaluateFinal(out, 0, ctx);
            distinct = ((LongBlock) out[0]).getLong(0);
            if (distinct != expected) {
                throw new AssertionError("[" + input + "] expected [" + expected + "] distinct but got [" + distinct + "]");
            }
            out[0].close();
        }
        for (FloatBlock b : blocks) {
            b.close();
        }
        mask.close();
        return distinct;
    }

    private static FloatBlock[] buildBlocks(String input) {
        return switch (input) {
            case VEC_ARRAY -> new FloatBlock[] { vecArrayBlock() };
            case VEC_ARROW -> new FloatBlock[] { vecArrowBlock() };
            case VEC_CONSTANT -> new FloatBlock[] { vecConstantBlock() };
            case VEC_MEGAMORPHIC -> new FloatBlock[] { vecArrayBlock(), vecArrowBlock(), vecConstantBlock() };
            default -> throw new IllegalArgumentException("unknown input [" + input + "]");
        };
    }

    private static FloatBlock vecArrayBlock() {
        float[] values = new float[BLOCK_LENGTH];
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            values[i] = (i % 7) + 1;
        }
        return blockFactory.newFloatArrayVector(values, BLOCK_LENGTH).asBlock();
    }

    private static FloatBlock vecArrowBlock() {
        ArrowBuf buf = arrowAllocator.buffer((long) BLOCK_LENGTH * Float.BYTES);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            buf.setFloat((long) i * Float.BYTES, (i % 7) + 1);
        }
        return new FloatArrowBufVector(buf, BLOCK_LENGTH, blockFactory).asBlock();
    }

    private static FloatBlock vecConstantBlock() {
        return blockFactory.newConstantFloatBlockWith(7, BLOCK_LENGTH);
    }

    static DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }
}
