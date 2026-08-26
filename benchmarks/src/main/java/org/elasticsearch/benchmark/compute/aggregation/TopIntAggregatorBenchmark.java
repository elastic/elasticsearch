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
import org.elasticsearch.compute.aggregation.TopIntAggregatorFunction;
import org.elasticsearch.compute.aggregation.TopIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.arrow.Int16ArrowBufVector;
import org.elasticsearch.compute.data.arrow.Int8ArrowBufVector;
import org.elasticsearch.compute.data.arrow.IntArrowBufVector;
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
 * Microbenchmark for {@link TopIntAggregatorFunction}'s ungrouped vector path, {@code addRawVector}. Top has a
 * moderate-cost {@code combine}: a bounded top-{@code k} insert into a {@code BucketedSort} (a comparison plus an
 * occasional sift-down), reached through a static call. That makes it a useful midpoint between aggregators with a
 * trivial {@code combine} (e.g. SUM, an {@code int} add) and an expensive one (e.g. {@code COUNT(DISTINCT)}, a
 * HyperLogLog add) when reasoning about how much the per-element dispatch in front of {@code combine} costs.
 *
 * <p>{@code addRawVector} reads each value with {@code IntVector.getInt(i)} over several concrete receivers
 * ({@code IntArrayVector}, {@code IntArrowBufVector}, {@code Int16ArrowBufVector}, {@code Int8ArrowBufVector},
 * {@code ConstantIntVector}). Each {@code vec_*} cell drives a single concrete type so that call site is monomorphic;
 * {@code vec_megamorphic} rotates through all of them within one fork so the call site goes megamorphic and the JIT
 * falls back to an interface-table lookup. Comparing the cells isolates the dispatch cost from the {@code combine} cost.
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
public class TopIntAggregatorBenchmark {

    static final int BLOCK_LENGTH = 8 * 1024;
    private static final int OP_COUNT = 1024;
    private static final int LIMIT = 1000;
    private static final boolean ASCENDING = false;

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("bench"))
        .build();

    private static final BufferAllocator arrowAllocator = new RootAllocator(Long.MAX_VALUE);

    private static final String VEC_ARRAY = "vec_array";
    private static final String VEC_ARROW32 = "vec_arrow32";
    private static final String VEC_ARROW16 = "vec_arrow16";
    private static final String VEC_ARROW8 = "vec_arrow8";
    private static final String VEC_CONSTANT = "vec_constant";
    private static final String VEC_MEGAMORPHIC = "vec_megamorphic";

    static {
        if ("true".equals(System.getProperty("skipSelfTest")) == false) {
            selfTest();
        }
    }

    static void selfTest() {
        for (String input : Utils.possibleValues(TopIntAggregatorBenchmark.class, "input")) {
            run(input, 10);
        }
    }

    @Param({ VEC_ARRAY, VEC_ARROW32, VEC_ARROW16, VEC_ARROW8, VEC_CONSTANT, VEC_MEGAMORPHIC })
    public String input;

    @Benchmark
    @OperationsPerInvocation(OP_COUNT * BLOCK_LENGTH)
    public int run() {
        return run(input, OP_COUNT);
    }

    private static int run(String input, int opCount) {
        IntBlock[] blocks = buildBlocks(input);
        BooleanVector mask = blockFactory.newConstantBooleanVector(true, BLOCK_LENGTH);
        DriverContext ctx = driverContext();
        int valueCount;
        try (TopIntAggregatorFunction agg = new TopIntAggregatorFunctionSupplier(LIMIT, ASCENDING).aggregator(ctx, List.of(0))) {
            for (int i = 0; i < opCount; i++) {
                IntBlock b = blocks[i % blocks.length];
                b.incRef();
                agg.addRawInput(new Page(b), mask);
            }
            Block[] out = new Block[1];
            agg.evaluateFinal(out, 0, ctx);
            IntBlock result = (IntBlock) out[0];
            // Every cell feeds opCount * BLOCK_LENGTH values in {1..7}, far more than LIMIT, so the top-k keeps exactly
            // LIMIT values. The descending top of {1..7} is all 7s, so the kept set is LIMIT copies of 7.
            valueCount = result.getValueCount(0);
            if (valueCount != LIMIT) {
                throw new AssertionError("[" + input + "] expected [" + LIMIT + "] kept values but got [" + valueCount + "]");
            }
            int first = result.getFirstValueIndex(0);
            for (int o = first; o < first + valueCount; o++) {
                int v = result.getInt(o);
                if (v != 7) {
                    throw new AssertionError("[" + input + "] expected kept value [7] but got [" + v + "]");
                }
            }
            out[0].close();
        }
        for (IntBlock b : blocks) {
            b.close();
        }
        mask.close();
        return valueCount;
    }

    private static IntBlock[] buildBlocks(String input) {
        return switch (input) {
            case VEC_ARRAY -> new IntBlock[] { vecArrayBlock() };
            case VEC_ARROW32 -> new IntBlock[] { vecArrow32Block() };
            case VEC_ARROW16 -> new IntBlock[] { vecArrow16Block() };
            case VEC_ARROW8 -> new IntBlock[] { vecArrow8Block() };
            case VEC_CONSTANT -> new IntBlock[] { vecConstantBlock() };
            case VEC_MEGAMORPHIC -> new IntBlock[] {
                vecArrayBlock(),
                vecArrow32Block(),
                vecArrow16Block(),
                vecArrow8Block(),
                vecConstantBlock() };
            default -> throw new IllegalArgumentException("unknown input [" + input + "]");
        };
    }

    private static IntBlock vecArrayBlock() {
        int[] values = new int[BLOCK_LENGTH];
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            values[i] = (i % 7) + 1;
        }
        return blockFactory.newIntArrayVector(values, BLOCK_LENGTH).asBlock();
    }

    private static IntBlock vecArrow32Block() {
        ArrowBuf buf = arrowAllocator.buffer((long) BLOCK_LENGTH * Integer.BYTES);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            buf.setInt((long) i * Integer.BYTES, (i % 7) + 1);
        }
        return new IntArrowBufVector(buf, BLOCK_LENGTH, blockFactory).asBlock();
    }

    private static IntBlock vecArrow16Block() {
        ArrowBuf buf = arrowAllocator.buffer((long) BLOCK_LENGTH * Short.BYTES);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            buf.setShort((long) i * Short.BYTES, (short) ((i % 7) + 1));
        }
        return new Int16ArrowBufVector(buf, BLOCK_LENGTH, blockFactory).asBlock();
    }

    private static IntBlock vecArrow8Block() {
        ArrowBuf buf = arrowAllocator.buffer(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            buf.setByte(i, (byte) ((i % 7) + 1));
        }
        return new Int8ArrowBufVector(buf, BLOCK_LENGTH, blockFactory).asBlock();
    }

    private static IntBlock vecConstantBlock() {
        return blockFactory.newConstantIntBlockWith(7, BLOCK_LENGTH);
    }

    static DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }
}
