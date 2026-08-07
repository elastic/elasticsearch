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
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.compute.aggregation.MaxDoubleAggregatorFunction;
import org.elasticsearch.compute.aggregation.MaxDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBigArrayBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.arrow.DoubleArrowBufBlock;
import org.elasticsearch.compute.data.arrow.DoubleArrowBufVector;
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

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmark for {@link MaxDoubleAggregatorFunction}'s ungrouped input paths. Unlike
 * {@link SumIntAggregatorBenchmark} (whose {@code SUM(int)} widens into a {@code long} accumulator, a shape C2's
 * superword pass does not auto-vectorize), {@code MAX(double)} has a non-widening, order-independent
 * {@code combine} ({@code Math.max(double, double)}) that the autovectorizer can turn into SIMD ({@code FMAXNM} on
 * aarch64/NEON, {@code VMAXPD} on x86). That makes this benchmark the one that can actually exercise (and prove)
 * vectorization of the bulk Arrow segment reduction; toggling {@code -XX:-UseSuperWord} on the {@code vec_arrow_double}
 * cell isolates the SIMD contribution from the monomorphization win.
 *
 * <p>{@link MaxDoubleAggregatorFunction#addRawInput(Page, BooleanVector)} routes through two polymorphic call sites:
 * <ul>
 *   <li><b>addRawVector(DoubleVector)</b>: over {@code DoubleArrayVector}, {@code DoubleArrowBufVector} (the bulk
 *       {@code MemorySegment} reduction), {@code ConstantDoubleVector}.</li>
 *   <li><b>addRawBlock(DoubleBlock)</b>: over {@code DoubleArrayBlock}, {@code DoubleBigArrayBlock},
 *       {@code DoubleArrowBufBlock}.</li>
 * </ul>
 * Plus the {@code ConstantNullBlock} short-circuit that {@code addRawInputNotMasked} takes via {@code areAllValuesNull()}.
 *
 * <p>Each {@code vec_*} / {@code block_*} cell exercises one concrete receiver type so its call site is monomorphic.
 * The {@code *_megamorphic} cells rotate through several types within a single fork so the call site goes megamorphic
 * and the JIT falls back to an interface-table lookup. Comparing the cells isolates the dispatch cost from the
 * {@code combine} cost.
 *
 * <p>Note: {@link #selfTest()} runs every cell at small {@code BLOCK_LENGTH} before measurement, which deliberately
 * poisons virtual dispatch so the benchmark behaves like production rather than an artificially monomorphic
 * microbenchmark (per {@code benchmarks/AGENTS.md}).
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(
    value = 1,
    jvmArgs = {
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        // MaxDoubleAggregatorFunction's vec_arrow_double arm reduces an off-heap ArrowBuf via
        // MemorySegment.reinterpret (a restricted FFM method). Without this flag the JVM
        // prints a runtime warning into JMH's result stream on JDK 22-23 and hard-fails on
        // JDK 24+ (production sets --illegal-native-access=deny there). Production grants
        // this via the load_native_libraries entitlement on the esql plugin.
        "--enable-native-access=ALL-UNNAMED" }
)
public class MaxDoubleAggregatorBenchmark {

    static final int BLOCK_LENGTH = 8 * 1024;
    private static final int OP_COUNT = 1024;

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("bench"))
        .build();

    private static final BufferAllocator arrowAllocator = new RootAllocator(Long.MAX_VALUE);

    private static final String VEC_ARRAY = "vec_array";
    private static final String VEC_ARROW_DOUBLE = "vec_arrow_double";
    private static final String VEC_CONSTANT = "vec_constant";
    private static final String VEC_MEGAMORPHIC = "vec_megamorphic";
    private static final String BLOCK_ARRAY = "block_array";
    private static final String BLOCK_ARROW_DOUBLE = "block_arrow_double";
    private static final String BLOCK_MEGAMORPHIC = "block_megamorphic";
    private static final String ALL_NULL = "all_null";

    static {
        if ("true".equals(System.getProperty("skipSelfTest")) == false) {
            selfTest();
        }
    }

    static void selfTest() {
        for (String input : Utils.possibleValues(MaxDoubleAggregatorBenchmark.class, "input")) {
            run(input, 10);
        }
    }

    @Param({ VEC_ARRAY, VEC_ARROW_DOUBLE, VEC_CONSTANT, VEC_MEGAMORPHIC, BLOCK_ARRAY, BLOCK_ARROW_DOUBLE, BLOCK_MEGAMORPHIC, ALL_NULL })
    public String input;

    @Benchmark
    @OperationsPerInvocation(OP_COUNT * BLOCK_LENGTH)
    public double run() {
        return run(input, OP_COUNT);
    }

    private static double run(String input, int opCount) {
        DoubleBlock[] blocks = buildBlocks(input);
        double expectedMax = computeMax(blocks);
        BooleanVector mask = blockFactory.newConstantBooleanVector(true, BLOCK_LENGTH);
        DriverContext ctx = driverContext();
        double max;
        boolean allNull = ALL_NULL.equals(input);
        try (MaxDoubleAggregatorFunction agg = new MaxDoubleAggregatorFunctionSupplier().aggregator(ctx, List.of(0))) {
            for (int i = 0; i < opCount; i++) {
                DoubleBlock b = blocks[i % blocks.length];
                b.incRef();
                agg.addRawInput(new Page(b), mask);
            }
            Block[] out = new Block[1];
            agg.evaluateFinal(out, 0, ctx);
            if (allNull) {
                if (out[0].areAllValuesNull() == false) {
                    throw new AssertionError("[" + input + "] expected all-null result, got " + out[0]);
                }
                max = Double.NEGATIVE_INFINITY;
            } else {
                max = ((DoubleBlock) out[0]).getDouble(0);
                if (max != expectedMax) {
                    throw new AssertionError("[" + input + "] expected [" + expectedMax + "] but got [" + max + "]");
                }
            }
            out[0].close();
        }
        for (DoubleBlock b : blocks) {
            b.close();
        }
        mask.close();
        return max;
    }

    private static DoubleBlock[] buildBlocks(String input) {
        return switch (input) {
            case VEC_ARRAY -> new DoubleBlock[] { vecArrayBlock() };
            case VEC_ARROW_DOUBLE -> new DoubleBlock[] { vecArrowDoubleBlock() };
            case VEC_CONSTANT -> new DoubleBlock[] { vecConstantBlock() };
            case VEC_MEGAMORPHIC -> new DoubleBlock[] { vecArrayBlock(), vecArrowDoubleBlock(), vecConstantBlock() };
            case BLOCK_ARRAY -> new DoubleBlock[] { blockArrayWithNulls() };
            case BLOCK_ARROW_DOUBLE -> new DoubleBlock[] { blockArrowDoubleWithNulls() };
            case BLOCK_MEGAMORPHIC -> new DoubleBlock[] { blockArrayWithNulls(), blockBigArrayWithNulls(), blockArrowDoubleWithNulls() };
            case ALL_NULL -> new DoubleBlock[] { (DoubleBlock) blockFactory.newConstantNullBlock(BLOCK_LENGTH) };
            default -> throw new IllegalArgumentException("unknown input [" + input + "]");
        };
    }

    private static double computeMax(DoubleBlock[] blocks) {
        double max = Double.NEGATIVE_INFINITY;
        for (DoubleBlock blk : blocks) {
            DoubleVector v = blk.asVector();
            if (v != null) {
                for (int p = 0; p < v.getPositionCount(); p++) {
                    max = Math.max(max, v.getDouble(p));
                }
            } else {
                for (int p = 0; p < blk.getPositionCount(); p++) {
                    int count = blk.getValueCount(p);
                    int start = blk.getFirstValueIndex(p);
                    for (int o = start; o < start + count; o++) {
                        max = Math.max(max, blk.getDouble(o));
                    }
                }
            }
        }
        return max;
    }

    // --- vector cells (asVector() != null, addRawVector path) ----------------------------

    private static DoubleBlock vecArrayBlock() {
        double[] values = new double[BLOCK_LENGTH];
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            values[i] = (i % 1024) * 1.5;
        }
        return blockFactory.newDoubleArrayVector(values, BLOCK_LENGTH).asBlock();
    }

    private static DoubleBlock vecArrowDoubleBlock() {
        ArrowBuf buf = arrowAllocator.buffer((long) BLOCK_LENGTH * Double.BYTES);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            buf.setDouble((long) i * Double.BYTES, (i % 1024) * 1.5);
        }
        return new DoubleArrowBufVector(buf, BLOCK_LENGTH, blockFactory).asBlock();
    }

    private static DoubleBlock vecConstantBlock() {
        return blockFactory.newConstantDoubleBlockWith(7.0, BLOCK_LENGTH);
    }

    // --- block cells (asVector() == null, addRawBlock path) ------------------------------

    /**
     * Half-null {@link org.elasticsearch.compute.data.DoubleArrayBlock}: even positions hold {@code (p % 1024) * 1.5},
     * odd positions are null. {@link DoubleBlock#asVector()} returns {@code null} because the null bitset is non-empty,
     * so {@code addRawBlock} is taken.
     */
    private static DoubleBlock blockArrayWithNulls() {
        double[] values = new double[BLOCK_LENGTH];
        int[] firstValueIndexes = new int[BLOCK_LENGTH + 1];
        BitSet nulls = new BitSet(BLOCK_LENGTH);
        for (int p = 0; p < BLOCK_LENGTH; p++) {
            firstValueIndexes[p] = p;
            if ((p & 1) == 0) {
                values[p] = (p % 1024) * 1.5;
            } else {
                nulls.set(p);
            }
        }
        firstValueIndexes[BLOCK_LENGTH] = BLOCK_LENGTH;
        return blockFactory.newDoubleArrayBlock(values, BLOCK_LENGTH, firstValueIndexes, nulls, Block.MvOrdering.UNORDERED);
    }

    private static DoubleBlock blockBigArrayWithNulls() {
        DoubleArray values = blockFactory.bigArrays().newDoubleArray(BLOCK_LENGTH, false);
        int[] firstValueIndexes = new int[BLOCK_LENGTH + 1];
        BitSet nulls = new BitSet(BLOCK_LENGTH);
        for (int p = 0; p < BLOCK_LENGTH; p++) {
            firstValueIndexes[p] = p;
            if ((p & 1) == 0) {
                values.set(p, (p % 1024) * 1.5);
            } else {
                values.set(p, 0);
                nulls.set(p);
            }
        }
        firstValueIndexes[BLOCK_LENGTH] = BLOCK_LENGTH;
        return new DoubleBigArrayBlock(values, BLOCK_LENGTH, firstValueIndexes, nulls, Block.MvOrdering.UNORDERED, blockFactory);
    }

    private static DoubleBlock blockArrowDoubleWithNulls() {
        ArrowBuf values = arrowAllocator.buffer((long) BLOCK_LENGTH * Double.BYTES);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            values.setDouble((long) i * Double.BYTES, (i & 1) == 0 ? (i % 1024) * 1.5 : 0.0);
        }
        ArrowBuf validity = newHalfNullValidityBuffer(BLOCK_LENGTH);
        return new DoubleArrowBufBlock(values, validity, null, BLOCK_LENGTH, 0, blockFactory);
    }

    /**
     * Build an Arrow validity bitmap (LSB-first, 1 bit per position, 1 == present) where even
     * positions are valid and odd positions are null. Length is rounded up to a multiple of 8 bytes,
     * matching {@code AbstractArrowBufBlock.validityBufferLength}.
     */
    private static ArrowBuf newHalfNullValidityBuffer(int positions) {
        int bytes = ((positions + 63) / 64) * Long.BYTES;
        ArrowBuf validity = arrowAllocator.buffer(bytes);
        validity.setZero(0, bytes);
        for (int p = 0; p < positions; p += 2) {
            int byteIdx = p / 8;
            byte cur = validity.getByte(byteIdx);
            validity.setByte(byteIdx, (byte) (cur | (1 << (p % 8))));
        }
        return validity;
    }

    static DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }
}
