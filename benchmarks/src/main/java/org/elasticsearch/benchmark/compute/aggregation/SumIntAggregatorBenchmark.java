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
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunction;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.arrow.Int16ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.Int16ArrowBufVector;
import org.elasticsearch.compute.data.arrow.Int8ArrowBufBlock;
import org.elasticsearch.compute.data.arrow.Int8ArrowBufVector;
import org.elasticsearch.compute.data.arrow.IntArrowBufBlock;
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

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmark for {@link SumIntAggregatorFunction}'s ungrouped input paths. SUM has a trivial {@code combine} (an
 * integer add), so the per-element receiver dispatch is the dominant cost. That makes this the low-cost end of the
 * range that {@link TopIntAggregatorBenchmark} (moderate {@code combine}) and {@link CountDistinctFloatAggregatorBenchmark}
 * (expensive {@code combine}) bracket.
 *
 * <p>{@link SumIntAggregatorFunction#addRawInput(Page, BooleanVector)} routes through two polymorphic call sites:
 * <ul>
 *   <li><b>addRawVector(IntVector)</b>: {@code IntVector.getInt(i)} over {@code IntArrayVector},
 *       {@code IntArrowBufVector} (32-bit), {@code Int16ArrowBufVector}, {@code Int8ArrowBufVector},
 *       {@code ConstantIntVector}.</li>
 *   <li><b>addRawBlock(IntBlock)</b>: {@code IntBlock.getInt(offset)} / {@code getValueCount} /
 *       {@code getFirstValueIndex} over {@code IntArrayBlock}, {@code IntBigArrayBlock},
 *       {@code IntArrowBufBlock}, {@code Int16ArrowBufBlock}, {@code Int8ArrowBufBlock}.</li>
 * </ul>
 * Plus the {@code ConstantNullBlock} short-circuit that {@code addRawInputNotMasked} takes via
 * {@code areAllValuesNull()}.
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
        // SumIntAggregatorFunction's vec_arrow32 arm reduces an off-heap ArrowBuf via
        // MemorySegment.reinterpret (a restricted FFM method). Without this flag the JVM
        // prints a runtime warning into JMH's result stream on JDK 22-23 and hard-fails on
        // JDK 24+ (production sets --illegal-native-access=deny there). Production grants
        // this via the load_native_libraries entitlement on the esql plugin.
        "--enable-native-access=ALL-UNNAMED" }
)
public class SumIntAggregatorBenchmark {

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
    private static final String VEC_ARROW32 = "vec_arrow32";
    private static final String VEC_ARROW16 = "vec_arrow16";
    private static final String VEC_ARROW8 = "vec_arrow8";
    private static final String VEC_CONSTANT = "vec_constant";
    private static final String VEC_MEGAMORPHIC = "vec_megamorphic";
    private static final String BLOCK_ARRAY = "block_array";
    private static final String BLOCK_ARROW32 = "block_arrow32";
    private static final String BLOCK_MEGAMORPHIC = "block_megamorphic";
    private static final String ALL_NULL = "all_null";

    static {
        if ("true".equals(System.getProperty("skipSelfTest")) == false) {
            selfTest();
        }
    }

    static void selfTest() {
        for (String input : Utils.possibleValues(SumIntAggregatorBenchmark.class, "input")) {
            run(input, 10);
        }
    }

    @Param(
        {
            VEC_ARRAY,
            VEC_ARROW32,
            VEC_ARROW16,
            VEC_ARROW8,
            VEC_CONSTANT,
            VEC_MEGAMORPHIC,
            BLOCK_ARRAY,
            BLOCK_ARROW32,
            BLOCK_MEGAMORPHIC,
            ALL_NULL }
    )
    public String input;

    @Benchmark
    @OperationsPerInvocation(OP_COUNT * BLOCK_LENGTH)
    public long run() {
        return run(input, OP_COUNT);
    }

    private static long run(String input, int opCount) {
        IntBlock[] blocks = buildBlocks(input);
        long[] blockSums = computeBlockSums(blocks);
        BooleanVector mask = blockFactory.newConstantBooleanVector(true, BLOCK_LENGTH);
        DriverContext ctx = driverContext();
        long sum;
        boolean allNull = ALL_NULL.equals(input);
        try (SumIntAggregatorFunction agg = new SumIntAggregatorFunctionSupplier().aggregator(ctx, List.of(0))) {
            for (int i = 0; i < opCount; i++) {
                IntBlock b = blocks[i % blocks.length];
                b.incRef();
                agg.addRawInput(new Page(b), mask);
            }
            Block[] out = new Block[1];
            agg.evaluateFinal(out, 0, ctx);
            if (allNull) {
                if (out[0].areAllValuesNull() == false) {
                    throw new AssertionError("[" + input + "] expected all-null result, got " + out[0]);
                }
                sum = 0;
            } else {
                sum = ((LongBlock) out[0]).getLong(0);
                long expected = 0;
                for (int i = 0; i < opCount; i++) {
                    expected += blockSums[i % blockSums.length];
                }
                if (sum != expected) {
                    throw new AssertionError("[" + input + "] expected [" + expected + "] but got [" + sum + "]");
                }
            }
            out[0].close();
        }
        for (IntBlock b : blocks) {
            b.close();
        }
        mask.close();
        return sum;
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
            case BLOCK_ARRAY -> new IntBlock[] { blockArrayWithNulls() };
            case BLOCK_ARROW32 -> new IntBlock[] { blockArrow32WithNulls() };
            case BLOCK_MEGAMORPHIC -> new IntBlock[] {
                blockArrayWithNulls(),
                blockBigArrayWithNulls(),
                blockArrow32WithNulls(),
                blockArrow16WithNulls(),
                blockArrow8WithNulls() };
            case ALL_NULL -> new IntBlock[] { (IntBlock) blockFactory.newConstantNullBlock(BLOCK_LENGTH) };
            default -> throw new IllegalArgumentException("unknown input [" + input + "]");
        };
    }

    private static long[] computeBlockSums(IntBlock[] blocks) {
        long[] sums = new long[blocks.length];
        for (int b = 0; b < blocks.length; b++) {
            IntBlock blk = blocks[b];
            long s = 0;
            IntVector v = blk.asVector();
            if (v != null) {
                for (int p = 0; p < v.getPositionCount(); p++) {
                    s += v.getInt(p);
                }
            } else {
                for (int p = 0; p < blk.getPositionCount(); p++) {
                    int count = blk.getValueCount(p);
                    int start = blk.getFirstValueIndex(p);
                    for (int o = start; o < start + count; o++) {
                        s += blk.getInt(o);
                    }
                }
            }
            sums[b] = s;
        }
        return sums;
    }

    // --- vector cells (asVector() != null, addRawVector path) ----------------------------

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

    // --- block cells (asVector() == null, addRawBlock path) ------------------------------

    /**
     * Half-null {@link org.elasticsearch.compute.data.IntArrayBlock}: even positions hold {@code (p % 7) + 1},
     * odd positions are null. {@link IntBlock#asVector()} returns {@code null} because the null bitset is non-empty,
     * so {@code addRawBlock} is taken.
     */
    private static IntBlock blockArrayWithNulls() {
        int[] values = new int[BLOCK_LENGTH];
        int[] firstValueIndexes = new int[BLOCK_LENGTH + 1];
        BitSet nulls = new BitSet(BLOCK_LENGTH);
        for (int p = 0; p < BLOCK_LENGTH; p++) {
            firstValueIndexes[p] = p;
            if ((p & 1) == 0) {
                values[p] = (p % 7) + 1;
            } else {
                nulls.set(p);
            }
        }
        firstValueIndexes[BLOCK_LENGTH] = BLOCK_LENGTH;
        return blockFactory.newIntArrayBlock(values, BLOCK_LENGTH, firstValueIndexes, nulls, Block.MvOrdering.UNORDERED);
    }

    private static IntBlock blockBigArrayWithNulls() {
        IntArray values = blockFactory.bigArrays().newIntArray(BLOCK_LENGTH, false);
        int[] firstValueIndexes = new int[BLOCK_LENGTH + 1];
        BitSet nulls = new BitSet(BLOCK_LENGTH);
        for (int p = 0; p < BLOCK_LENGTH; p++) {
            firstValueIndexes[p] = p;
            if ((p & 1) == 0) {
                values.set(p, (p % 7) + 1);
            } else {
                values.set(p, 0);
                nulls.set(p);
            }
        }
        firstValueIndexes[BLOCK_LENGTH] = BLOCK_LENGTH;
        return new IntBigArrayBlock(values, BLOCK_LENGTH, firstValueIndexes, nulls, Block.MvOrdering.UNORDERED, blockFactory);
    }

    private static IntBlock blockArrow32WithNulls() {
        ArrowBuf values = arrowAllocator.buffer((long) BLOCK_LENGTH * Integer.BYTES);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            values.setInt((long) i * Integer.BYTES, (i & 1) == 0 ? ((i % 7) + 1) : 0);
        }
        ArrowBuf validity = newHalfNullValidityBuffer(BLOCK_LENGTH);
        return new IntArrowBufBlock(values, validity, null, BLOCK_LENGTH, 0, blockFactory);
    }

    private static IntBlock blockArrow16WithNulls() {
        ArrowBuf values = arrowAllocator.buffer((long) BLOCK_LENGTH * Short.BYTES);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            values.setShort((long) i * Short.BYTES, (i & 1) == 0 ? (short) ((i % 7) + 1) : (short) 0);
        }
        ArrowBuf validity = newHalfNullValidityBuffer(BLOCK_LENGTH);
        return new Int16ArrowBufBlock(values, validity, null, BLOCK_LENGTH, 0, blockFactory);
    }

    private static IntBlock blockArrow8WithNulls() {
        ArrowBuf values = arrowAllocator.buffer(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            values.setByte(i, (i & 1) == 0 ? (byte) ((i % 7) + 1) : (byte) 0);
        }
        ArrowBuf validity = newHalfNullValidityBuffer(BLOCK_LENGTH);
        return new Int8ArrowBufBlock(values, validity, null, BLOCK_LENGTH, 0, blockFactory);
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
