/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class BlockKeepMaskBenchmark extends BlockBenchmark {
    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        int totalPositions = 10;
        for (String paramString : RELEVANT_TYPE_BLOCK_COMBINATIONS) {
            String[] params = paramString.split("/");
            String dataType = params[0];
            String blockKind = params[1];
            BooleanVector mask = buildMask(totalPositions);

            BenchmarkBlocks data = buildBenchmarkBlocks(dataType, blockKind, mask, totalPositions);
            Block[] results = new Block[NUM_BLOCKS_PER_ITERATION];
            run(data, mask, results);
            assertCheckSums(dataType, blockKind, data, results, totalPositions);
        }
    }

    record BenchmarkBlocks(Block[] blocks, long[] checkSums) {};

    static BenchmarkBlocks buildBenchmarkBlocks(String dataType, String blockKind, BooleanVector mask, int totalPositions) {
        Block[] blocks = BlockBenchmark.buildBlocks(dataType, blockKind, totalPositions);
        return new BenchmarkBlocks(blocks, checksumsFor(dataType, blocks, mask));
    }

    static long[] checksumsFor(String dataType, Block[] blocks, BooleanVector mask) {
        long[] checkSums = new long[NUM_BLOCKS_PER_ITERATION];
        switch (dataType) {
            case "boolean" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    BooleanBlock block = (BooleanBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeBooleanCheckSum(block, mask);
                }
            }
            case "BytesRef" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    BytesRefBlock block = (BytesRefBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeBytesRefCheckSum(block, mask);
                }
            }
            case "double" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    DoubleBlock block = (DoubleBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeDoubleCheckSum(block, mask);
                }
            }
            case "int" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    IntBlock block = (IntBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeIntCheckSum(block, mask);
                }
            }
            case "long" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    LongBlock block = (LongBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeLongCheckSum(block, mask);
                }
            }
            // TODO float
            default -> throw new IllegalStateException("illegal data type [" + dataType + "]");
        }
        return checkSums;
    }

    static BooleanVector buildMask(int totalPositions) {
        try (BooleanVector.FixedBuilder builder = blockFactory.newBooleanVectorFixedBuilder(totalPositions)) {
            for (int p = 0; p < totalPositions; p++) {
                builder.appendBoolean(p % 2 == 0);
            }
            return builder.build();
        }
    }

    private static void run(BenchmarkBlocks data, BooleanVector mask, Block[] results) {
        for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
            results[blockIndex] = data.blocks[blockIndex].keepMask(mask);
        }
    }

    private static void assertCheckSums(String dataType, String blockKind, BenchmarkBlocks data, Block[] results, int positionCount) {
        long[] checkSums = checksumsFor(dataType, results, blockFactory.newConstantBooleanVector(true, positionCount));
        for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
            if (checkSums[blockIndex] != data.checkSums[blockIndex]) {
                throw new AssertionError(
                    "checksums do not match for block ["
                        + blockIndex
                        + "]["
                        + dataType
                        + "]["
                        + blockKind
                        + "]: "
                        + checkSums[blockIndex]
                        + " vs "
                        + data.checkSums[blockIndex]
                );
            }
        }
    }

    private static long computeBooleanCheckSum(BooleanBlock block, BooleanVector mask) {
        long sum = 0;

        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p) || mask.getBoolean(p) == false) {
                continue;
            }
            int start = block.getFirstValueIndex(p);
            int end = start + block.getValueCount(p);
            for (int i = start; i < end; i++) {
                sum += block.getBoolean(i) ? 1 : 0;
            }
        }

        return sum;
    }

    private static long computeBytesRefCheckSum(BytesRefBlock block, BooleanVector mask) {
        long sum = 0;
        BytesRef scratch = new BytesRef();

        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p) || mask.getBoolean(p) == false) {
                continue;
            }
            int start = block.getFirstValueIndex(p);
            int end = start + block.getValueCount(p);
            for (int i = start; i < end; i++) {
                BytesRef v = block.getBytesRef(i, scratch);
                sum += v.length > 0 ? v.bytes[v.offset] : 0;
            }
        }

        return sum;
    }

    private static long computeDoubleCheckSum(DoubleBlock block, BooleanVector mask) {
        long sum = 0;

        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p) || mask.getBoolean(p) == false) {
                continue;
            }
            int start = block.getFirstValueIndex(p);
            int end = start + block.getValueCount(p);
            for (int i = start; i < end; i++) {
                sum += (long) block.getDouble(i);
            }
        }

        return sum;
    }

    private static long computeIntCheckSum(IntBlock block, BooleanVector mask) {
        int sum = 0;

        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p) || mask.getBoolean(p) == false) {
                continue;
            }
            int start = block.getFirstValueIndex(p);
            int end = start + block.getValueCount(p);
            for (int i = start; i < end; i++) {
                sum += block.getInt(i);
            }
        }

        return sum;
    }

    private static long computeLongCheckSum(LongBlock block, BooleanVector mask) {
        long sum = 0;

        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p) || mask.getBoolean(p) == false) {
                continue;
            }
            int start = block.getFirstValueIndex(p);
            int end = start + block.getValueCount(p);
            for (int i = start; i < end; i++) {
                sum += block.getLong(i);
            }
        }

        return sum;
    }

    /**
     * Must be a subset of {@link BlockBenchmark#RELEVANT_TYPE_BLOCK_COMBINATIONS}
     */
    @Param(
        {
            "boolean/array",
            "boolean/array-multivalue-null",
            "boolean/big-array",
            "boolean/big-array-multivalue-null",
            "boolean/vector",
            "boolean/vector-big-array",
            "boolean/vector-const",
            "BytesRef/array",
            "BytesRef/array-multivalue-null",
            "BytesRef/vector",
            "BytesRef/vector-const",
            "double/array",
            "double/array-multivalue-null",
            "double/big-array",
            "double/big-array-multivalue-null",
            "double/vector",
            "double/vector-big-array",
            "double/vector-const",
            "int/array",
            "int/array-multivalue-null",
            "int/big-array",
            "int/big-array-multivalue-null",
            "int/vector",
            "int/vector-big-array",
            "int/vector-const",
            "long/array",
            "long/array-multivalue-null",
            "long/big-array",
            "long/big-array-multivalue-null",
            "long/vector",
            "long/vector-big-array",
            "long/vector-const" }
    )
    public String dataTypeAndBlockKind;

    private BenchmarkBlocks data;

    private final BooleanVector mask = buildMask(BLOCK_TOTAL_POSITIONS);

    private final Block[] results = new Block[NUM_BLOCKS_PER_ITERATION];

    @Setup
    public void setup() {
        String[] params = dataTypeAndBlockKind.split("/");
        String dataType = params[0];
        String blockKind = params[1];

        data = buildBenchmarkBlocks(dataType, blockKind, mask, BLOCK_TOTAL_POSITIONS);
    }

    @Benchmark
    @OperationsPerInvocation(NUM_BLOCKS_PER_ITERATION * BLOCK_TOTAL_POSITIONS)
    public void run() {
        run(data, mask, results);
    }

    @TearDown(Level.Iteration)
    public void assertCheckSums() {
        String[] params = dataTypeAndBlockKind.split("/");
        String dataType = params[0];
        String blockKind = params[1];
        assertCheckSums(dataType, blockKind, data, results, BLOCK_TOTAL_POSITIONS);
    }
}
