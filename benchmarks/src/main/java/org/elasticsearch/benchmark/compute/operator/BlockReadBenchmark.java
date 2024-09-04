/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.*;
import org.elasticsearch.compute.data.*;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class BlockReadBenchmark extends BlockBenchmark {
    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        int totalPositions = 10;
        long[] actualCheckSums = new long[NUM_BLOCKS_PER_ITERATION];

        for (String paramString : RELEVANT_TYPE_BLOCK_COMBINATIONS) {
            String[] params = paramString.split("/");
            String dataType = params[0];
            String blockKind = params[1];

            BenchmarkBlocks data = buildBenchmarkBlocks(dataType, blockKind, totalPositions);
            int[][] traversalOrders = createTraversalOrders(data.blocks(), false);
            run(dataType, data, traversalOrders, actualCheckSums);
            assertCheckSums(data, actualCheckSums);
        }
    }

    private static int[][] createTraversalOrders(Block[] blocks, boolean randomized) {
        int[][] orders = new int[blocks.length][];

        for (int i = 0; i < blocks.length; i++) {
            IntStream positionsStream = IntStream.range(0, blocks[i].getPositionCount());

            if (randomized) {
                List<Integer> positions = new ArrayList<>(positionsStream.boxed().toList());
                Collections.shuffle(positions, random);
                orders[i] = positions.stream().mapToInt(x -> x).toArray();
            } else {
                orders[i] = positionsStream.toArray();
            }
        }

        return orders;
    }

    record BenchmarkBlocks(Block[] blocks, long[] checkSums) {};

    static BenchmarkBlocks buildBenchmarkBlocks(String dataType, String blockKind, int totalPositions) {
        Block[] blocks = BlockBenchmark.buildBlocks(dataType, blockKind, totalPositions);
        long[] checkSums = new long[NUM_BLOCKS_PER_ITERATION];
        switch (dataType) {
            case "boolean" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    BooleanBlock block = (BooleanBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeBooleanCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "BytesRef" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    BytesRefBlock block = (BytesRefBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeBytesRefCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "double" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    DoubleBlock block = (DoubleBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeDoubleCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "int" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    IntBlock block = (IntBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeIntCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "long" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    LongBlock block = (LongBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeLongCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            // TODO float
            default -> throw new IllegalStateException("illegal data type [" + dataType + "]");
        }
        return new BenchmarkBlocks(blocks, checkSums);
    }

    private static void run(String dataType, BenchmarkBlocks data, int[][] traversalOrders, long[] resultCheckSums) {
        switch (dataType) {
            case "boolean" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    BooleanBlock block = (BooleanBlock) data.blocks[blockIndex];

                    resultCheckSums[blockIndex] = computeBooleanCheckSum(block, traversalOrders[blockIndex]);
                }
            }
            case "BytesRef" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    BytesRefBlock block = (BytesRefBlock) data.blocks[blockIndex];

                    resultCheckSums[blockIndex] = computeBytesRefCheckSum(block, traversalOrders[blockIndex]);
                }
            }
            case "double" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    DoubleBlock block = (DoubleBlock) data.blocks[blockIndex];

                    resultCheckSums[blockIndex] = computeDoubleCheckSum(block, traversalOrders[blockIndex]);
                }
            }
            case "int" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    IntBlock block = (IntBlock) data.blocks[blockIndex];

                    resultCheckSums[blockIndex] = computeIntCheckSum(block, traversalOrders[blockIndex]);
                }
            }
            case "long" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    LongBlock block = (LongBlock) data.blocks[blockIndex];

                    resultCheckSums[blockIndex] = computeLongCheckSum(block, traversalOrders[blockIndex]);
                }
            }
            default -> {
                throw new IllegalStateException();
            }
        }
    }

    private static void assertCheckSums(BenchmarkBlocks data, long[] actualCheckSums) {
        for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
            if (actualCheckSums[blockIndex] != data.checkSums[blockIndex]) {
                throw new AssertionError("checksums do not match for block [" + blockIndex + "]");
            }
        }
    }

    private static long computeBooleanCheckSum(BooleanBlock block, int[] traversalOrder) {
        long sum = 0;

        for (int position : traversalOrder) {
            if (block.isNull(position)) {
                continue;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                sum += block.getBoolean(i) ? 1 : 0;
            }
        }

        return sum;
    }

    private static long computeBytesRefCheckSum(BytesRefBlock block, int[] traversalOrder) {
        long sum = 0;
        BytesRef scratch = new BytesRef();

        for (int position : traversalOrder) {
            if (block.isNull(position)) {
                continue;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                BytesRef v = block.getBytesRef(i, scratch);
                sum += v.length > 0 ? v.bytes[v.offset] : 0;
            }
        }

        return sum;
    }

    private static long computeDoubleCheckSum(DoubleBlock block, int[] traversalOrder) {
        long sum = 0;

        for (int position : traversalOrder) {
            if (block.isNull(position)) {
                continue;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                // Use an operation that is not affected by rounding errors. Otherwise, the result may depend on the traversalOrder.
                sum += (long) block.getDouble(i);
            }
        }

        return sum;
    }

    private static long computeIntCheckSum(IntBlock block, int[] traversalOrder) {
        int sum = 0;

        for (int position : traversalOrder) {
            if (block.isNull(position)) {
                continue;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                sum += block.getInt(i);
            }
        }

        return sum;
    }

    private static long computeLongCheckSum(LongBlock block, int[] traversalOrder) {
        long sum = 0;

        for (int position : traversalOrder) {
            if (block.isNull(position)) {
                continue;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                sum += block.getLong(i);
            }
        }

        return sum;
    }

    private static boolean isRandom(String accessType) {
        return accessType.equalsIgnoreCase("random");
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

    @Param({ "sequential", "random" })
    public String accessType;

    private BenchmarkBlocks data;

    private int[][] traversalOrders;

    private final long[] actualCheckSums = new long[NUM_BLOCKS_PER_ITERATION];

    @Setup
    public void setup() {
        String[] params = dataTypeAndBlockKind.split("/");
        String dataType = params[0];
        String blockKind = params[1];

        data = buildBenchmarkBlocks(dataType, blockKind, BLOCK_TOTAL_POSITIONS);
        traversalOrders = createTraversalOrders(data.blocks(), isRandom(accessType));
    }

    @Benchmark
    @OperationsPerInvocation(NUM_BLOCKS_PER_ITERATION * BLOCK_TOTAL_POSITIONS)
    public void run() {
        String[] params = dataTypeAndBlockKind.split("/");
        String dataType = params[0];

        run(dataType, data, traversalOrders, actualCheckSums);
    }

    @TearDown(Level.Iteration)
    public void assertCheckSums() {
        assertCheckSums(data, actualCheckSums);
    }
}
