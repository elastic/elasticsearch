/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
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

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class BlockBenchmark {
    public static final int NUM_BLOCKS_PER_ITERATION = 1024;

    private static final Random random = new Random();

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        try {
            int totalPositions = 10;
            long[] actualCheckSums = new long[NUM_BLOCKS_PER_ITERATION];

            for (String dataType : getParamValues("dataType")) {
                for (String blockKind : getParamValues("blockKind")) {
                    BenchmarkBlocks data = buildBlocks(dataType, blockKind, totalPositions);
                    int[][] traversalOrders = createTraversalOrders(data.blocks, false);
                    run(dataType, data, traversalOrders, actualCheckSums);
                    assertCheckSums(data, actualCheckSums);
                }
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    private static String[] getParamValues(String parameterName) throws NoSuchFieldException {
        return BlockBenchmark.class.getField(parameterName).getAnnotationsByType(Param.class)[0].value();
    }

    private record BenchmarkBlocks(Block[] blocks, long[] checkSums) {};

    private static BenchmarkBlocks buildBlocks(String dataType, String blockKind, int totalPositions) {
        Block[] blocks = new Block[NUM_BLOCKS_PER_ITERATION];
        long[] checkSums = new long[NUM_BLOCKS_PER_ITERATION];

        switch (dataType) {
            case "int" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    int[] values = new int[totalPositions];
                    long sum = 0;
                    for (int i = 0; i < totalPositions; i++) {
                        values[i] = random.nextInt();
                        sum += values[i];
                    }
                    checkSums[blockIndex] = sum;

                    switch (blockKind) {
                        case "array" -> {
                            // TODO: bench with MVs and with nulls
                            blocks[blockIndex] = new IntArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "vector" -> {
                            // TODO: more vector kinds
                            IntVector vector = new IntArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException();
                        }
                    }
                }
            }
            case "long" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    long[] values = new long[totalPositions];
                    long sum = 0L;
                    for (int i = 0; i < totalPositions; i++) {
                        values[i] = random.nextLong();
                        sum += values[i];
                    }
                    checkSums[blockIndex] = sum;

                    switch (blockKind) {
                        case "array" -> {
                            blocks[blockIndex] = new LongArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "vector" -> {
                            LongVector vector = new LongArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException();
                        }
                    }
                }
            }
            default -> {
                throw new IllegalStateException();
            }
        }

        return new BenchmarkBlocks(blocks, checkSums);
    }

    private static int[][] createTraversalOrders(Block[] blocks, boolean randomized) {
        int[][] orders = new int[blocks.length][];

        for (int i = 0; i < blocks.length; i++) {
            IntStream positionsStream = IntStream.range(0, blocks[i].getPositionCount());

            if (randomized) {
                List<Integer> positions = new java.util.ArrayList<>(positionsStream.boxed().toList());
                Collections.shuffle(positions, random);
                orders[i] = positions.stream().mapToInt(x -> x).toArray();
            } else {
                orders[i] = positionsStream.toArray();
            }
        }

        return orders;
    }

    private static void run(String dataType, BenchmarkBlocks data, int[][] traversalOrders, long[] resultCheckSums) {
        switch (dataType) {
            case "int" -> {
                // TODO benchmark random access in addition to sequential
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    IntBlock block = (IntBlock) data.blocks[blockIndex];
                    long sum = 0;

                    int positionCount = block.getPositionCount();
                    for (int position : traversalOrders[blockIndex]) {
                        int start = block.getFirstValueIndex(position);
                        int end = start + block.getValueCount(position);
                        for (int i = start; i < end; i++) {
                            sum += block.getInt(i);
                        }
                    }

                    resultCheckSums[blockIndex] = sum;
                }
            }
            case "long" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    LongBlock block = (LongBlock) data.blocks[blockIndex];
                    long sum = 0;

                    int positionCount = block.getPositionCount();
                    for (int position : traversalOrders[blockIndex]) {
                        int start = block.getFirstValueIndex(position);
                        int end = start + block.getValueCount(position);
                        for (int i = start; i < end; i++) {
                            sum += block.getLong(i);
                        }
                    }

                    resultCheckSums[blockIndex] = sum;
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

    private static int totalPositions(String blockLength) {
        return (int) ByteSizeValue.parseBytesSizeValue(blockLength, "block length").getBytes();
    }

    private static boolean isRandom(String accessType) {
        return accessType.equalsIgnoreCase("random");
    }

    @Param({ "1K", "8K" })
    public String blockLength;

    // TODO other types
    @Param({ "int", "long" })
    public String dataType;

    @Param({ "array", "vector" })
    public String blockKind;

    @Param({ "sequential", "random" })
    public String accessType;

    private BenchmarkBlocks data;

    private int[][] traversalOrders;

    private final long[] actualCheckSums = new long[NUM_BLOCKS_PER_ITERATION];

    @Setup
    public void setup() {
        data = buildBlocks(dataType, blockKind, totalPositions(blockLength));
        traversalOrders = createTraversalOrders(data.blocks, isRandom(accessType));
    }

    @Benchmark
    @OperationsPerInvocation(BlockBenchmark.NUM_BLOCKS_PER_ITERATION)
    public void run() {
        run(dataType, data, traversalOrders, actualCheckSums);
    }

    @TearDown(Level.Iteration)
    public void assertCheckSums() {
        assertCheckSums(data, actualCheckSums);
    }
}
