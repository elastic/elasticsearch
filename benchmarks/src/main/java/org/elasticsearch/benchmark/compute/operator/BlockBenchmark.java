/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBigArrayBlock;
import org.elasticsearch.compute.data.BooleanBigArrayVector;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBigArrayBlock;
import org.elasticsearch.compute.data.DoubleBigArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBigArrayBlock;
import org.elasticsearch.compute.data.LongBigArrayVector;
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

import java.util.ArrayList;
import java.util.BitSet;
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

    /**
     * All data type/block kind combinations to be loaded before the benchmark.
     * It is important to be exhaustive here so that all implementers of {@link IntBlock#getInt(int)} are actually loaded when we benchmark
     * {@link IntBlock}s etc.
     */
    // We could also consider DocBlocks/DocVectors but they do not implement any of the typed block interfaces like IntBlock etc.
    public static final String[] RELEVANT_TYPE_BLOCK_COMBINATIONS = {
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
        "long/vector-const" };
    public static final int NUM_BLOCKS_PER_ITERATION = 1024;
    public static final int BLOCK_TOTAL_POSITIONS = 8096;

    private static final double MV_PERCENTAGE = 0.3;
    private static final double NULL_PERCENTAGE = 0.1;
    private static final int MAX_MV_ELEMENTS = 100;
    private static final int MAX_BYTES_REF_LENGTH = 255;

    private static final Random random = new Random();

    private static final BlockFactory blockFactory = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        int totalPositions = 10;
        long[] actualCheckSums = new long[NUM_BLOCKS_PER_ITERATION];

        for (String paramString : RELEVANT_TYPE_BLOCK_COMBINATIONS) {
            String[] params = paramString.split("/");
            String dataType = params[0];
            String blockKind = params[1];

            BenchmarkBlocks data = buildBlocks(dataType, blockKind, totalPositions);
            int[][] traversalOrders = createTraversalOrders(data.blocks, false);
            run(dataType, data, traversalOrders, actualCheckSums);
            assertCheckSums(data, actualCheckSums);
        }
    }

    private record BenchmarkBlocks(Block[] blocks, long[] checkSums) {};

    private static BenchmarkBlocks buildBlocks(String dataType, String blockKind, int totalPositions) {
        Block[] blocks = new Block[NUM_BLOCKS_PER_ITERATION];
        long[] checkSums = new long[NUM_BLOCKS_PER_ITERATION];

        switch (dataType) {
            case "boolean" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    if (blockKind.equalsIgnoreCase("vector-const")) {
                        BooleanVector vector = blockFactory.newConstantBooleanVector(random.nextBoolean(), totalPositions);
                        blocks[blockIndex] = vector.asBlock();
                        continue;
                    }

                    boolean[] values = new boolean[totalPositions];
                    for (int i = 0; i < totalPositions; i++) {
                        values[i] = random.nextBoolean();
                    }

                    switch (blockKind) {
                        case "array" -> {
                            blocks[blockIndex] = blockFactory.newBooleanArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);

                            blocks[blockIndex] = blockFactory.newBooleanArrayBlock(
                                values,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED
                            );
                        }
                        case "big-array" -> {
                            BitArray valuesBigArray = new BitArray(totalPositions, BigArrays.NON_RECYCLING_INSTANCE);
                            for (int i = 0; i < values.length; i++) {
                                if (values[i]) {
                                    valuesBigArray.set(i);
                                }
                            }

                            blocks[blockIndex] = new BooleanBigArrayBlock(
                                valuesBigArray,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING,
                                blockFactory
                            );
                        }
                        case "big-array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);
                            BitArray valuesBigArray = new BitArray(totalPositions, BigArrays.NON_RECYCLING_INSTANCE);
                            for (int i = 0; i < values.length; i++) {
                                if (values[i]) {
                                    valuesBigArray.set(i);
                                }
                            }

                            blocks[blockIndex] = new BooleanBigArrayBlock(
                                valuesBigArray,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED,
                                blockFactory
                            );
                        }
                        case "vector" -> {
                            BooleanVector vector = blockFactory.newBooleanArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        case "vector-big-array" -> {
                            BitArray valuesBigArray = new BitArray(totalPositions, BigArrays.NON_RECYCLING_INSTANCE);
                            for (int i = 0; i < values.length; i++) {
                                if (values[i]) {
                                    valuesBigArray.set(i);
                                }
                            }
                            BooleanVector vector = new BooleanBigArrayVector(valuesBigArray, totalPositions, blockFactory);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException("illegal block kind [" + blockKind + "]");
                        }
                    }
                }

                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    BooleanBlock block = (BooleanBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeBooleanCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "BytesRef" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    if (blockKind.equalsIgnoreCase("vector-const")) {
                        byte[] bytes = new byte[random.nextInt(MAX_BYTES_REF_LENGTH)];
                        random.nextBytes(bytes);

                        BytesRefVector vector = blockFactory.newConstantBytesRefVector(new BytesRef(bytes), totalPositions);
                        blocks[blockIndex] = vector.asBlock();
                        continue;
                    }

                    BytesRefArray values = new BytesRefArray(totalPositions, BigArrays.NON_RECYCLING_INSTANCE);
                    byte[] bytes;
                    for (int i = 0; i < totalPositions; i++) {
                        bytes = new byte[random.nextInt(MAX_BYTES_REF_LENGTH)];
                        random.nextBytes(bytes);
                        values.append(new BytesRef(bytes));
                    }

                    switch (blockKind) {
                        case "array" -> {
                            blocks[blockIndex] = blockFactory.newBytesRefArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);

                            blocks[blockIndex] = blockFactory.newBytesRefArrayBlock(
                                values,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED
                            );
                        }
                        case "vector" -> {
                            BytesRefVector vector = blockFactory.newBytesRefArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException("illegal block kind [" + blockKind + "]");
                        }
                    }
                }

                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    BytesRefBlock block = (BytesRefBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeBytesRefCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "double" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    if (blockKind.equalsIgnoreCase("vector-const")) {
                        DoubleVector vector = blockFactory.newConstantDoubleVector(random.nextDouble() * 1000000.0, totalPositions);
                        blocks[blockIndex] = vector.asBlock();
                        continue;
                    }

                    double[] values = new double[totalPositions];
                    for (int i = 0; i < totalPositions; i++) {
                        values[i] = random.nextDouble() * 1000000.0;
                    }

                    switch (blockKind) {
                        case "array" -> {
                            blocks[blockIndex] = blockFactory.newDoubleArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);

                            blocks[blockIndex] = blockFactory.newDoubleArrayBlock(
                                values,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED
                            );
                        }
                        case "big-array" -> {
                            DoubleArray valuesBigArray = blockFactory.bigArrays().newDoubleArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }

                            blocks[blockIndex] = new DoubleBigArrayBlock(
                                valuesBigArray,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING,
                                blockFactory
                            );
                        }
                        case "big-array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);
                            DoubleArray valuesBigArray = blockFactory.bigArrays().newDoubleArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }

                            blocks[blockIndex] = new DoubleBigArrayBlock(
                                valuesBigArray,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED,
                                blockFactory
                            );
                        }
                        case "vector" -> {
                            DoubleVector vector = blockFactory.newDoubleArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        case "vector-big-array" -> {
                            DoubleArray valuesBigArray = blockFactory.bigArrays().newDoubleArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }
                            DoubleVector vector = new DoubleBigArrayVector(valuesBigArray, totalPositions, blockFactory);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException("illegal block kind [" + blockKind + "]");
                        }
                    }
                }

                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    DoubleBlock block = (DoubleBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeDoubleCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "int" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    if (blockKind.equalsIgnoreCase("vector-const")) {
                        IntVector vector = blockFactory.newConstantIntVector(random.nextInt(), totalPositions);
                        blocks[blockIndex] = vector.asBlock();
                        continue;
                    }

                    int[] values = new int[totalPositions];
                    for (int i = 0; i < totalPositions; i++) {
                        values[i] = random.nextInt();
                    }

                    switch (blockKind) {
                        case "array" -> {
                            blocks[blockIndex] = blockFactory.newIntArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);

                            blocks[blockIndex] = blockFactory.newIntArrayBlock(
                                values,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED
                            );
                        }
                        case "big-array" -> {
                            IntArray valuesBigArray = blockFactory.bigArrays().newIntArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }

                            blocks[blockIndex] = new IntBigArrayBlock(
                                valuesBigArray,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING,
                                blockFactory
                            );
                        }
                        case "big-array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);
                            IntArray valuesBigArray = blockFactory.bigArrays().newIntArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }

                            blocks[blockIndex] = new IntBigArrayBlock(
                                valuesBigArray,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED,
                                blockFactory
                            );
                        }
                        case "vector" -> {
                            IntVector vector = blockFactory.newIntArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        case "vector-big-array" -> {
                            IntArray valuesBigArray = blockFactory.bigArrays().newIntArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }
                            IntVector vector = new IntBigArrayVector(valuesBigArray, totalPositions, blockFactory);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException("illegal block kind [" + blockKind + "]");
                        }
                    }
                }

                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    IntBlock block = (IntBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeIntCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            case "long" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    if (blockKind.equalsIgnoreCase("vector-const")) {
                        LongVector vector = blockFactory.newConstantLongVector(random.nextLong(), totalPositions);
                        blocks[blockIndex] = vector.asBlock();
                        continue;
                    }

                    long[] values = new long[totalPositions];
                    for (int i = 0; i < totalPositions; i++) {
                        values[i] = random.nextLong();
                    }

                    switch (blockKind) {
                        case "array" -> {
                            blocks[blockIndex] = blockFactory.newLongArrayBlock(
                                values,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);

                            blocks[blockIndex] = blockFactory.newLongArrayBlock(
                                values,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED
                            );
                        }
                        case "big-array" -> {
                            LongArray valuesBigArray = blockFactory.bigArrays().newLongArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }

                            blocks[blockIndex] = new LongBigArrayBlock(
                                valuesBigArray,
                                totalPositions,
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING,
                                blockFactory
                            );
                        }
                        case "big-array-multivalue-null" -> {
                            int[] firstValueIndexes = randomFirstValueIndexes(totalPositions);
                            int positionCount = firstValueIndexes.length - 1;
                            BitSet nulls = randomNulls(positionCount);
                            LongArray valuesBigArray = blockFactory.bigArrays().newLongArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }

                            blocks[blockIndex] = new LongBigArrayBlock(
                                valuesBigArray,
                                positionCount,
                                firstValueIndexes,
                                nulls,
                                Block.MvOrdering.UNORDERED,
                                blockFactory
                            );
                        }
                        case "vector" -> {
                            LongVector vector = blockFactory.newLongArrayVector(values, totalPositions);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        case "vector-big-array" -> {
                            LongArray valuesBigArray = blockFactory.bigArrays().newLongArray(totalPositions, false);
                            for (int i = 0; i < values.length; i++) {
                                valuesBigArray.set(i, values[i]);
                            }
                            LongVector vector = new LongBigArrayVector(valuesBigArray, totalPositions, blockFactory);
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException("illegal block kind [" + blockKind + "]");
                        }
                    }
                }

                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_ITERATION; blockIndex++) {
                    LongBlock block = (LongBlock) blocks[blockIndex];
                    checkSums[blockIndex] = computeLongCheckSum(block, IntStream.range(0, block.getPositionCount()).toArray());
                }
            }
            default -> {
                throw new IllegalStateException("illegal data type [" + dataType + "]");
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

    private static int[] randomFirstValueIndexes(int totalPositions) {
        ArrayList<Integer> firstValueIndexes = new ArrayList<>();
        firstValueIndexes.add(0);

        int currentPosition = 0;
        int nextPosition;
        while (currentPosition < totalPositions) {
            if (random.nextDouble() < MV_PERCENTAGE) {
                nextPosition = Math.min(currentPosition + 1 + random.nextInt(MAX_MV_ELEMENTS), totalPositions);
            } else {
                nextPosition = currentPosition + 1;
            }
            firstValueIndexes.add(nextPosition);
            currentPosition = nextPosition;
        }

        return firstValueIndexes.stream().mapToInt(x -> x).toArray();
    }

    private static BitSet randomNulls(int positionCount) {
        BitSet nulls = new BitSet(positionCount);
        for (int i = 0; i < positionCount; i++) {
            if (random.nextDouble() < NULL_PERCENTAGE) {
                nulls.set(i);
            }
        }

        return nulls;
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
        BytesRef currentValue = new BytesRef();

        for (int position : traversalOrder) {
            if (block.isNull(position)) {
                continue;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + block.getValueCount(position);
            for (int i = start; i < end; i++) {
                block.getBytesRef(i, currentValue);
                sum += currentValue.length > 0 ? currentValue.bytes[0] : 0;
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

        data = buildBlocks(dataType, blockKind, BLOCK_TOTAL_POSITIONS);
        traversalOrders = createTraversalOrders(data.blocks, isRandom(accessType));
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
