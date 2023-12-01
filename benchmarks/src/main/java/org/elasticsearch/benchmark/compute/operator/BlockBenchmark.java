/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operator;

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
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class BlockBenchmark {
    public static final int NUM_BLOCKS_PER_RUN = 1024;

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        // TODO
    }

    @Param({ "1", "8" })
    public String blockLengthInKilos;

    // TODO other types
    @Param({ "int", "long" })
    public String dataType;

    @Param({ "array", "vector" })
    public String blockKind;

    @Setup
    public void buildBlocks() {
        switch (dataType) {
            case "int" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_RUN; blockIndex++) {
                    int[] values = new int[totalPositions()];
                    int sum = 0;
                    for (int i = 0; i < totalPositions(); i++) {
                        values[i] = random.nextInt();
                        sum += values[i];
                    }
                    checkSums[blockIndex] = sum;

                    switch (blockKind) {
                        case "array" -> {
                            // TODO: bench with MVs and with nulls
                            blocks[blockIndex] = new IntArrayBlock(
                                values,
                                totalPositions(),
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "vector" -> {
                            // TODO: more vector kinds
                            IntVector vector = new IntArrayVector(values, totalPositions());
                            blocks[blockIndex] = vector.asBlock();
                        }
                        default -> {
                            throw new IllegalStateException();
                        }
                    }
                }
            }
            case "long" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_RUN; blockIndex++) {
                    long[] values = new long[totalPositions()];
                    long sum = 0L;
                    for (int i = 0; i < totalPositions(); i++) {
                        values[i] = random.nextLong();
                        sum += values[i];
                    }
                    checkSums[blockIndex] = sum;

                    switch (blockKind) {
                        case "array" -> {
                            blocks[blockIndex] = new LongArrayBlock(
                                values,
                                totalPositions(),
                                null,
                                null,
                                Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
                            );
                        }
                        case "vector" -> {
                            LongVector vector = new LongArrayVector(values, totalPositions());
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
    }

    @Benchmark
    @OperationsPerInvocation(BlockBenchmark.NUM_BLOCKS_PER_RUN)
    public void run() {
        int totalPositions = totalPositions();
        switch (dataType) {
            case "int" -> {
                // TODO benchmark random access in addition to sequential
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_RUN; blockIndex++) {
                    IntBlock block = (IntBlock) blocks[blockIndex];
                    long sum = 0;

                    int positionCount = block.getPositionCount();
                    for (int position = 0; position < positionCount; position++) {
                        int start = block.getFirstValueIndex(position);
                        int end = start + block.getValueCount(position);
                        for (int i = start; i < end; i++) {
                            sum += block.getInt(i);
                        }
                    }

                    if (sum != checkSums[blockIndex]) {
                        throw new AssertionError("wrong checksum");
                    }
                }
            }
            case "long" -> {
                for (int blockIndex = 0; blockIndex < NUM_BLOCKS_PER_RUN; blockIndex++) {
                    LongBlock block = (LongBlock) blocks[blockIndex];
                    long sum = 0;

                    int positionCount = block.getPositionCount();
                    for (int position = 0; position < positionCount; position++) {
                        int start = block.getFirstValueIndex(position);
                        int end = start + block.getValueCount(position);
                        for (int i = start; i < end; i++) {
                            sum += block.getLong(i);
                        }
                    }

                    if (sum != checkSums[blockIndex]) {
                        throw new AssertionError("wrong checksum");
                    }
                }
            }
            default -> {
                throw new IllegalStateException();
            }
        }
    }

    private final Random random = new Random();

    private final Block[] blocks = new Block[NUM_BLOCKS_PER_RUN];
    private final long[] checkSums = new long[NUM_BLOCKS_PER_RUN];

    private int totalPositions() {
        return Integer.parseInt(blockLengthInKilos);
    }
}
