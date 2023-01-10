/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operation;

import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.Operator;
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
import java.util.stream.LongStream;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class AggregatorBenchmark {
    private static final int BLOCK_LENGTH = 8 * 1024;

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        try {
            for (String op : AggregatorBenchmark.class.getField("op").getAnnotationsByType(Param.class)[0].value()) {
                for (String blockType : AggregatorBenchmark.class.getField("blockType").getAnnotationsByType(Param.class)[0].value()) {
                    run(op, blockType);
                }
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError();
        }
    }

    @Param({ "avg", "count", "min", "max", "sum" })
    public String op;

    @Param({ "vector", "half_null" })
    public String blockType;

    private static Operator operator(String op) {
        AggregatorFunction.Factory factory = switch (op) {
            case "avg" -> AggregatorFunction.AVG_LONGS;
            case "count" -> AggregatorFunction.COUNT;
            case "min" -> AggregatorFunction.MIN_LONGS;
            case "max" -> AggregatorFunction.MAX_LONGS;
            case "sum" -> AggregatorFunction.SUM_LONGS;
            default -> throw new IllegalArgumentException("bad op " + op);
        };
        return new AggregationOperator(List.of(new Aggregator(factory, AggregatorMode.SINGLE, 0)));
    }

    private static void checkExpected(Block block, String op, String blockType) {
        String prefix = String.format("[%s][%s] ", op, blockType);
        switch (op) {
            case "avg":
                if (block.getDouble(0) != (BLOCK_LENGTH - 1) / 2.0) {
                    throw new AssertionError(prefix + "expected [" + ((BLOCK_LENGTH - 1) / 2.0) + "] but was [" + block.getDouble(0) + "]");
                }
                return;
            case "count":
                if (block.getLong(0) != BLOCK_LENGTH * 1024) {
                    throw new AssertionError(prefix + "expected [" + (BLOCK_LENGTH * 1024) + "] but was [" + block.getLong(0) + "]");
                }
                return;
            case "min":
                if (block.getLong(0) != 0L) {
                    throw new AssertionError(prefix + "expected [0] but was [" + block.getLong(0) + "]");
                }
                return;
            case "max":
                if (block.getLong(0) != BLOCK_LENGTH - 1) {
                    throw new AssertionError(prefix + "expected [" + (BLOCK_LENGTH - 1) + "] but was [" + block.getLong(0) + "]");
                }
                return;
            case "sum":
                long expected = (BLOCK_LENGTH * (BLOCK_LENGTH - 1L)) * 1024L / 2;
                if (block.getLong(0) != expected) {
                    throw new AssertionError(prefix + "expected [" + expected + "] but was [" + block.getLong(0) + "]");
                }
                return;
            default:
                throw new IllegalArgumentException("bad op " + op);
        }
    }

    private static Page page(String blockType) {
        return new Page(switch (blockType) {
            case "vector" -> new LongVector(LongStream.range(0, BLOCK_LENGTH).toArray(), BLOCK_LENGTH).asBlock();
            case "multivalued" -> {
                BlockBuilder builder = BlockBuilder.newLongBlockBuilder(BLOCK_LENGTH);
                builder.beginPositionEntry();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendLong(i);
                    if (i % 5 == 0) {
                        builder.endPositionEntry();
                        builder.beginPositionEntry();
                    }
                }
                builder.endPositionEntry();
                yield builder.build();
            }
            case "half_null" -> {
                BlockBuilder builder = BlockBuilder.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendLong(i);
                    builder.appendNull();
                }
                yield builder.build();
            }
            default -> throw new IllegalArgumentException("bad blockType: " + blockType);
        });
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void run() {
        run(op, blockType);
    }

    private static void run(String op, String blockType) {
        Operator operator = operator(op);
        Page page = page(blockType);
        for (int i = 0; i < 1024; i++) {
            operator.addInput(page);
        }
        operator.finish();
        checkExpected(operator.getOutput().getBlock(0), op, blockType);
    }
}
