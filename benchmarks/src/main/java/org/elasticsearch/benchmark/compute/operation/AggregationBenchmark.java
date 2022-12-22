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
import org.elasticsearch.compute.data.LongArrayBlock;
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
public class AggregationBenchmark {
    private static final int PAGE_LENGTH = 8 * 1024;
    private static final Page PAGE = new Page(new LongArrayBlock(LongStream.range(0, PAGE_LENGTH).toArray(), PAGE_LENGTH));

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        run("avg");
        run("count");
        run("min");
        run("max");
        try {
            run("sum");
        } catch (ArithmeticException e) {

        }
    }

    @Param({ "avg", "count", "min", "max", "sum" })
    private String op;

    private static Operator operator(String op) {
        AggregatorFunction.Factory factory = switch (op) {
            case "avg" -> AggregatorFunction.AVG_LONGS;
            case "count" -> AggregatorFunction.COUNT;
            case "min" -> AggregatorFunction.MIN_LONGS;
            case "max" -> AggregatorFunction.MAX;
            case "sum" -> AggregatorFunction.SUM_LONGS;
            default -> throw new IllegalArgumentException("bad impl " + op);
        };
        return new AggregationOperator(List.of(new Aggregator(factory, AggregatorMode.SINGLE, 0)));
    }

    private static void checkExpected(Block block, String op) {
        switch (op) {
            case "avg":
                if (block.getDouble(0) != (PAGE_LENGTH - 1) / 2.0) {
                    throw new AssertionError("expected [" + ((PAGE_LENGTH - 1) / 2.0) + "] but was [" + block.getDouble(0) + "]");
                }
                return;
            case "count":
                if (block.getLong(0) != PAGE_LENGTH * 1024) {
                    throw new AssertionError("expected [" + (PAGE_LENGTH * 1024) + "] but was [" + block.getLong(0) + "]");
                }
                return;
            case "min":
                if (block.getLong(0) != 0L) {
                    throw new AssertionError("expected [0] but was [" + block.getLong(0) + "]");
                }
                return;
            case "max":
                if (block.getDouble(0) != PAGE_LENGTH - 1) {
                    throw new AssertionError("expected [" + (PAGE_LENGTH - 1) + "] but was [" + block.getDouble(0) + "]");
                }
                return;
            case "sum":
                long expected = (PAGE_LENGTH * (PAGE_LENGTH - 1L)) * 1024L / 2;
                if (block.getLong(0) != expected) {
                    throw new AssertionError("expected [" + expected + "] but was [" + block.getLong(0) + "]");
                }
                return;
            default:
                throw new IllegalArgumentException("bad impl " + op);
        }
    }

    @Benchmark
    @OperationsPerInvocation(1024 * PAGE_LENGTH)
    public void run() {
        run(op);
    }

    private static void run(String op) {
        Operator operator = operator(op);
        for (int i = 0; i < 1024; i++) {
            operator.addInput(PAGE);
        }
        operator.finish();
        checkExpected(operator.getOutput().getBlock(0), op);
    }
}
