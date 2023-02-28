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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.TopNOperator;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class TopNBenchmark {
    private static final int BLOCK_LENGTH = 8 * 1024;

    private static final String LONGS = "longs";
    private static final String INTS = "ints";
    private static final String DOUBLES = "doubles";
    private static final String BOOLEANS = "booleans";
    private static final String BYTES_REFS = "bytes_refs";
    private static final String TWO_LONGS = "two_" + LONGS;
    private static final String LONGS_AND_BYTES_REFS = LONGS + "_and_" + BYTES_REFS;

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        try {
            for (String data : TopNBenchmark.class.getField("data").getAnnotationsByType(Param.class)[0].value()) {
                for (String topCount : TopNBenchmark.class.getField("topCount").getAnnotationsByType(Param.class)[0].value()) {
                    run(data, Integer.parseInt(topCount));
                }
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError();
        }
    }

    @Param({ LONGS, INTS, DOUBLES, BOOLEANS, BYTES_REFS, TWO_LONGS, LONGS_AND_BYTES_REFS })
    public String data;

    @Param({ "10", "10000" })
    public int topCount;

    private static Operator operator(String data, int topCount) {
        int count = switch (data) {
            case LONGS, INTS, DOUBLES, BOOLEANS, BYTES_REFS -> 1;
            case TWO_LONGS, LONGS_AND_BYTES_REFS -> 2;
            default -> throw new IllegalArgumentException("unsupported data type [" + data + "]");
        };
        return new TopNOperator(topCount, IntStream.range(0, count).mapToObj(c -> new TopNOperator.SortOrder(c, false, false)).toList());
    }

    private static void checkExpected(int topCount, List<Page> pages) {
        if (topCount != pages.size()) {
            throw new AssertionError("expected [" + topCount + "] but got [" + pages.size() + "]");
        }
    }

    private static Page page(String data) {
        return switch (data) {
            case TWO_LONGS -> new Page(block(LONGS), block(LONGS));
            case LONGS_AND_BYTES_REFS -> new Page(block(LONGS), block(BYTES_REFS));
            default -> new Page(block(data));
        };
    }

    private static Block block(String data) {
        return switch (data) {
            case LONGS -> {
                var builder = LongBlock.newBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendLong(i);
                }
                yield builder.build();
            }
            case INTS -> {
                var builder = IntBlock.newBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendInt(i);
                }
                yield builder.build();
            }
            case DOUBLES -> {
                var builder = DoubleBlock.newBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendDouble(i);
                }
                yield builder.build();
            }
            case BOOLEANS -> {
                BooleanBlock.Builder builder = BooleanBlock.newBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendBoolean(i % 2 == 1);
                }
                yield builder.build();
            }
            case BYTES_REFS -> {
                BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendBytesRef(new BytesRef(Integer.toString(i)));
                }
                yield builder.build();
            }
            default -> throw new UnsupportedOperationException("unsupported data [" + data + "]");
        };
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void run() {
        run(data, topCount);
    }

    private static void run(String data, int topCount) {
        try (Operator operator = operator(data, topCount)) {
            Page page = page(data);
            for (int i = 0; i < 1024; i++) {
                operator.addInput(page);
            }
            operator.finish();
            List<Page> results = new ArrayList<>();
            Page p;
            while ((p = operator.getOutput()) != null) {
                results.add(p);
            }
            checkExpected(topCount, results);
        }
    }
}
