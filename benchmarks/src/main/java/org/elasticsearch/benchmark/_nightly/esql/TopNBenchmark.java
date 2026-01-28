/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class TopNBenchmark {
    private static final BigArrays BIG_ARRAYS = BigArrays.NON_RECYCLING_INSTANCE;  // TODO real big arrays?
    private static final BlockFactory blockFactory = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    private static final int BLOCK_LENGTH = 4 * 1024;

    private static final String LONGS = "longs";
    private static final String INTS = "ints";
    private static final String DOUBLES = "doubles";
    private static final String BOOLEANS = "booleans";
    private static final String BYTES_REFS = "bytes_refs";
    private static final String TWO_LONGS = "two_" + LONGS;
    private static final String LONGS_AND_BYTES_REFS = LONGS + "_and_" + BYTES_REFS;

    static {
        LogConfigurator.configureESLogging();
        // Smoke test all the expected values and force loading subclasses more like prod
        selfTest();
    }

    static void selfTest() {
        try {
            for (String data : TopNBenchmark.class.getField("data").getAnnotationsByType(Param.class)[0].value()) {
                for (String topCount : TopNBenchmark.class.getField("topCount").getAnnotationsByType(Param.class)[0].value()) {
                    for (String sortedInput : TopNBenchmark.class.getField("sortedInput").getAnnotationsByType(Param.class)[0].value()) {
                        run(data, Integer.parseInt(topCount), Boolean.parseBoolean(sortedInput));
                    }
                }
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError();
        }
    }

    @Param({ LONGS, INTS, DOUBLES, BOOLEANS, BYTES_REFS, TWO_LONGS, LONGS_AND_BYTES_REFS })
    public String data;

    @Param({ "true", "false" })
    public boolean sortedInput;

    /*
        - 4096 is the page size,
        - 10000 reflects using a LIMIT with smaller pages, which seems to be a more realistic
          benchmark than having a LIMIT 10 and receiving pages from the data nodes that
          contain 4096 documents
     */
    @Param({ "10", "1000", "4096", "10000" })
    public int topCount;

    private static Operator operator(String data, int topCount, boolean sortedInput) {
        int count = switch (data) {
            case LONGS, INTS, DOUBLES, BOOLEANS, BYTES_REFS -> 1;
            case TWO_LONGS, LONGS_AND_BYTES_REFS -> 2;
            default -> throw new IllegalArgumentException("unsupported data type [" + data + "]");
        };
        List<ElementType> elementTypes = switch (data) {
            case LONGS -> List.of(ElementType.LONG);
            case INTS -> List.of(ElementType.INT);
            case DOUBLES -> List.of(ElementType.DOUBLE);
            case BOOLEANS -> List.of(ElementType.BOOLEAN);
            case BYTES_REFS -> List.of(ElementType.BYTES_REF);
            case TWO_LONGS -> List.of(ElementType.LONG, ElementType.LONG);
            case LONGS_AND_BYTES_REFS -> List.of(ElementType.LONG, ElementType.BYTES_REF);
            default -> throw new IllegalArgumentException("unsupported data type [" + data + "]");
        };
        List<TopNEncoder> encoders = switch (data) {
            case LONGS, INTS, DOUBLES, BOOLEANS -> List.of(TopNEncoder.DEFAULT_SORTABLE);
            case BYTES_REFS -> List.of(TopNEncoder.UTF8);
            case TWO_LONGS -> List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.DEFAULT_SORTABLE);
            case LONGS_AND_BYTES_REFS -> List.of(TopNEncoder.DEFAULT_SORTABLE, TopNEncoder.UTF8);
            default -> throw new IllegalArgumentException("unsupported data type [" + data + "]");
        };
        CircuitBreakerService breakerService = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            List.of(),
            ClusterSettings.createBuiltInClusterSettings()
        );
        return new TopNOperator(
            blockFactory,
            breakerService.getBreaker(CircuitBreaker.REQUEST),
            topCount,
            elementTypes,
            encoders,
            IntStream.range(0, count).mapToObj(c -> new TopNOperator.SortOrder(c, true, false)).toList(),
            8 * 1024,
            sortedInput ? TopNOperator.InputOrdering.SORTED : TopNOperator.InputOrdering.NOT_SORTED
        );
    }

    private static void checkExpected(int topCount, List<Page> pages) {
        if (topCount != pages.stream().mapToLong(Page::getPositionCount).sum()) {
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

    // This creates blocks with uniformly random distributed and sorted data
    private static Block block(String data) {
        return switch (data) {
            case LONGS -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);

                new Random().longs(BLOCK_LENGTH, 0, Long.MAX_VALUE).sorted().forEachOrdered(builder::appendLong);

                yield builder.build();
            }
            case INTS -> {
                var builder = blockFactory.newIntBlockBuilder(BLOCK_LENGTH);

                new Random().ints(BLOCK_LENGTH, 0, Integer.MAX_VALUE).sorted().forEachOrdered(builder::appendInt);

                yield builder.build();
            }
            case DOUBLES -> {
                var builder = blockFactory.newDoubleBlockBuilder(BLOCK_LENGTH);

                new Random().doubles(BLOCK_LENGTH, 0, Double.MAX_VALUE).sorted().forEachOrdered(builder::appendDouble);

                yield builder.build();
            }
            case BOOLEANS -> {
                BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(BLOCK_LENGTH);

                int falseCount = BLOCK_LENGTH / 2;
                int trueCount = BLOCK_LENGTH - falseCount;

                for (int i = 0; i < falseCount; i++) {
                    builder.appendBoolean(false);
                }
                for (int i = 0; i < trueCount; i++) {
                    builder.appendBoolean(true);
                }

                yield builder.build();
            }
            case BYTES_REFS -> {
                BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(BLOCK_LENGTH);
                new Random().ints(BLOCK_LENGTH, 0, Integer.MAX_VALUE)
                    .mapToObj(Integer::toString)
                    .sorted()
                    .forEachOrdered(s -> builder.appendBytesRef(new BytesRef(s)));

                yield builder.build();
            }
            default -> throw new UnsupportedOperationException("unsupported data [" + data + "]");
        };
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void run() {
        run(data, topCount, sortedInput);
    }

    private static void run(String data, int topCount, boolean sortedInput) {
        try (Operator operator = operator(data, topCount, sortedInput)) {
            Page page = page(data);
            for (int i = 0; i < 1024; i++) {
                operator.addInput(page.shallowCopy());
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
