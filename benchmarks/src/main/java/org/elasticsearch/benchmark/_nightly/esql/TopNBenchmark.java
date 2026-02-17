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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class TopNBenchmark {
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

    private static final String ASC = "_asc";
    private static final String DESC = "_desc";

    private static final String AND = "_and_";

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

    @Param(
        {
            LONGS + ASC,
            LONGS + DESC,
            INTS + ASC,
            DOUBLES + ASC,
            BOOLEANS + ASC,
            BYTES_REFS + ASC,
            LONGS + ASC + AND + LONGS + ASC,
            LONGS + ASC + AND + LONGS + DESC,
            LONGS + DESC + AND + LONGS + DESC,
            LONGS + ASC + AND + BYTES_REFS + ASC,
            LONGS + DESC + AND + BYTES_REFS + DESC }
    )
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
        String[] dataSpec = data.split("_and_");
        List<ElementType> elementTypes = Arrays.stream(dataSpec).map(TopNBenchmark::elementType).toList();
        List<TopNEncoder> encoders = Arrays.stream(dataSpec).map(TopNBenchmark::encoder).toList();
        List<TopNOperator.SortOrder> sortOrders = IntStream.range(0, dataSpec.length).mapToObj(c -> sortOrder(c, dataSpec[c])).toList();
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
            sortOrders,
            8 * 1024,
            sortedInput ? TopNOperator.InputOrdering.SORTED : TopNOperator.InputOrdering.NOT_SORTED
        );
    }

    private static ElementType elementType(String data) {
        return switch (data.replace(ASC, "").replace(DESC, "")) {
            case LONGS -> ElementType.LONG;
            case INTS -> ElementType.INT;
            case DOUBLES -> ElementType.DOUBLE;
            case BOOLEANS -> ElementType.BOOLEAN;
            case BYTES_REFS -> ElementType.BYTES_REF;
            default -> throw new IllegalArgumentException("unsupported data type [" + data + "]");
        };
    }

    private static TopNEncoder encoder(String data) {
        return switch (data.replace(ASC, "").replace(DESC, "")) {
            case LONGS, INTS, DOUBLES, BOOLEANS -> TopNEncoder.DEFAULT_SORTABLE;
            case BYTES_REFS -> TopNEncoder.UTF8;
            default -> throw new IllegalArgumentException("unsupported data type [" + data + "]");
        };
    }

    private static boolean ascDesc(String data) {
        if (data.endsWith(ASC)) {
            return true;
        } else if (data.endsWith(DESC)) {
            return false;
        } else {
            throw new IllegalArgumentException("data neither asc nor desc: " + data);
        }
    }

    private static TopNOperator.SortOrder sortOrder(int channel, String data) {
        return new TopNOperator.SortOrder(channel, ascDesc(data), false);
    }

    private static void checkExpected(int topCount, List<Page> pages) {
        if (topCount != pages.stream().mapToLong(Page::getPositionCount).sum()) {
            throw new AssertionError("expected [" + topCount + "] but got [" + pages.size() + "]");
        }
    }

    private static Page page(boolean sortedInput, String data) {
        String[] dataSpec = data.split("_and_");
        return new Page(Arrays.stream(dataSpec).map(d -> block(sortedInput, d)).toArray(Block[]::new));
    }

    // This creates blocks with uniformly random distributed and sorted data
    private static Block block(boolean sortedInput, String data) {
        return switch (data.replace(ASC, "").replace(DESC, "")) {
            case LONGS -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                maybeSort(sortedInput, data, new Random().longs(BLOCK_LENGTH, 0, Long.MAX_VALUE).boxed()).forEach(builder::appendLong);
                yield builder.build();
            }
            case INTS -> {
                var builder = blockFactory.newIntBlockBuilder(BLOCK_LENGTH);
                maybeSort(sortedInput, data, new Random().ints(BLOCK_LENGTH, 0, Integer.MAX_VALUE).boxed()).forEach(builder::appendInt);
                yield builder.build();
            }
            case DOUBLES -> {
                var builder = blockFactory.newDoubleBlockBuilder(BLOCK_LENGTH);
                maybeSort(sortedInput, data, new Random().doubles(BLOCK_LENGTH, 0, Double.MAX_VALUE).boxed()).forEach(
                    builder::appendDouble
                );
                yield builder.build();
            }
            case BOOLEANS -> {
                BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(BLOCK_LENGTH);
                maybeSort(sortedInput, data, new Random().ints(BLOCK_LENGTH, 0, 1).boxed()).forEach(i -> builder.appendBoolean(i == 1));
                yield builder.build();
            }
            case BYTES_REFS -> {
                BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(BLOCK_LENGTH);
                maybeSort(sortedInput, data, new Random().ints(BLOCK_LENGTH, 0, Integer.MAX_VALUE).boxed()).forEach(
                    i -> builder.appendBytesRef(new BytesRef(i.toString()))
                );
                yield builder.build();
            }
            default -> throw new UnsupportedOperationException("unsupported data [" + data + "]");
        };
    }

    private static <T extends Comparable<T>> List<T> maybeSort(boolean sortedInput, String data, Stream<T> randomValues) {
        List<T> values = new ArrayList<>();
        randomValues.forEachOrdered(values::add);
        if (sortedInput) {
            values.sort(Comparator.naturalOrder());
            return ascDesc(data) ? values : values.reversed();
        }
        return values;
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void run() {
        run(data, topCount, sortedInput);
    }

    private static void run(String data, int topCount, boolean sortedInput) {
        try (Operator operator = operator(data, topCount, sortedInput)) {
            Page page = page(sortedInput, data);
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
