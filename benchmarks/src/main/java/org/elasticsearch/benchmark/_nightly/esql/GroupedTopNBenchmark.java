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
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.GroupKeyEncoder;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.topn.GroupedTopNOperator;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.core.Releasables;
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
public class GroupedTopNBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final int BLOCK_LENGTH = 4 * 1024;
    private static final int NUM_PAGES = 1024;
    private static final int SELF_TEST_PAGES = 16;

    private static final String LONGS = "longs";
    private static final String INTS = "ints";
    private static final String DOUBLES = "doubles";
    private static final String BOOLEANS = "booleans";
    private static final String BYTES_REFS = "bytes_refs";

    private static final String ASC = "_asc";
    private static final String DESC = "_desc";

    private static final String AND = "_and_";

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        selfTest();
    }

    static void selfTest() {
        try {
            for (String data : GroupedTopNBenchmark.class.getField("data").getAnnotationsByType(Param.class)[0].value()) {
                for (String topCount : GroupedTopNBenchmark.class.getField("topCount").getAnnotationsByType(Param.class)[0].value()) {
                    for (String groupCount : GroupedTopNBenchmark.class.getField("groupCount").getAnnotationsByType(Param.class)[0]
                        .value()) {
                        for (String gk : GroupedTopNBenchmark.class.getField("groupKeys").getAnnotationsByType(Param.class)[0].value()) {
                            run(data, Integer.parseInt(topCount), Integer.parseInt(groupCount), gk, SELF_TEST_PAGES);
                        }
                    }
                }
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError();
        }
    }

    @Param({ LONGS + ASC, LONGS + DESC, BYTES_REFS + ASC, LONGS + ASC + AND + LONGS + ASC, LONGS + ASC + AND + BYTES_REFS + ASC })
    public String data;

    @Param({ "1", "10", "1000" })
    public int topCount;

    @Param({ "10", "100", "1000" })
    public int groupCount;

    @Param({ LONGS, BYTES_REFS, LONGS + AND + LONGS, BYTES_REFS + AND + BYTES_REFS, LONGS + AND + BYTES_REFS })
    public String groupKeys;

    private static Operator operator(String data, int topCount, String groupKeys) {
        String[] dataSpec = data.split(AND);
        List<ElementType> elementTypes = new ArrayList<>(Arrays.stream(dataSpec).map(GroupedTopNBenchmark::elementType).toList());
        List<TopNEncoder> encoders = new ArrayList<>(Arrays.stream(dataSpec).map(GroupedTopNBenchmark::encoder).toList());
        List<TopNOperator.SortOrder> sortOrders = IntStream.range(0, dataSpec.length).mapToObj(c -> sortOrder(c, dataSpec[c])).toList();

        String[] groupKeySpec = groupKeys.split(AND);
        int[] groupKeyChannels = new int[groupKeySpec.length];
        for (int i = 0; i < groupKeySpec.length; i++) {
            groupKeyChannels[i] = elementTypes.size();
            elementTypes.add(elementType(groupKeySpec[i]));
            encoders.add(TopNEncoder.DEFAULT_UNSORTABLE);
        }

        return new GroupedTopNOperator(
            blockFactory,
            blockFactory.breaker(),
            topCount,
            elementTypes,
            encoders,
            sortOrders,
            new GroupKeyEncoder(groupKeyChannels, elementTypes, new BreakingBytesRefBuilder(blockFactory.breaker(), "group-key-encoder")),
            8 * 1024,
            Long.MAX_VALUE
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

    private static void checkExpected(int topCount, int groupCount, int numPages, List<Page> pages) {
        int effectiveGroupCount = Math.min(groupCount, BLOCK_LENGTH);
        long expectedOutput = 0;
        for (int g = 0; g < effectiveGroupCount; g++) {
            int rowsPerPage = BLOCK_LENGTH / effectiveGroupCount + (g < BLOCK_LENGTH % effectiveGroupCount ? 1 : 0);
            long totalRowsForGroup = (long) rowsPerPage * numPages;
            expectedOutput += Math.min(topCount, totalRowsForGroup);
        }
        long actualOutput = pages.stream().mapToLong(Page::getPositionCount).sum();
        if (expectedOutput != actualOutput) {
            throw new AssertionError("expected [" + expectedOutput + "] but got [" + actualOutput + "]");
        }
    }

    private static Page page(String data, int groupCount, String groupKeys) {
        String[] dataSpec = data.split(AND);
        String[] groupKeySpec = groupKeys.split(AND);
        int effectiveGroupCount = Math.min(groupCount, BLOCK_LENGTH);
        int divisor = (int) Math.ceil(Math.sqrt(effectiveGroupCount));

        Block[] blocks = new Block[dataSpec.length + groupKeySpec.length];
        for (int i = 0; i < dataSpec.length; i++) {
            blocks[i] = block(dataSpec[i]);
        }
        for (int k = 0; k < groupKeySpec.length; k++) {
            blocks[dataSpec.length + k] = groupKeyBlock(groupKeySpec[k], effectiveGroupCount, divisor, k, groupKeySpec.length);
        }
        return new Page(blocks);
    }

    private static Block block(String data) {
        return switch (data.replace(ASC, "").replace(DESC, "")) {
            case LONGS -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                new Random().longs(BLOCK_LENGTH, 0, Long.MAX_VALUE).forEach(builder::appendLong);
                yield builder.build();
            }
            case INTS -> {
                var builder = blockFactory.newIntBlockBuilder(BLOCK_LENGTH);
                new Random().ints(BLOCK_LENGTH, 0, Integer.MAX_VALUE).forEach(builder::appendInt);
                yield builder.build();
            }
            case DOUBLES -> {
                var builder = blockFactory.newDoubleBlockBuilder(BLOCK_LENGTH);
                new Random().doubles(BLOCK_LENGTH, 0, Double.MAX_VALUE).forEach(builder::appendDouble);
                yield builder.build();
            }
            case BOOLEANS -> {
                BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(BLOCK_LENGTH);
                new Random().ints(BLOCK_LENGTH, 0, 2).forEach(i -> builder.appendBoolean(i == 1));
                yield builder.build();
            }
            case BYTES_REFS -> {
                BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(BLOCK_LENGTH);
                new Random().ints(BLOCK_LENGTH, 0, Integer.MAX_VALUE)
                    .forEach(i -> builder.appendBytesRef(new BytesRef(Integer.toString(i))));
                yield builder.build();
            }
            default -> throw new UnsupportedOperationException("unsupported data [" + data + "]");
        };
    }

    private static Block groupKeyBlock(String groupKeyType, int effectiveGroupCount, int divisor, int keyIndex, int groupKeyCount) {
        return switch (groupKeyType) {
            case LONGS -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    int groupId = i % effectiveGroupCount;
                    long keyValue = groupKeyCount == 1 ? groupId : (keyIndex == 0 ? groupId / divisor : groupId % divisor);
                    builder.appendLong(keyValue);
                }
                yield builder.build();
            }
            case BYTES_REFS -> {
                BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    int groupId = i % effectiveGroupCount;
                    long keyValue = groupKeyCount == 1 ? groupId : (keyIndex == 0 ? groupId / divisor : groupId % divisor);
                    builder.appendBytesRef(new BytesRef(Long.toString(keyValue)));
                }
                yield builder.build();
            }
            default -> throw new IllegalArgumentException("unsupported group key type [" + groupKeyType + "]");
        };
    }

    // @Benchmark - disabled temporarily, too slow (225 parameter combinations)
    @OperationsPerInvocation(NUM_PAGES * BLOCK_LENGTH)
    public void run() {
        run(data, topCount, groupCount, groupKeys, NUM_PAGES);
    }

    private static void run(String data, int topCount, int groupCount, String groupKeys, int numPages) {
        try (Operator operator = operator(data, topCount, groupKeys)) {
            Page page = page(data, groupCount, groupKeys);
            for (int i = 0; i < numPages; i++) {
                operator.addInput(page.shallowCopy());
            }
            operator.finish();
            List<Page> results = new ArrayList<>();
            try {
                Page p;
                while ((p = operator.getOutput()) != null) {
                    results.add(p);
                }
                checkExpected(topCount, groupCount, numPages, results);
            } finally {
                Releasables.close(results);
            }
        }
    }
}
