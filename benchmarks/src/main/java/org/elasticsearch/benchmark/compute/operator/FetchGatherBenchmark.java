/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.fetch.FetchGather;
import org.elasticsearch.core.Releasables;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Puts the fetched columns back next to the rows of the coordinator cut. The responses come back grouped by shard, so
 * every input row reads its values from somewhere else. {@code fetch_gather} is {@link FetchGather}.
 * {@code row_at_a_time} copies every value of every row on its own with a generic range copy, like the remote fetch
 * prototype does.
 */
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 7, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(1)
public class FetchGatherBenchmark {
    static {
        // BlockFactory needs logging before its class initializes
        BenchmarkLogging.configure();
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();

    private static final int FETCHED_COLUMNS = 3;

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            // both gathers must return the same values for every shape the benchmark measures
            selfTest();
        }
    }

    static void selfTest() {
        for (String type : new String[] { "long", "keyword" }) {
            for (int shards : new int[] { 1, 10 }) {
                FetchGatherBenchmark fetchGather = new FetchGatherBenchmark();
                FetchGatherBenchmark rowAtATime = new FetchGatherBenchmark();
                for (FetchGatherBenchmark benchmark : List.of(fetchGather, rowAtATime)) {
                    benchmark.rows = 100;
                    benchmark.shards = shards;
                    benchmark.type = type;
                    benchmark.setup();
                }
                fetchGather.gather = "fetch_gather";
                rowAtATime.gather = "row_at_a_time";
                List<Page> expected = rowAtATime.runGather();
                List<Page> actual = fetchGather.runGather();
                try {
                    if (expected.equals(actual) == false) {
                        throw new AssertionError("the gathers disagree for [" + type + "] over [" + shards + "] shards");
                    }
                } finally {
                    expected.forEach(Page::releaseBlocks);
                    actual.forEach(Page::releaseBlocks);
                    fetchGather.teardown();
                    rowAtATime.teardown();
                }
            }
        }
    }

    @Param({ "20", "500", "10000" })
    public int rows;

    /**
     * Response pages, one per shard the rows come from.
     */
    @Param({ "1", "10" })
    public int shards;

    @Param({ "long", "keyword" })
    public String type;

    @Param({ "fetch_gather", "row_at_a_time" })
    public String gather;

    private Block[] input;
    private Block[][] fetched;
    private List<ElementType> fetchedTypes;
    private int[] responseRows;

    @Setup
    public void setup() {
        Random random = new Random(0);
        ElementType elementType = switch (type) {
            case "long" -> ElementType.LONG;
            case "keyword" -> ElementType.BYTES_REF;
            default -> throw new IllegalArgumentException("unknown type [" + type + "]");
        };
        fetchedTypes = Collections.nCopies(FETCHED_COLUMNS, elementType);
        // the sort key the cut kept
        input = new Block[] { blockFactory.newLongArrayVector(random.longs(rows).toArray(), rows).asBlock() };
        fetched = new Block[shards][];
        int start = 0;
        for (int s = 0; s < shards; s++) {
            int end = rows * (s + 1) / shards;
            fetched[s] = new Block[FETCHED_COLUMNS];
            for (int c = 0; c < FETCHED_COLUMNS; c++) {
                fetched[s][c] = randomBlock(random, elementType, end - start);
            }
            start = end;
        }
        // every row of the cut reads a different response row
        List<Integer> order = new ArrayList<>(rows);
        for (int r = 0; r < rows; r++) {
            order.add(r);
        }
        Collections.shuffle(order, random);
        responseRows = order.stream().mapToInt(Integer::intValue).toArray();
    }

    private static Block randomBlock(Random random, ElementType elementType, int positions) {
        return switch (elementType) {
            case LONG -> {
                try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(positions)) {
                    for (int p = 0; p < positions; p++) {
                        builder.appendLong(random.nextLong());
                    }
                    yield builder.build();
                }
            }
            case BYTES_REF -> {
                try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positions)) {
                    for (int p = 0; p < positions; p++) {
                        // values like host names and log levels
                        byte[] bytes = new byte[10 + random.nextInt(30)];
                        for (int b = 0; b < bytes.length; b++) {
                            bytes[b] = (byte) ('a' + random.nextInt(26));
                        }
                        builder.appendBytesRef(new BytesRef(bytes));
                    }
                    yield builder.build();
                }
            }
            default -> throw new IllegalArgumentException("unsupported [" + elementType + "]");
        };
    }

    @TearDown
    public void teardown() {
        Releasables.closeExpectNoException(input);
        for (Block[] page : fetched) {
            Releasables.closeExpectNoException(page);
        }
    }

    @Benchmark
    public void run(Blackhole bh) {
        List<Page> output = runGather();
        bh.consume(output);
        output.forEach(Page::releaseBlocks);
    }

    private List<Page> runGather() {
        // both gathers release their input, so each run gets new pages over the same blocks
        Page inputPage = page(input);
        List<Page> fetchedPages = new ArrayList<>(fetched.length);
        for (Block[] page : fetched) {
            fetchedPages.add(page(page));
        }
        return switch (gather) {
            case "fetch_gather" -> FetchGather.gather(blockFactory, List.of(inputPage), fetchedPages, fetchedTypes, responseRows, false);
            case "row_at_a_time" -> List.of(rowAtATime(inputPage, fetchedPages));
            default -> throw new IllegalArgumentException("unknown gather [" + gather + "]");
        };
    }

    private static Page page(Block[] blocks) {
        for (Block block : blocks) {
            block.incRef();
        }
        return new Page(blocks);
    }

    private record FetchedRowRef(int page, int position) {}

    /**
     * The gather of the remote fetch prototype: a reference object for every row, then one single row copy per row and
     * column.
     */
    private Page rowAtATime(Page inputPage, List<Page> fetchedPages) {
        FetchedRowRef[] byResponseRow = new FetchedRowRef[rows];
        int responseRow = 0;
        for (int page = 0; page < fetchedPages.size(); page++) {
            for (int position = 0; position < fetchedPages.get(page).getPositionCount(); position++) {
                byResponseRow[responseRow++] = new FetchedRowRef(page, position);
            }
        }
        FetchedRowRef[] fetchedRows = new FetchedRowRef[responseRows.length];
        for (int row = 0; row < responseRows.length; row++) {
            fetchedRows[row] = byResponseRow[responseRows[row]];
        }
        Block[] outputBlocks = new Block[inputPage.getBlockCount() + fetchedTypes.size()];
        Block.Builder[] builders = new Block.Builder[fetchedTypes.size()];
        try {
            for (int b = 0; b < inputPage.getBlockCount(); b++) {
                outputBlocks[b] = inputPage.getBlock(b);
                outputBlocks[b].incRef();
            }
            for (int field = 0; field < fetchedTypes.size(); field++) {
                builders[field] = fetchedTypes.get(field).newBlockBuilder(inputPage.getPositionCount(), blockFactory);
                for (FetchedRowRef rowRef : fetchedRows) {
                    Block block = fetchedPages.get(rowRef.page()).getBlock(field);
                    builders[field].copyFrom(block, rowRef.position(), rowRef.position() + 1);
                }
                outputBlocks[inputPage.getBlockCount() + field] = builders[field].build();
            }
            return new Page(inputPage.getPositionCount(), outputBlocks);
        } finally {
            inputPage.releaseBlocks();
            fetchedPages.forEach(Page::releaseBlocks);
            Releasables.closeExpectNoException(builders);
        }
    }
}
