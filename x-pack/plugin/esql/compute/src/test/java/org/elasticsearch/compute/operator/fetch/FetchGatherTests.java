/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fetch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class FetchGatherTests extends ComputeTestCase {
    /**
     * Random input pages, random fetched columns with nulls and multivalues split over random response pages, and a
     * random row map that may drop rows and read a response row twice. Some response pages hold only nulls for a
     * column, like the answer of a shard without values for it. The result must match a row by row gather.
     */
    public void testMatchesRowByRowGather() {
        BlockFactory blockFactory = blockFactory();
        List<ElementType> fetchedTypes = randomList(1, 3, RandomBlock::randomElementType);
        int responseRowCount = between(0, 60);
        boolean mayContainDuplicates = randomBoolean();

        // the fetched columns, in response order, split over response pages
        List<List<List<Object>>> fetchedValues = new ArrayList<>();
        for (int c = 0; c < fetchedTypes.size(); c++) {
            fetchedValues.add(new ArrayList<>());
        }
        List<Page> fetchedPages = new ArrayList<>();
        int written = 0;
        while (written < responseRowCount) {
            int rows = Math.min(between(1, 25), responseRowCount - written);
            Block[] blocks = new Block[fetchedTypes.size()];
            for (int c = 0; c < blocks.length; c++) {
                if (randomInt(9) == 0) {
                    blocks[c] = blockFactory.newConstantNullBlock(rows);
                    fetchedValues.get(c).addAll(Collections.nCopies(rows, null));
                    continue;
                }
                RandomBlock random = RandomBlock.randomBlock(
                    blockFactory,
                    fetchedTypes.get(c),
                    rows,
                    true,
                    1,
                    maxValues(fetchedTypes.get(c)),
                    0,
                    0
                );
                blocks[c] = random.block();
                fetchedValues.get(c).addAll(random.values());
            }
            fetchedPages.add(new Page(rows, blocks));
            written += rows;
        }

        // input pages with a column that numbers their rows, and the response row each row reads
        List<Page> inputPages = new ArrayList<>();
        List<Integer> responseRows = new ArrayList<>();
        int inputRow = 0;
        for (int i = between(0, 5); i > 0; i--) {
            int rows = between(0, 20);
            try (IntBlock.Builder numbers = blockFactory.newIntBlockBuilder(rows)) {
                for (int p = 0; p < rows; p++) {
                    numbers.appendInt(inputRow++);
                    boolean drop = responseRowCount == 0 || randomInt(4) == 0;
                    responseRows.add(drop ? -1 : between(0, responseRowCount - 1));
                }
                inputPages.add(new Page(rows, numbers.build()));
            }
        }
        if (mayContainDuplicates == false) {
            // without duplicates two rows never read the same response row
            List<Integer> seen = new ArrayList<>();
            for (int r = 0; r < responseRows.size(); r++) {
                if (seen.contains(responseRows.get(r))) {
                    responseRows.set(r, -1);
                } else if (responseRows.get(r) >= 0) {
                    seen.add(responseRows.get(r));
                }
            }
        }
        int[] rowMap = responseRows.stream().mapToInt(Integer::intValue).toArray();
        int inputPageCount = inputPages.size();

        List<Page> output = FetchGather.gather(blockFactory, inputPages, fetchedPages, fetchedTypes, rowMap, mayContainDuplicates);
        try {
            assertThat("one output page per input page", output.size(), equalTo(inputPageCount));
            List<List<Object>> expected = new ArrayList<>();
            for (int r = 0; r < rowMap.length; r++) {
                if (rowMap[r] < 0) {
                    continue;
                }
                List<Object> row = new ArrayList<>();
                row.add(r);
                for (int c = 0; c < fetchedTypes.size(); c++) {
                    row.add(fetchedValues.get(c).get(rowMap[r]));
                }
                expected.add(row);
            }
            List<List<Object>> actual = new ArrayList<>();
            for (Page page : output) {
                assertThat(page.getBlockCount(), equalTo(1 + fetchedTypes.size()));
                for (int p = 0; p < page.getPositionCount(); p++) {
                    List<Object> row = new ArrayList<>();
                    row.add(((IntBlock) page.getBlock(0)).getInt(p));
                    for (int c = 0; c < fetchedTypes.size(); c++) {
                        row.add(valuesAtPositions(page.getBlock(1 + c), p, p + 1).getFirst());
                    }
                    actual.add(row);
                }
            }
            assertThat(actual, equalTo(expected));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    /**
     * The most values {@link RandomBlock} puts in one position. It can't build multivalues of composite types.
     */
    private static int maxValues(ElementType type) {
        return switch (type) {
            case BOOLEAN, INT, LONG, FLOAT, DOUBLE, BYTES_REF -> 3;
            case AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, LONG_RANGE, DOUBLE_RANGE -> 1;
            case NULL, DOC, DOC_REF, COMPOSITE, UNKNOWN -> throw new AssertionError("random blocks are never [" + type + "]");
        };
    }

    /**
     * When no row is dropped the input columns are shared, not copied.
     */
    public void testKeepsInputColumnsWhenNothingIsDropped() {
        BlockFactory blockFactory = blockFactory();
        IntBlock numbers = blockFactory.newIntArrayVector(new int[] { 7, 8, 9 }, 3).asBlock();
        Page fetched = new Page(blockFactory.newLongArrayVector(new long[] { 30, 10, 20 }, 3).asBlock());
        List<Page> output = FetchGather.gather(
            blockFactory,
            List.of(new Page(numbers)),
            List.of(fetched),
            List.of(ElementType.LONG),
            new int[] { 1, 2, 0 },
            false
        );
        try {
            Page page = output.getFirst();
            assertThat(page.getBlock(0), sameInstance(numbers));
            LongBlock gathered = page.getBlock(1);
            assertThat(gathered.getLong(0), equalTo(10L));
            assertThat(gathered.getLong(1), equalTo(20L));
            assertThat(gathered.getLong(2), equalTo(30L));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    /**
     * The input of a real fetch carries its document references. They are shared when every row stays, and filtered
     * with the other input columns when rows are dropped.
     */
    public void testCarriesDocumentReferences() {
        BlockFactory blockFactory = blockFactory();
        DocRefBlock docRefs = (DocRefBlock) RandomBlock.randomDocRefBlock(blockFactory, 3, between(1, 3)).block();
        DocRefBlock expectedKept;
        try (DocRefBlock.Builder builder = DocRefBlock.newBlockBuilder(blockFactory, 2)) {
            builder.copyFrom(docRefs, 0, 1);
            builder.copyFrom(docRefs, 2, 3);
            expectedKept = builder.build();
        }
        boolean dropOne = randomBoolean();
        Page fetched = new Page(blockFactory.newLongArrayVector(new long[] { 1, 2, 3 }, 3).asBlock());
        docRefs.incRef();
        List<Page> output = FetchGather.gather(
            blockFactory,
            List.of(new Page(docRefs)),
            List.of(fetched),
            List.of(ElementType.LONG),
            dropOne ? new int[] { 0, -1, 2 } : new int[] { 0, 1, 2 },
            false
        );
        try {
            Block gathered = output.getFirst().getBlock(0);
            if (dropOne) {
                assertThat(gathered, equalTo(expectedKept));
            } else {
                assertThat(gathered, sameInstance(docRefs));
            }
        } finally {
            output.forEach(Page::releaseBlocks);
            docRefs.decRef();
            expectedKept.close();
        }
    }

    /**
     * Downstream operators have fast paths for sorted, deduplicated multivalues. Gathering from several response pages
     * keeps the ordering when every page keeps it. A page without multivalues, like one with only nulls, keeps every
     * ordering. Fixed width values and {@link BytesRef}s take different paths, so both are checked.
     */
    public void testKeepsMultivalueOrdering() {
        BlockFactory blockFactory = blockFactory();
        for (ElementType type : List.of(ElementType.LONG, ElementType.BYTES_REF)) {
            List<Page> fetchedPages = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                fetchedPages.add(new Page(sortedMultivalue(blockFactory, type)));
            }
            fetchedPages.add(new Page(blockFactory.newConstantNullBlock(1)));
            Page input = new Page(blockFactory.newConstantIntBlockWith(0, 3));
            List<Page> output = FetchGather.gather(blockFactory, List.of(input), fetchedPages, List.of(type), new int[] { 2, 1, 0 }, false);
            try {
                assertThat(
                    type.toString(),
                    output.getFirst().getBlock(1).mvOrdering(),
                    equalTo(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING)
                );
            } finally {
                output.forEach(Page::releaseBlocks);
            }
        }
    }

    private static Block sortedMultivalue(BlockFactory blockFactory, ElementType type) {
        return switch (type) {
            case LONG -> {
                try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(1)) {
                    builder.mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
                    builder.beginPositionEntry().appendLong(1).appendLong(2).endPositionEntry();
                    yield builder.build();
                }
            }
            case BYTES_REF -> {
                try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(1)) {
                    builder.mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
                    builder.beginPositionEntry().appendBytesRef(new BytesRef("a")).appendBytesRef(new BytesRef("b")).endPositionEntry();
                    yield builder.build();
                }
            }
            default -> throw new AssertionError("only longs and bytes are checked but got [" + type + "]");
        };
    }

    public void testEmptyPagesKeepTheFetchedColumns() {
        BlockFactory blockFactory = blockFactory();
        Page input = new Page(0, blockFactory.newConstantIntBlockWith(0, 0));
        List<Page> output = FetchGather.gather(
            blockFactory,
            List.of(input),
            List.of(),
            List.of(ElementType.BYTES_REF, ElementType.LONG),
            new int[0],
            false
        );
        try {
            Page page = output.getFirst();
            assertThat(page.getPositionCount(), equalTo(0));
            assertThat(page.getBlockCount(), equalTo(3));
            assertThat(page.getBlock(1).elementType(), equalTo(ElementType.BYTES_REF));
            assertThat(page.getBlock(2).elementType(), equalTo(ElementType.LONG));
        } finally {
            output.forEach(Page::releaseBlocks);
        }
    }

    /**
     * A circuit breaker can trip while the gather builds a column. Every page it was given and every page it built
     * must be released.
     */
    public void testReleasesEverythingWhenTheBreakerTrips() {
        BlockFactory blockFactory = blockFactory();
        testWithCrankyBlockFactory(cranky -> {
            List<Page> inputPages = new ArrayList<>();
            List<Integer> responseRows = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                inputPages.add(new Page(blockFactory.newConstantIntBlockWith(i, 10)));
                for (int p = 0; p < 10; p++) {
                    responseRows.add(i * 10 + p);
                }
            }
            Collections.shuffle(responseRows, random());
            List<Page> fetchedPages = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                fetchedPages.add(
                    new Page(
                        RandomBlock.randomBlock(blockFactory, ElementType.BYTES_REF, 10, true, 1, 3, 0, 0).block(),
                        RandomBlock.randomBlock(blockFactory, ElementType.LONG, 10, true, 1, 3, 0, 0).block()
                    )
                );
            }
            List<Page> output = FetchGather.gather(
                cranky,
                inputPages,
                fetchedPages,
                List.of(ElementType.BYTES_REF, ElementType.LONG),
                responseRows.stream().mapToInt(Integer::intValue).toArray(),
                false
            );
            output.forEach(Page::releaseBlocks);
        });
    }

    public void testRejectsAMapOfTheWrongLength() {
        BlockFactory blockFactory = blockFactory();
        Page input = new Page(blockFactory.newConstantIntBlockWith(0, 2));
        Page fetched = new Page(blockFactory.newConstantLongBlockWith(1, 2));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FetchGather.gather(blockFactory, List.of(input), List.of(fetched), List.of(ElementType.LONG), new int[] { 0 }, false)
        );
        assertThat(e.getMessage(), containsString("[1] response rows for [2] input rows"));
    }

    public void testRejectsResponseRowsOutOfRange() {
        BlockFactory blockFactory = blockFactory();
        Page input = new Page(blockFactory.newConstantIntBlockWith(0, 2));
        Page fetched = new Page(blockFactory.newConstantLongBlockWith(1, 2));
        int[] rowMap = randomBoolean() ? new int[] { 0, 2 } : new int[] { -2, 0 };
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FetchGather.gather(blockFactory, List.of(input), List.of(fetched), List.of(ElementType.LONG), rowMap, false)
        );
        assertThat(e.getMessage(), containsString("out of [-1, 2)"));
        assertThat(Arrays.toString(rowMap), e.getMessage(), containsString("response row"));
    }

    /**
     * A response column of another type than the planner expects would fail later with a cast, or pass unnoticed from
     * a single page. The gather refuses it up front.
     */
    public void testRejectsColumnsOfAnotherType() {
        BlockFactory blockFactory = blockFactory();
        Page input = new Page(blockFactory.newConstantIntBlockWith(0, 2));
        Page fetched = new Page(blockFactory.newConstantLongBlockWith(1, 2));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FetchGather.gather(
                blockFactory,
                List.of(input),
                List.of(fetched),
                List.of(ElementType.BYTES_REF),
                new int[] { 0, 1 },
                false
            )
        );
        assertThat(e.getMessage(), equalTo("fetched column [0] should hold [BYTES_REF] but holds [LONG]"));
    }

    public void testRejectsColumnsThatCantBeFetched() {
        BlockFactory blockFactory = blockFactory();
        Page input = new Page(blockFactory.newConstantIntBlockWith(0, 1));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FetchGather.gather(blockFactory, List.of(input), List.of(), List.of(ElementType.DOC_REF), new int[] { -1 }, false)
        );
        assertThat(e.getMessage(), equalTo("[DOC_REF] columns can't be fetched"));
    }
}
