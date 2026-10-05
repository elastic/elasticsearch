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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Puts fetched columns next to the rows that asked for them. The coordinator cuts its rows, sends their documents to
 * the nodes that own them grouped by shard, and gets the values back in that other order. The gather turns every page
 * of the cut into the same page with the fetched columns appended, in the order of the cut.
 * <p>
 * Each fetched column is built in one of three ways:
 * <ul>
 *     <li>From a single response page, with one {@link Block#filter} per output page. Filtering keeps nulls,
 *     multivalues and the encoding of constants and vectors.</li>
 *     <li>Fixed width values from several response pages: one concatenation of the response pages, then one
 *     {@link Block#filter} per output page. That copies each value twice, and still runs faster than copying the
 *     values one at a time.</li>
 *     <li>{@link BytesRef} values from several response pages: each value is copied once, straight from its response
 *     page. A concatenation would copy every byte twice and hold both copies at once, and the wide columns a fetch
 *     defers are mostly {@link BytesRef}s.</li>
 * </ul>
 * {@code FetchGatherBenchmark} measures them against a gather that copies one row at a time.
 */
public final class FetchGather {
    private FetchGather() {}

    /**
     * Gathers the fetched columns of every input row. Releases the input and fetched pages, also when it fails.
     *
     * @param inputPages           the pages of the cut, in order
     * @param fetchedPages         the fetched columns, their rows in response order across the pages
     * @param fetchedTypes         the element type of each fetched column. A response block has this type, or is all
     *                             {@code null}
     * @param responseRows         for each row of the input pages, in order, the response row that holds its values, or
     *                             {@code -1} to drop the row, for example because the fetch of its shard failed
     * @param mayContainDuplicates whether two input rows can read the same response row
     * @return one page per input page, with the kept rows: the input columns followed by the fetched columns
     */
    public static List<Page> gather(
        BlockFactory blockFactory,
        List<Page> inputPages,
        List<Page> fetchedPages,
        List<ElementType> fetchedTypes,
        int[] responseRows,
        boolean mayContainDuplicates
    ) {
        ColumnGather[] columns = new ColumnGather[fetchedTypes.size()];
        List<Page> output = new ArrayList<>(inputPages.size());
        boolean success = false;
        try {
            int responseRowCount = checkResponseRows(inputPages, fetchedPages, fetchedTypes, responseRows);
            ResponseRows located = null;
            for (int c = 0; c < columns.length; c++) {
                ElementType type = fetchedTypes.get(c);
                columns[c] = switch (type) {
                    case BYTES_REF -> {
                        if (fetchedPages.size() == 1) {
                            yield new FilterGather(shared(fetchedPages.getFirst().getBlock(c)), mayContainDuplicates);
                        }
                        if (located == null) {
                            located = ResponseRows.locate(fetchedPages, responseRowCount);
                        }
                        yield new BytesRefCopyGather(blockFactory, fetchedPages, c, located);
                    }
                    case BOOLEAN, INT, LONG, FLOAT, DOUBLE -> new FilterGather(
                        concatenate(blockFactory, fetchedPages, c, type, responseRowCount, commonOrdering(fetchedPages, c)),
                        mayContainDuplicates
                    );
                    // values without a natural order have no sorted multivalues to keep
                    case NULL, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, LONG_RANGE, DOUBLE_RANGE -> new FilterGather(
                        concatenate(blockFactory, fetchedPages, c, type, responseRowCount, null),
                        mayContainDuplicates
                    );
                    case DOC, DOC_REF, COMPOSITE, UNKNOWN -> throw new IllegalArgumentException("[" + type + "] columns can't be fetched");
                };
            }
            int rowStart = 0;
            for (Page input : inputPages) {
                output.add(gatherPage(input, columns, responseRows, rowStart));
                rowStart += input.getPositionCount();
            }
            success = true;
            return output;
        } finally {
            Releasables.closeExpectNoException(columns);
            for (Page page : inputPages) {
                page.releaseBlocks();
            }
            for (Page page : fetchedPages) {
                page.releaseBlocks();
            }
            if (success == false) {
                for (Page page : output) {
                    page.releaseBlocks();
                }
            }
        }
    }

    private static int checkResponseRows(
        List<Page> inputPages,
        List<Page> fetchedPages,
        List<ElementType> fetchedTypes,
        int[] responseRows
    ) {
        int inputRowCount = 0;
        for (Page page : inputPages) {
            inputRowCount += page.getPositionCount();
        }
        if (responseRows.length != inputRowCount) {
            throw new IllegalArgumentException("[" + responseRows.length + "] response rows for [" + inputRowCount + "] input rows");
        }
        int responseRowCount = 0;
        for (Page page : fetchedPages) {
            if (page.getBlockCount() != fetchedTypes.size()) {
                throw new IllegalArgumentException(
                    "expected [" + fetchedTypes.size() + "] fetched columns but got [" + page.getBlockCount() + "]"
                );
            }
            for (int c = 0; c < fetchedTypes.size(); c++) {
                ElementType type = page.getBlock(c).elementType();
                // a shard without values for a column answers with a block of nulls
                if (type != fetchedTypes.get(c) && type != ElementType.NULL) {
                    throw new IllegalArgumentException(
                        "fetched column [" + c + "] should hold [" + fetchedTypes.get(c) + "] but holds [" + type + "]"
                    );
                }
            }
            responseRowCount += page.getPositionCount();
        }
        for (int row : responseRows) {
            if (row < -1 || row >= responseRowCount) {
                throw new IllegalArgumentException("response row [" + row + "] out of [-1, " + responseRowCount + ")");
            }
        }
        return responseRowCount;
    }

    private static Block shared(Block block) {
        block.incRef();
        return block;
    }

    /**
     * One column of every response page as one block. A single page needs no copy.
     *
     * @param ordering the ordering of the multivalues of every page, {@code null} for values without a natural order
     */
    private static Block concatenate(
        BlockFactory blockFactory,
        List<Page> pages,
        int column,
        ElementType type,
        int rows,
        @Nullable Block.MvOrdering ordering
    ) {
        if (pages.size() == 1) {
            return shared(pages.getFirst().getBlock(column));
        }
        try (Block.Builder builder = type.newBlockBuilder(rows, blockFactory)) {
            if (ordering != null) {
                // keeps the fast paths downstream operators take for sorted and deduplicated multivalues
                builder.mvOrdering(ordering);
            }
            for (Page page : pages) {
                builder.copyFrom(page.getBlock(column), 0, page.getPositionCount());
            }
            return builder.build();
        }
    }

    /**
     * The ordering every page of a column keeps. Pages without multivalues, like an all {@code null} one, keep every
     * ordering.
     */
    private static Block.MvOrdering commonOrdering(List<Page> pages, int column) {
        boolean deduplicated = true;
        boolean sortedAscending = true;
        for (Page page : pages) {
            Block block = page.getBlock(column);
            deduplicated &= block.mvDeduplicated();
            sortedAscending &= block.mvSortedAscending();
        }
        if (deduplicated) {
            return sortedAscending ? Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING : Block.MvOrdering.DEDUPLICATED_UNORDERED;
        }
        return sortedAscending ? Block.MvOrdering.SORTED_ASCENDING : Block.MvOrdering.UNORDERED;
    }

    private static Page gatherPage(Page input, ColumnGather[] columns, int[] responseRows, int rowStart) {
        int positions = input.getPositionCount();
        int[] keptPositions = new int[positions];
        int[] sources = new int[positions];
        int kept = 0;
        for (int p = 0; p < positions; p++) {
            int source = responseRows[rowStart + p];
            if (source >= 0) {
                keptPositions[kept] = p;
                sources[kept] = source;
                kept++;
            }
        }
        Block[] blocks = new Block[input.getBlockCount() + columns.length];
        boolean success = false;
        try {
            for (int b = 0; b < input.getBlockCount(); b++) {
                Block block = input.getBlock(b);
                blocks[b] = kept == positions ? shared(block) : block.filter(false, keptPositions, 0, kept);
            }
            for (int c = 0; c < columns.length; c++) {
                blocks[input.getBlockCount() + c] = columns[c].gather(sources, kept);
            }
            Page page = new Page(kept, blocks);
            success = true;
            return page;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    /**
     * Builds one fetched column of each output page.
     */
    private interface ColumnGather extends Releasable {
        /**
         * The column of the response rows {@code responseRows[0..count)}, in that order.
         */
        Block gather(int[] responseRows, int count);
    }

    /**
     * Filters a block that holds the column of every response row.
     */
    private record FilterGather(Block column, boolean mayContainDuplicates) implements ColumnGather {
        @Override
        public Block gather(int[] responseRows, int count) {
            return column.filter(mayContainDuplicates, responseRows, 0, count);
        }

        @Override
        public void close() {
            column.close();
        }
    }

    /**
     * The response page and the position in it of every response row.
     */
    private record ResponseRows(int[] pages, int[] positions) {
        static ResponseRows locate(List<Page> fetchedPages, int responseRowCount) {
            int[] pages = new int[responseRowCount];
            int[] positions = new int[responseRowCount];
            int row = 0;
            for (int page = 0; page < fetchedPages.size(); page++) {
                for (int p = 0; p < fetchedPages.get(page).getPositionCount(); p++) {
                    pages[row] = page;
                    positions[row] = p;
                    row++;
                }
            }
            return new ResponseRows(pages, positions);
        }
    }

    /**
     * Copies each value once, straight from the response page that holds it. The response pages stay with the caller.
     */
    private static final class BytesRefCopyGather implements ColumnGather {
        private final BlockFactory blockFactory;
        private final BytesRefBlock[] sources;
        private final ResponseRows located;
        private final Block.MvOrdering ordering;
        private final BytesRef scratch = new BytesRef();

        BytesRefCopyGather(BlockFactory blockFactory, List<Page> fetchedPages, int column, ResponseRows located) {
            this.blockFactory = blockFactory;
            this.sources = new BytesRefBlock[fetchedPages.size()];
            for (int page = 0; page < sources.length; page++) {
                // a block of nulls is a BytesRefBlock too
                sources[page] = fetchedPages.get(page).getBlock(column);
            }
            this.located = located;
            this.ordering = commonOrdering(fetchedPages, column);
        }

        @Override
        public Block gather(int[] responseRows, int count) {
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(count)) {
                builder.mvOrdering(ordering);
                for (int i = 0; i < count; i++) {
                    int row = responseRows[i];
                    builder.copyFrom(sources[located.pages()[row]], located.positions()[row], scratch);
                }
                return builder.build();
            }
        }

        @Override
        public void close() {}
    }
}
