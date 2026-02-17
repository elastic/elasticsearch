/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Performs an in-memory hash join (LEFT JOIN semantics) using {@link BlockHash}.
 * <p>
 * The entire right-side dataset is loaded into memory and indexed by one or more
 * join key values using a memory-tracked {@link BlockHash}. Left-side pages are
 * then probed against the hash table in bulk via {@link BlockHash#lookup}. Each
 * left row is output at least once: matched rows are joined with the corresponding
 * right columns, and unmatched rows are padded with nulls.
 * </p>
 * <p>
 * For composite keys (multiple join columns), the {@link BlockHash} naturally
 * handles the AND semantics: a match requires all key columns to match simultaneously.
 * </p>
 * <p>
 * Right-side pages are expected to have a positions column at index 0 (which is ignored)
 * followed by data columns. The join keys are data columns identified by
 * {@code joinKeyColumnsInRight}.
 * </p>
 */
public class HashJoin implements Releasable {
    static final String MULTI_VALUE_WARNING_MESSAGE = "LOOKUP JOIN encountered multi-value";

    private final List<Page> rightPages;
    private final int[] joinKeyColumnsInRight;
    private final int rightDataColumnCount;
    private final BlockFactory blockFactory;
    private final Warnings warnings;
    private final BlockHash hash;
    private final ElementType[] keyElementTypes;
    /**
     * Maps group IDs (assigned by {@link BlockHash}) to the list of right-side rows
     * that share that composite key. Group ID 0 is reserved for null keys by BlockHash,
     * so index 0 in this list is always empty.
     */
    private final List<List<RowRef>> groupIdToRows;

    record RowRef(int pageIndex, int rowPosition) {}

    /**
     * @param rightPages              pages from the server with positions at column 0, data at columns 1+
     * @param joinKeyColumnsInRight   column indices of the join keys in the right pages
     *                                (typically 1+ since column 0 is the positions column)
     * @param rightOutputColumnCount  number of right-side columns to include in the output
     *                                (= addedFields.size()). The last {@code rightOutputColumnCount}
     *                                columns from each right page are output, matching the layout
     *                                built by the planner.
     * @param blockFactory            block factory for memory-tracked hash table and output blocks
     * @param warnings                warnings collector for multi-value key handling
     */
    public HashJoin(
        List<Page> rightPages,
        int[] joinKeyColumnsInRight,
        int rightOutputColumnCount,
        BlockFactory blockFactory,
        Warnings warnings
    ) {
        this.rightPages = new ArrayList<>(rightPages);
        this.joinKeyColumnsInRight = joinKeyColumnsInRight;
        this.rightDataColumnCount = rightOutputColumnCount;
        this.blockFactory = blockFactory;
        this.warnings = warnings;
        this.keyElementTypes = determineKeyElementTypes();
        // groupId 0 is reserved for null keys
        this.groupIdToRows = new ArrayList<>();
        this.groupIdToRows.add(new ArrayList<>());
        this.hash = buildHashTable();
    }

    private BlockHash buildHashTable() {
        List<BlockHash.GroupSpec> groupSpecs = new ArrayList<>(keyElementTypes.length);
        for (int k = 0; k < keyElementTypes.length; k++) {
            groupSpecs.add(new BlockHash.GroupSpec(k, keyElementTypes[k]));
        }
        BlockHash blockHash = BlockHash.build(
            groupSpecs,
            blockFactory,
            (int) BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE.getBytes(),
            false
        );
        boolean success = false;
        try {
            for (int pageIdx = 0; pageIdx < rightPages.size(); pageIdx++) {
                Page page = rightPages.get(pageIdx);
                // Build a multi-column page with one block per join key
                Block[] keyBlocks = new Block[joinKeyColumnsInRight.length];
                for (int k = 0; k < joinKeyColumnsInRight.length; k++) {
                    keyBlocks[k] = page.getBlock(joinKeyColumnsInRight[k]);
                }
                Page keyPage = new Page(keyBlocks);
                final int currentPageIdx = pageIdx;
                blockHash.add(keyPage, new GroupingAggregatorFunction.AddInput() {
                    @Override
                    public void add(int positionOffset, IntArrayBlock groupIds) {
                        recordGroupIds(positionOffset, groupIds, currentPageIdx);
                    }

                    @Override
                    public void add(int positionOffset, IntBigArrayBlock groupIds) {
                        recordGroupIds(positionOffset, groupIds, currentPageIdx);
                    }

                    @Override
                    public void add(int positionOffset, IntVector groupIds) {
                        recordGroupIds(positionOffset, groupIds, currentPageIdx);
                    }

                    @Override
                    public void close() {}
                });
            }
            success = true;
            return blockHash;
        } finally {
            if (success == false) {
                blockHash.close();
            }
        }
    }

    private void recordGroupIds(int positionOffset, IntBlock groupIds, int pageIdx) {
        for (int p = 0; p < groupIds.getPositionCount(); p++) {
            int row = positionOffset + p;
            int valueCount = groupIds.getValueCount(p);
            int firstValueIndex = groupIds.getFirstValueIndex(p);
            for (int v = 0; v < valueCount; v++) {
                int groupId = groupIds.getInt(firstValueIndex + v);
                ensureGroupCapacity(groupId);
                groupIdToRows.get(groupId).add(new RowRef(pageIdx, row));
            }
        }
    }

    private void recordGroupIds(int positionOffset, IntVector groupIds, int pageIdx) {
        for (int p = 0; p < groupIds.getPositionCount(); p++) {
            int row = positionOffset + p;
            int groupId = groupIds.getInt(p);
            ensureGroupCapacity(groupId);
            groupIdToRows.get(groupId).add(new RowRef(pageIdx, row));
        }
    }

    private void ensureGroupCapacity(int groupId) {
        while (groupIdToRows.size() <= groupId) {
            groupIdToRows.add(new ArrayList<>());
        }
    }

    /**
     * Determines the element types of the join keys from the right pages.
     * Falls back to LONG for any key whose type cannot be determined (e.g., empty right side).
     */
    private ElementType[] determineKeyElementTypes() {
        ElementType[] types = new ElementType[joinKeyColumnsInRight.length];
        for (int k = 0; k < joinKeyColumnsInRight.length; k++) {
            types[k] = ElementType.LONG; // default
            for (Page page : rightPages) {
                if (page.getBlockCount() > joinKeyColumnsInRight[k]) {
                    types[k] = page.getBlock(joinKeyColumnsInRight[k]).elementType();
                    break;
                }
            }
        }
        return types;
    }

    public static final int DEFAULT_MAX_OUTPUT_PAGE_SIZE = 8192;

    /**
     * Join a left page against the hash table, returning an iterator that yields output pages
     * of bounded size. This avoids materializing the entire expanded result when fan-out is high
     * (e.g., many right rows match a single left key).
     *
     * @param leftPage              the left-side page
     * @param leftJoinKeyChannels   column indices of the join keys in the left page
     * @return an iterator of pages, each containing left columns followed by right data columns
     */
    public ReleasableIterator<Page> join(Page leftPage, int[] leftJoinKeyChannels) {
        return join(leftPage, leftJoinKeyChannels, DEFAULT_MAX_OUTPUT_PAGE_SIZE);
    }

    ReleasableIterator<Page> join(Page leftPage, int[] leftJoinKeyChannels, int maxOutputPageSize) {
        for (int k = 0; k < leftJoinKeyChannels.length; k++) {
            Block leftKeyBlock = leftPage.getBlock(leftJoinKeyChannels[k]);
            if (leftKeyBlock.elementType() != keyElementTypes[k]) {
                return ReleasableIterator.single(buildNoMatchOutput(leftPage));
            }
        }

        Block[] leftKeyBlocks = new Block[leftJoinKeyChannels.length];
        for (int k = 0; k < leftJoinKeyChannels.length; k++) {
            leftKeyBlocks[k] = leftPage.getBlock(leftJoinKeyChannels[k]);
        }
        IntBlock lookupResult;
        Page keyPage = new Page(leftKeyBlocks);
        try (ReleasableIterator<IntBlock> lookupIterator = hash.lookup(keyPage, BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE)) {
            lookupResult = collectLookupResults(lookupIterator, leftPage.getPositionCount());
        }

        return new JoinIterator(leftPage, leftJoinKeyChannels, lookupResult, maxOutputPageSize);
    }

    /**
     * Iterator that lazily walks left page positions and yields bounded-size output pages.
     * Each call to {@link #next()} processes left rows until the output page reaches
     * {@code maxOutputPageSize} rows, then builds and returns that page.
     */
    private class JoinIterator implements ReleasableIterator<Page> {
        private final Page leftPage;
        private final int[] leftJoinKeyChannels;
        private final IntBlock lookupResult;
        private final int maxOutputPageSize;
        private int leftPos;
        private int matchIdx;
        private boolean done;

        JoinIterator(Page leftPage, int[] leftJoinKeyChannels, IntBlock lookupResult, int maxOutputPageSize) {
            this.leftPage = leftPage;
            this.leftJoinKeyChannels = leftJoinKeyChannels;
            this.lookupResult = lookupResult;
            this.maxOutputPageSize = maxOutputPageSize;
            this.leftPos = 0;
            this.matchIdx = 0;
            this.done = false;
        }

        @Override
        public boolean hasNext() {
            return done == false;
        }

        @Override
        public Page next() {
            int[] leftPositions = new int[maxOutputPageSize];
            RowRef[] rightRefs = new RowRef[maxOutputPageSize];
            int outputCount = 0;

            while (leftPos < leftPage.getPositionCount() && outputCount < maxOutputPageSize) {
                if (matchIdx == 0) {
                    boolean leftMv = false;
                    for (int k = 0; k < leftJoinKeyChannels.length; k++) {
                        Block leftKeyBlock = leftPage.getBlock(leftJoinKeyChannels[k]);
                        if (leftKeyBlock.isNull(leftPos) == false && leftKeyBlock.getValueCount(leftPos) > 1) {
                            leftMv = true;
                            break;
                        }
                    }
                    if (leftMv) {
                        warnings.registerException(IllegalArgumentException.class, MULTI_VALUE_WARNING_MESSAGE);
                        outputCount = appendRow(leftPositions, rightRefs, outputCount, leftPos, null);
                        leftPos++;
                        continue;
                    }

                    if (lookupResult.isNull(leftPos)) {
                        outputCount = appendRow(leftPositions, rightRefs, outputCount, leftPos, null);
                        leftPos++;
                        continue;
                    }
                }

                int groupId = lookupResult.getInt(lookupResult.getFirstValueIndex(leftPos));
                List<RowRef> matches = groupId >= 0 && groupId < groupIdToRows.size() ? groupIdToRows.get(groupId) : null;
                if (matches == null || matches.isEmpty()) {
                    outputCount = appendRow(leftPositions, rightRefs, outputCount, leftPos, null);
                    leftPos++;
                    matchIdx = 0;
                    continue;
                }

                boolean emittedAny = matchIdx > 0;
                while (matchIdx < matches.size() && outputCount < maxOutputPageSize) {
                    RowRef ref = matches.get(matchIdx);
                    boolean rightMv = false;
                    for (int k = 0; k < joinKeyColumnsInRight.length; k++) {
                        Block rightKeyBlock = rightPages.get(ref.pageIndex()).getBlock(joinKeyColumnsInRight[k]);
                        if (rightKeyBlock.getValueCount(ref.rowPosition()) != 1) {
                            rightMv = true;
                            break;
                        }
                    }
                    if (rightMv) {
                        warnings.registerException(IllegalArgumentException.class, MULTI_VALUE_WARNING_MESSAGE);
                    } else {
                        outputCount = appendRow(leftPositions, rightRefs, outputCount, leftPos, ref);
                        emittedAny = true;
                    }
                    matchIdx++;
                }

                if (matchIdx >= matches.size()) {
                    if (emittedAny == false) {
                        outputCount = appendRow(leftPositions, rightRefs, outputCount, leftPos, null);
                    }
                    leftPos++;
                    matchIdx = 0;
                }
            }

            if (leftPos >= leftPage.getPositionCount() && matchIdx == 0) {
                done = true;
            }

            return buildOutputPage(leftPage, leftPositions, rightRefs, outputCount);
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(lookupResult, leftPage::releaseBlocks);
        }
    }

    private static int appendRow(int[] leftPositions, RowRef[] rightRefs, int idx, int leftPos, RowRef ref) {
        leftPositions[idx] = leftPos;
        rightRefs[idx] = ref;
        return idx + 1;
    }

    private Page buildOutputPage(Page leftPage, int[] leftPositions, RowRef[] rightRefs, int outputSize) {
        Block[] outputBlocks = new Block[leftPage.getBlockCount() + rightDataColumnCount];
        boolean success = false;
        try {
            int[] leftFilter = outputSize == leftPositions.length ? leftPositions : Arrays.copyOf(leftPositions, outputSize);
            for (int c = 0; c < leftPage.getBlockCount(); c++) {
                outputBlocks[c] = leftPage.getBlock(c).filter(true, leftFilter);
            }

            if (rightPages.isEmpty()) {
                for (int col = 0; col < rightDataColumnCount; col++) {
                    outputBlocks[leftPage.getBlockCount() + col] = blockFactory.newConstantNullBlock(outputSize);
                }
            } else {
                int totalRightCols = rightPages.get(0).getBlockCount();
                int firstOutputCol = totalRightCols - rightDataColumnCount;
                int outputIdx = 0;
                for (int rightCol = firstOutputCol; rightCol < totalRightCols; rightCol++) {
                    ElementType elementType = determineRightColumnType(rightCol);

                    try (Block.Builder builder = elementType.newBlockBuilder(outputSize, blockFactory)) {
                        for (int i = 0; i < outputSize; i++) {
                            RowRef ref = rightRefs[i];
                            if (ref == null) {
                                builder.appendNull();
                            } else {
                                Block srcBlock = rightPages.get(ref.pageIndex()).getBlock(rightCol);
                                builder.copyFrom(srcBlock, ref.rowPosition(), ref.rowPosition() + 1);
                            }
                        }
                        outputBlocks[leftPage.getBlockCount() + outputIdx] = builder.build();
                    }
                    outputIdx++;
                }
            }

            success = true;
            return new Page(outputBlocks);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(outputBlocks);
            }
        }
    }

    /**
     * Collects all IntBlock chunks from the lookup iterator into a single IntBlock.
     */
    private IntBlock collectLookupResults(ReleasableIterator<IntBlock> iterator, int expectedPositions) {
        IntBlock first = iterator.next();
        if (iterator.hasNext() == false && first.getPositionCount() == expectedPositions) {
            return first;
        }
        // Multiple chunks — merge them
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(expectedPositions)) {
            copyIntoBuilder(first, builder);
            first.close();
            while (iterator.hasNext()) {
                try (IntBlock chunk = iterator.next()) {
                    copyIntoBuilder(chunk, builder);
                }
            }
            return builder.build();
        }
    }

    private static void copyIntoBuilder(IntBlock source, IntBlock.Builder builder) {
        for (int p = 0; p < source.getPositionCount(); p++) {
            if (source.isNull(p)) {
                builder.appendNull();
            } else {
                int valueCount = source.getValueCount(p);
                int firstValueIndex = source.getFirstValueIndex(p);
                if (valueCount == 1) {
                    builder.appendInt(source.getInt(firstValueIndex));
                } else {
                    builder.beginPositionEntry();
                    for (int v = 0; v < valueCount; v++) {
                        builder.appendInt(source.getInt(firstValueIndex + v));
                    }
                    builder.endPositionEntry();
                }
            }
        }
    }

    /**
     * Builds an output page where every left row is paired with null right columns.
     * Used when key types are incompatible and no matches are possible.
     */
    private Page buildNoMatchOutput(Page leftPage) {
        int outputSize = leftPage.getPositionCount();
        Block[] outputBlocks = new Block[leftPage.getBlockCount() + rightDataColumnCount];
        boolean success = false;
        try {
            for (int c = 0; c < leftPage.getBlockCount(); c++) {
                outputBlocks[c] = leftPage.getBlock(c);
                outputBlocks[c].incRef();
            }
            for (int col = 0; col < rightDataColumnCount; col++) {
                outputBlocks[leftPage.getBlockCount() + col] = blockFactory.newConstantNullBlock(outputSize);
            }
            success = true;
            return new Page(outputBlocks);
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(outputBlocks);
            }
        }
    }

    private ElementType determineRightColumnType(int rightCol) {
        for (Page page : rightPages) {
            if (page.getBlockCount() > rightCol) {
                return page.getBlock(rightCol).elementType();
            }
        }
        return ElementType.NULL;
    }

    /**
     * Returns the total number of distinct composite keys in the hash table.
     */
    public int keyCount() {
        return hash.numKeys();
    }

    /**
     * Returns the total number of right-side rows indexed in the hash table.
     */
    public int indexedRowCount() {
        int count = 0;
        for (List<RowRef> refs : groupIdToRows) {
            count += refs.size();
        }
        return count;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(hash);
        for (Page page : rightPages) {
            page.releaseBlocks();
        }
        rightPages.clear();
        groupIdToRows.clear();
    }
}
