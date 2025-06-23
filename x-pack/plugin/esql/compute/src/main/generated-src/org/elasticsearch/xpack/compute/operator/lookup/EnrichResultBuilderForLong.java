/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * {@link EnrichResultBuilder} for Longs.
 * This class is generated. Edit `X-EnrichResultBuilder.java.st` instead.
 */
final class EnrichResultBuilderForLong extends EnrichResultBuilder {
    private ObjectArray<long[]> cells;

    EnrichResultBuilderForLong(BlockFactory blockFactory, int channel) {
        super(blockFactory, channel);
        this.cells = blockFactory.bigArrays().newObjectArray(1);
    }

    @Override
    void addInputPage(IntVector positions, Page page) {
        LongBlock block = page.getBlock(channel);
        for (int i = 0; i < positions.getPositionCount(); i++) {
            int valueCount = block.getValueCount(i);
            if (valueCount == 0) {
                continue;
            }
            int cellPosition = positions.getInt(i);
            cells = blockFactory.bigArrays().grow(cells, cellPosition + 1);
            final var oldCell = cells.get(cellPosition);
            final var newCell = extendCell(oldCell, valueCount);
            cells.set(cellPosition, newCell);
            int dstIndex = oldCell != null ? oldCell.length : 0;
            adjustBreaker(RamUsageEstimator.sizeOf(newCell) - (oldCell != null ? RamUsageEstimator.sizeOf(oldCell) : 0));
            int firstValueIndex = block.getFirstValueIndex(i);
            for (int v = 0; v < valueCount; v++) {
                newCell[dstIndex + v] = block.getLong(firstValueIndex + v);
            }
        }
    }

    private long[] extendCell(long[] oldCell, int newValueCount) {
        if (oldCell == null) {
            return new long[newValueCount];
        } else {
            return Arrays.copyOf(oldCell, oldCell.length + newValueCount);
        }
    }

    private long[] combineCell(long[] first, long[] second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        var result = new long[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private void appendGroupToBlockBuilder(LongBlock.Builder builder, long[] group) {
        if (group == null) {
            builder.appendNull();
        } else if (group.length == 1) {
            builder.appendLong(group[0]);
        } else {
            builder.beginPositionEntry();
            // TODO: sort and dedup and set MvOrdering
            for (var v : group) {
                builder.appendLong(v);
            }
            builder.endPositionEntry();
        }
    }

    private long[] getCellOrNull(int position) {
        return position < cells.size() ? cells.get(position) : null;
    }

    private Block buildWithSelected(IntBlock selected) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int selectedCount = selected.getValueCount(i);
                switch (selectedCount) {
                    case 0 -> builder.appendNull();
                    case 1 -> {
                        int groupId = selected.getInt(selected.getFirstValueIndex(i));
                        appendGroupToBlockBuilder(builder, getCellOrNull(groupId));
                    }
                    default -> {
                        int firstValueIndex = selected.getFirstValueIndex(i);
                        var cell = getCellOrNull(selected.getInt(firstValueIndex));
                        for (int p = 1; p < selectedCount; p++) {
                            int groupId = selected.getInt(firstValueIndex + p);
                            cell = combineCell(cell, getCellOrNull(groupId));
                        }
                        appendGroupToBlockBuilder(builder, cell);
                    }
                }
            }
            return builder.build();
        }
    }

    private Block buildWithSelected(IntVector selected) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                appendGroupToBlockBuilder(builder, getCellOrNull(selected.getInt(i)));
            }
            return builder.build();
        }
    }

    @Override
    Block build(IntBlock selected) {
        var vector = selected.asVector();
        if (vector != null) {
            return buildWithSelected(vector);
        } else {
            return buildWithSelected(selected);
        }
    }

    @Override
    public void close() {
        Releasables.close(cells, super::close);
    }
}
