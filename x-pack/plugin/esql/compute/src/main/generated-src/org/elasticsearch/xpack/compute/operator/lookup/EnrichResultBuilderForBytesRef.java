/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * {@link EnrichResultBuilder} for BytesRefs.
 * This class is generated. Edit `X-EnrichResultBuilder.java.st` instead.
 */
final class EnrichResultBuilderForBytesRef extends EnrichResultBuilder {
    private final BytesRefArray bytes; // shared between all cells
    private BytesRef scratch = new BytesRef();
    private ObjectArray<int[]> cells;

    EnrichResultBuilderForBytesRef(BlockFactory blockFactory, int channel) {
        super(blockFactory, channel);
        this.cells = blockFactory.bigArrays().newObjectArray(1);
        BytesRefArray bytes = null;
        try {
            bytes = new BytesRefArray(1L, blockFactory.bigArrays());
            this.bytes = bytes;
        } finally {
            if (bytes == null) {
                this.cells.close();
            }
        }
    }

    @Override
    void addInputPage(IntVector positions, Page page) {
        BytesRefBlock block = page.getBlock(channel);
        BytesRef scratch = new BytesRef();
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
            int bytesOrd = Math.toIntExact(bytes.size());
            for (int v = 0; v < valueCount; v++) {
                scratch = block.getBytesRef(firstValueIndex + v, scratch);
                bytes.append(scratch);
                newCell[dstIndex + v] = bytesOrd + v;
            }
        }
    }

    private int[] extendCell(int[] oldCell, int newValueCount) {
        if (oldCell == null) {
            return new int[newValueCount];
        } else {
            return Arrays.copyOf(oldCell, oldCell.length + newValueCount);
        }
    }

    private int[] combineCell(int[] first, int[] second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        var result = new int[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private void appendGroupToBlockBuilder(BytesRefBlock.Builder builder, int[] group) {
        if (group == null) {
            builder.appendNull();
        } else if (group.length == 1) {
            builder.appendBytesRef(bytes.get(group[0], scratch));
        } else {
            builder.beginPositionEntry();
            // TODO: sort and dedup and set MvOrdering
            for (var v : group) {
                builder.appendBytesRef(bytes.get(v, scratch));
            }
            builder.endPositionEntry();
        }
    }

    private int[] getCellOrNull(int position) {
        return position < cells.size() ? cells.get(position) : null;
    }

    private Block buildWithSelected(IntBlock selected) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(selected.getPositionCount())) {
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
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(selected.getPositionCount())) {
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
        Releasables.close(bytes, cells, super::close);
    }
}
