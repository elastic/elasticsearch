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
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * {@link EnrichResultBuilder} for DoubleRanges.
 * This class is generated. Edit `X-EnrichResultBuilder.java.st` instead.
 */
final class EnrichResultBuilderForDoubleRange extends EnrichResultBuilder {
    private ObjectArray<double[]> cells;

    EnrichResultBuilderForDoubleRange(BlockFactory blockFactory, int channel) {
        super(blockFactory, channel);
        this.cells = blockFactory.bigArrays().newObjectArray(1);
    }

    @Override
    void addInputPage(IntVector positions, Page page) {
        DoubleRangeBlock block = page.getBlock(channel);
        DoubleRangeBlockBuilder.DoubleRange scratch = new DoubleRangeBlockBuilder.DoubleRange();
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
                scratch = block.getDoubleRange(firstValueIndex + v, scratch);
                newCell[dstIndex + v * 2] = scratch.from();
                newCell[dstIndex + v * 2 + 1] = scratch.to();
            }
        }
    }

    private double[] extendCell(double[] oldCell, int newValueCount) {
        if (oldCell == null) {
            return new double[newValueCount * 2];
        } else {
            return Arrays.copyOf(oldCell, oldCell.length + newValueCount * 2);
        }
    }

    private double[] combineCell(double[] first, double[] second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        var result = new double[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private void appendGroupToBlockBuilder(DoubleRangeBlock.Builder builder, double[] group) {
        if (group == null) {
            builder.appendNull();
        } else if (group.length == 2) {
            builder.appendDoubleRange(group[0], group[1]);
        } else {
            builder.beginPositionEntry();
            // TODO: sort and dedup and set MvOrdering
            for (int v = 0; v < group.length / 2; v++) {
                builder.appendDoubleRange(group[v * 2], group[v * 2 + 1]);
            }
            builder.endPositionEntry();
        }
    }

    private double[] getCellOrNull(int position) {
        return position < cells.size() ? cells.get(position) : null;
    }

    private Block buildWithSelected(IntBlock selected) {
        try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(selected.getPositionCount())) {
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
        try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(selected.getPositionCount())) {
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
