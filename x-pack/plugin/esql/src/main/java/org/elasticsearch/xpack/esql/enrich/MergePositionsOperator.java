/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Combines values at the given blocks with the same positions into a single position for the blocks at the given channels
 * Example, input page consisting of three blocks:
 * positions    | field-1 | field-2 |
 *-----------------------------------
 *    2         |  a,b    |   2020  |
 *    3         |  c      |   2021  |
 *    2         |  a,e    |   2021  |
 *    1         |  d      |   null  |
 *    5         |  null   |   2023  |
 * Output:
 * |  field-1   | field-2    |
 * ---------------------------
 * |  null      | null       |
 * |  d         | null       |
 * |  a, b, e   | 2020, 2021 |
 * |  c         | 2021       |
 * |  null      | null       |
 * |  null      | 2023       |
 */
// TODO: support multi positions and deduplicate
final class MergePositionsOperator implements Operator {
    private final List<Page> pages = new ArrayList<>();
    private boolean finished = false;
    private final int positionCount;
    private final int[] mergingChannels;

    MergePositionsOperator(int positionCount, int[] mergingChannels) {
        this.positionCount = positionCount;
        this.mergingChannels = mergingChannels;
    }

    // Add the more positions
    @Override
    public boolean needsInput() {
        return true;
    }

    @Override
    public void addInput(Page page) {
        pages.add(page);
        if (pages.size() > 1) {
            // TODO: Use PQ to support multiple pages
            throw new UnsupportedOperationException("Expected single segment for enrich now");
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && pages.isEmpty();
    }

    @Override
    public Page getOutput() {
        if (finished == false) {
            return null;
        }
        if (pages.isEmpty()) {
            return null;
        }
        Page page = pages.get(0);
        pages.clear();

        IntVector positions = ((IntBlock) page.getBlock(0)).asVector();
        int[] indices = sortedIndicesByPositions(positions);
        final Block[] inputs = new Block[mergingChannels.length];
        final Block.Builder[] outputs = new Block.Builder[mergingChannels.length];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = page.getBlock(mergingChannels[i]);
            outputs[i] = inputs[i].elementType().newBlockBuilder(inputs[i].getPositionCount());
        }
        int addedPositions = 0;
        int lastPosition = -1;
        for (int index : indices) {
            int position = positions.getInt(index);
            if (lastPosition < position) {
                for (int i = addedPositions; i < position; i++) {
                    for (Block.Builder builder : outputs) {
                        builder.appendNull();
                    }
                    addedPositions++;
                }
                for (int c = 0; c < outputs.length; c++) {
                    outputs[c].copyFrom(inputs[c], index, index + 1);
                }
                addedPositions++;
            } else {
                // TODO: combine multiple positions into a single position
                throw new UnsupportedOperationException("Multiple matches are not supported yet ");
            }
            lastPosition = position;
        }
        for (int i = addedPositions; i < positionCount; i++) {
            for (Block.Builder builder : outputs) {
                builder.appendNull();
            }
            addedPositions++;
        }
        Page result = new Page(Arrays.stream(outputs).map(Block.Builder::build).toArray(Block[]::new));
        assert result.getPositionCount() == positionCount;
        return result;
    }

    private static int[] sortedIndicesByPositions(IntVector positions) {
        int[] indices = new int[positions.getPositionCount()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        new IntroSorter() {
            int pivot;

            @Override
            protected void setPivot(int i) {
                pivot = indices[i];
            }

            @Override
            protected int comparePivot(int j) {
                return Integer.compare(positions.getInt(pivot), positions.getInt(indices[j]));
            }

            @Override
            protected void swap(int i, int j) {
                int tmp = indices[i];
                indices[i] = indices[j];
                indices[j] = tmp;
            }
        }.sort(0, indices.length);
        return indices;
    }

    @Override
    public void close() {

    }
}
