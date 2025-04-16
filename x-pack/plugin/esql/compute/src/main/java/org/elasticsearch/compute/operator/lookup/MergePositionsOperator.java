/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.Objects;

/**
 * Combines values at the given blocks with the same positions into a single position
 * for the blocks at the given channels.
 * <p>
 * Example, input pages consisting of three blocks:
 * </p>
 * <pre>{@code
 * | positions    | field-1 | field-2 |
 * ------------------------------------
 * Page 1:
 * | 1            |  a,b    |   2020  |
 * | 1            |  c      |   2021  |
 * Page 2:
 * | 2            |  a,e    |   2021  |
 * Page 3:
 * | 4            |  d      |   null  |
 * }</pre>
 * Output:
 * <pre>{@code
 * |  field-1   | field-2    |
 * ---------------------------
 * |  null      | null       |
 * |  a,b,c     | 2020,2021  |
 * |  a,e       | 2021       |
 * |  null      | null       |
 * |  d         | 2023       |
 * }</pre>
 */
public final class MergePositionsOperator implements Operator {
    private boolean finished = false;
    private final int positionChannel;
    private final EnrichResultBuilder[] builders;
    private final IntBlock selectedPositions;

    private Page outputPage;

    public MergePositionsOperator(
        int positionChannel,
        int[] mergingChannels,
        ElementType[] mergingTypes,
        IntBlock selectedPositions,
        BlockFactory blockFactory
    ) {
        if (mergingChannels.length != mergingTypes.length) {
            throw new IllegalArgumentException(
                "Merging channels don't match merging types; channels="
                    + Arrays.toString(mergingChannels)
                    + ",types="
                    + Arrays.toString(mergingTypes)
            );
        }
        this.positionChannel = positionChannel;
        this.builders = new EnrichResultBuilder[mergingTypes.length];
        try {
            for (int i = 0; i < mergingTypes.length; i++) {
                builders[i] = EnrichResultBuilder.enrichResultBuilder(mergingTypes[i], blockFactory, mergingChannels[i]);
            }
        } finally {
            if (builders[builders.length - 1] == null) {
                Releasables.close(Releasables.wrap(builders));
            }
        }
        selectedPositions.mustIncRef();
        this.selectedPositions = selectedPositions;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            final IntBlock positions = page.getBlock(positionChannel);
            final IntVector positionsVector = Objects.requireNonNull(positions.asVector(), "positions must be a vector");
            for (EnrichResultBuilder builder : builders) {
                builder.addInputPage(positionsVector, page);
            }
        } finally {
            Releasables.closeExpectNoException(page::releaseBlocks);
        }
    }

    @Override
    public void finish() {
        final Block[] blocks = new Block[builders.length];
        try {
            for (int i = 0; i < builders.length; i++) {
                blocks[i] = builders[i].build(selectedPositions);
            }
            outputPage = new Page(blocks);
        } finally {
            finished = true;
            if (outputPage == null) {
                Releasables.close(blocks);
            }
        }
    }

    @Override
    public boolean isFinished() {
        return finished && outputPage == null;
    }

    @Override
    public Page getOutput() {
        Page page = this.outputPage;
        this.outputPage = null;
        return page;
    }

    @Override
    public void close() {
        Releasables.close(Releasables.wrap(builders), selectedPositions, () -> {
            if (outputPage != null) {
                outputPage.releaseBlocks();
            }
        });
    }
}
