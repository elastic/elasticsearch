/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Combines values at the given blocks with the same positions into a single position for the blocks at the given channels
 * Example, input pages consisting of three blocks:
 * positions    | field-1 | field-2 |
 * -----------------------------------
 * Page 1:
 * 1           |  a,b    |   2020  |
 * 1           |  c      |   2021  |
 * ---------------------------------
 * Page 2:
 * 2           |  a,e    |   2021  |
 * ---------------------------------
 * Page 3:
 * 4           |  d      |   null  |
 * ---------------------------------
 * Output:
 * |  field-1   | field-2    |
 * ---------------------------
 * |  null      | null       |
 * |  a,b,c     | 2020,2021  |
 * |  a,e       | 2021       |
 * |  null      | null       |
 * |  d         | 2023       |
 */
final class MergePositionsOperator implements Operator {
    private boolean finished = false;
    private int filledPositions = 0;
    private final boolean singleMode;
    private final int positionCount;
    private final int positionChannel;

    private final Block.Builder[] outputBuilders;
    private final int[] mergingChannels;
    private final ElementType[] mergingTypes;
    private PositionBuilder positionBuilder = null;

    private Page outputPage;
    private final BlockFactory blockFactory;

    MergePositionsOperator(
        boolean singleMode,
        int positionCount,
        int positionChannel,
        int[] mergingChannels,
        ElementType[] mergingTypes,
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
        this.blockFactory = blockFactory;
        this.singleMode = singleMode;
        this.positionCount = positionCount;
        this.positionChannel = positionChannel;
        this.mergingChannels = mergingChannels;
        this.mergingTypes = mergingTypes;
        this.outputBuilders = new Block.Builder[mergingTypes.length];
        try {
            for (int i = 0; i < mergingTypes.length; i++) {
                outputBuilders[i] = mergingTypes[i].newBlockBuilder(positionCount, blockFactory);
            }
        } finally {
            if (outputBuilders[outputBuilders.length - 1] == null) {
                Releasables.close(outputBuilders);
            }
        }
    }

    @Override
    public boolean needsInput() {
        return true;
    }

    @Override
    public void addInput(Page page) {
        try {
            final IntBlock positions = page.getBlock(positionChannel);
            final int currentPosition = positions.getInt(0);
            if (singleMode) {
                fillNullUpToPosition(currentPosition);
                for (int i = 0; i < mergingChannels.length; i++) {
                    int channel = mergingChannels[i];
                    outputBuilders[i].appendAllValuesToCurrentPosition(page.getBlock(channel));
                }
                filledPositions++;
            } else {
                if (positionBuilder != null && positionBuilder.position != currentPosition) {
                    flushPositionBuilder();
                }
                if (positionBuilder == null) {
                    positionBuilder = new PositionBuilder(currentPosition, mergingTypes, blockFactory);
                }
                positionBuilder.combine(page, mergingChannels);
            }
        } finally {
            Releasables.closeExpectNoException(page::releaseBlocks);
        }
    }

    static final class PositionBuilder implements Releasable {
        private final int position;
        private final Block.Builder[] builders;

        PositionBuilder(int position, ElementType[] elementTypes, BlockFactory blockFactory) {
            this.position = position;
            this.builders = new Block.Builder[elementTypes.length];
            try {
                for (int i = 0; i < builders.length; i++) {
                    builders[i] = elementTypes[i].newBlockBuilder(1, blockFactory);
                }
            } finally {
                if (builders[builders.length - 1] == null) {
                    Releasables.close(builders);
                }
            }
        }

        void combine(Page page, int[] channels) {
            for (int i = 0; i < channels.length; i++) {
                Block block = page.getBlock(channels[i]);
                builders[i].appendAllValuesToCurrentPosition(block);
            }
        }

        void buildTo(Block.Builder[] output) {
            for (int i = 0; i < output.length; i++) {
                try (var b = builders[i]; Block block = b.build()) {
                    output[i].appendAllValuesToCurrentPosition(block);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(builders);
        }
    }

    private void flushPositionBuilder() {
        fillNullUpToPosition(positionBuilder.position);
        filledPositions++;
        try (var p = positionBuilder) {
            p.buildTo(outputBuilders);
        } finally {
            positionBuilder = null;
        }
    }

    private void fillNullUpToPosition(int position) {
        while (filledPositions < position) {
            for (Block.Builder builder : outputBuilders) {
                builder.appendNull();
            }
            filledPositions++;
        }
    }

    @Override
    public void finish() {
        if (positionBuilder != null) {
            flushPositionBuilder();
        }
        fillNullUpToPosition(positionCount);
        final Block[] blocks = Block.Builder.buildAll(outputBuilders);
        outputPage = new Page(blocks);
        assert outputPage.getPositionCount() == positionCount;
        finished = true;
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
        Releasables.close(Releasables.wrap(outputBuilders), positionBuilder, () -> {
            if (outputPage != null) {
                outputPage.releaseBlocks();
            }
        });
    }
}
