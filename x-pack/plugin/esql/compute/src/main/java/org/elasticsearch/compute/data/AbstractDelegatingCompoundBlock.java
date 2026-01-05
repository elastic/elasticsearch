/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Abstract base class for block types that are implemented by delegating to several concrete sub blocks.
 */
public abstract class AbstractDelegatingCompoundBlock extends AbstractNonThreadSafeRefCounted implements Block {

    /**
     * @return a list of the sub-blocks composing this compound block.  The order of the list should match the order
     * expected by {@link AbstractDelegatingCompoundBlock#buildFromSubBlocks(List)}
     */
    protected abstract List<Block> getSubBlocks();

    /**
     * Construct a new instance of the block, based on the given list of sub-blocks.
     * @param subBlocks List of sub-blocks, in the same order as {@link AbstractDelegatingCompoundBlock#getSubBlocks()}
     * @return a new instance based on the given blocks.
     */
    protected abstract AbstractDelegatingCompoundBlock buildFromSubBlocks(List<Block> subBlocks);

    @Override
    public void allowPassingToDifferentDriver() {
        getSubBlocks().forEach(Block::allowPassingToDifferentDriver);
    }

    private Block applyOperationToSubBlocks(Function<Block, Block> operation) {
        List<Block> modifiedBlocks = new ArrayList<>(getSubBlocks().size());
        boolean success = false;
        try {
            for  (Block block : getSubBlocks()) {
                modifiedBlocks.add(operation.apply(block));
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(getSubBlocks());
            }
        }
        return buildFromSubBlocks(modifiedBlocks);
    }

    @Override
    public Block filter(int... positions) {
        return applyOperationToSubBlocks(block -> block.filter(positions));
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        return applyOperationToSubBlocks(block -> block.keepMask(mask));
    }

    @Override
    public Block deepCopy(BlockFactory blockFactory) {
        return applyOperationToSubBlocks(block -> block.deepCopy(blockFactory));
    }

    @Override
    public long ramBytesUsed() {
        long bytes = 0;
        for (Block b : getSubBlocks()) {
            bytes += b.ramBytesUsed();
        }
        return bytes;
    }

}
