/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

/**
 * A {@link LocalSupplier} that allways creates a new copy of the {@link Block}s initially provided at creation time.
 */
public class CopyingLocalSupplier extends ImmediateLocalSupplier {

    public CopyingLocalSupplier(Block[] blocks) {
        super(blocks);
    }

    @Override
    public Block[] get() {
        Block[] blockCopies = new Block[blocks.length];
        for (int i = 0; i < blockCopies.length; i++) {
            blockCopies[i] = BlockUtils.deepCopyOf(blocks[i], PlannerUtils.NON_BREAKING_BLOCK_FACTORY);
        }
        return blockCopies;
    }
}
