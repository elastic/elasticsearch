/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;

/**
 * A {@link LocalSupplier} that allways creates a new copy of the {@link Block}s initially provided at creation time.
 */
public class CopyingLocalSupplier extends ImmediateLocalSupplier {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LocalSupplier.class,
        "CopyingSupplier",
        CopyingLocalSupplier::new
    );

    public CopyingLocalSupplier(Block[] blocks) {
        super(blocks);
    }

    public CopyingLocalSupplier(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Block[] get() {
        Block[] blockCopies = new Block[blocks.length];
        for (int i = 0; i < blockCopies.length; i++) {
            blockCopies[i] = BlockUtils.deepCopyOf(blocks[i], PlannerUtils.NON_BREAKING_BLOCK_FACTORY);
        }
        return blockCopies;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }
}
