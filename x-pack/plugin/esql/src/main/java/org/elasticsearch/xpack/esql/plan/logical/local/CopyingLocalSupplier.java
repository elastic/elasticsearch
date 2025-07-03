/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceRowAsLocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.EsqlSession;

import java.io.IOException;
import java.util.Arrays;

/**
 * A {@link LocalSupplier} that allways creates a new copy of the {@link Block}s initially provided at creation time.
 * This is created specifically for {@link InlineStats} usage in {@link EsqlSession} for queries that use ROW command.
 *
 * The ROW which gets replaced by {@link ReplaceRowAsLocalRelation} with a {@link LocalRelation} will have its blocks
 * used (and released) at least twice:
 * - the {@link LocalRelation} from the left-hand side is used as a source for the right-hand side
 * - the same {@link LocalRelation} is then used to continue the execution of the query on the left-hand side
 *
 * It delegates all its operations to {@link ImmediateLocalSupplier} and, to prevent the double release, it will always
 * create a deep copy of the blocks received in the constructor initially.
 *
 * Example with the flow and the blocks reuse for a query like "row x = 1 | inlinestats y = max(x)"
 * Step 1:
 * Limit[1000[INTEGER],true]
 * \_InlineJoin[LEFT,[],[],[]]
 *   |_Limit[1000[INTEGER],false]
 *   | \_LocalRelation[[x{r}#99],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
 *   \_Aggregate[[],[MAX(x{r}#99,true[BOOLEAN]) AS y#102]]
 *     \_StubRelation[[x{r}#99]]
 *
 * Step 2:
 * Aggregate[[],[MAX(x{r}#99,true[BOOLEAN]) AS y#102]]
 * \_Limit[1000[INTEGER],false]
 *   \_LocalRelation[[x{r}#99],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
 *
 * Step 3:
 * Limit[1000[INTEGER],true]
 * \_Eval[[1[INTEGER] AS y#102]]
 *   \_Limit[1000[INTEGER],false]
 *     \_LocalRelation[[x{r}#99],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
 */
public class CopyingLocalSupplier implements LocalSupplier {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LocalSupplier.class,
        "CopyingSupplier",
        CopyingLocalSupplier::new
    );

    private final ImmediateLocalSupplier delegate;

    public CopyingLocalSupplier(Block[] blocks) {
        delegate = new ImmediateLocalSupplier(blocks);
    }

    public CopyingLocalSupplier(StreamInput in) throws IOException {
        delegate = new ImmediateLocalSupplier(in);
    }

    @Override
    public Block[] get() {
        Block[] blockCopies = new Block[delegate.blocks.length];
        for (int i = 0; i < blockCopies.length; i++) {
            blockCopies[i] = BlockUtils.deepCopyOf(delegate.blocks[i], PlannerUtils.NON_BREAKING_BLOCK_FACTORY);
        }
        return blockCopies;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        delegate.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        CopyingLocalSupplier other = (CopyingLocalSupplier) obj;
        return Arrays.equals(delegate.blocks, other.delegate.blocks);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}
