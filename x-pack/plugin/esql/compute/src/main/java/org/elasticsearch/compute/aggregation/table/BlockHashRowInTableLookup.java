/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.ReleasableIterator;

import java.util.ArrayList;
import java.util.List;

final class BlockHashRowInTableLookup extends RowInTableLookup {
    private final BlockHash hash;

    BlockHashRowInTableLookup(BlockFactory blockFactory, Block[] keys) {
        List<BlockHash.GroupSpec> groups = new ArrayList<>(keys.length);
        for (int k = 0; k < keys.length; k++) {
            groups.add(new BlockHash.GroupSpec(k, keys[k].elementType()));
        }

        hash = BlockHash.buildPackedValuesBlockHash(
            groups,
            blockFactory,
            (int) BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE.getBytes()
        );
        boolean success = false;
        try {
            hash.add(new Page(keys), new BlockHashGroupIdsAddInput() {
                private int lastOrd = -1;

                @Override
                protected void processGroupId(int position, int ord) {
                    if (ord != lastOrd + 1) {
                        // TODO double check these errors over REST once we have LOOKUP
                        throw new IllegalArgumentException("found a duplicate row");
                    }
                    lastOrd = ord;
                }
            });
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        return hash.lookup(page, targetBlockSize);
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(
        Page page,
        ByteSizeValue targetBlockSize,
        BlockFactory resultBlockFactory,
        Warnings warnings
    ) {
        return hash.concurrentLookup(page, targetBlockSize, resultBlockFactory);
    }

    @Override
    public String toString() {
        return hash.toString();
    }

    @Override
    public void close() {
        hash.close();
    }
}
