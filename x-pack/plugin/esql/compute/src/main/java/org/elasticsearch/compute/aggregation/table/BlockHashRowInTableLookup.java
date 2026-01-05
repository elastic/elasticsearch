/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
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
            hash.add(new Page(keys), new GroupingAggregatorFunction.AddInput() {
                private int lastOrd = -1;

                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    for (int p = 0; p < groupIds.getPositionCount(); p++) {
                        int first = groupIds.getFirstValueIndex(p);
                        int end = groupIds.getValueCount(p) + first;
                        for (int i = first; i < end; i++) {
                            int ord = groupIds.getInt(i);
                            if (ord != lastOrd + 1) {
                                // TODO double check these errors over REST once we have LOOKUP
                                throw new IllegalArgumentException("found a duplicate row");
                            }
                            lastOrd = ord;
                        }
                    }
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    for (int p = 0; p < groupIds.getPositionCount(); p++) {
                        int first = groupIds.getFirstValueIndex(p);
                        int end = groupIds.getValueCount(p) + first;
                        for (int i = first; i < end; i++) {
                            int ord = groupIds.getInt(i);
                            if (ord != lastOrd + 1) {
                                // TODO double check these errors over REST once we have LOOKUP
                                throw new IllegalArgumentException("found a duplicate row");
                            }
                            lastOrd = ord;
                        }
                    }
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    for (int p = 0; p < groupIds.getPositionCount(); p++) {
                        int ord = groupIds.getInt(p);
                        if (ord != lastOrd + 1) {
                            throw new IllegalArgumentException("found a duplicate row");
                        }
                        lastOrd = ord;
                    }
                }

                @Override
                public void close() {}
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
    public String toString() {
        return hash.toString();
    }

    @Override
    public void close() {
        hash.close();
    }
}
