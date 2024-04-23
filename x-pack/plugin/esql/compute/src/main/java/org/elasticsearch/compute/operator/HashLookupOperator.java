/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HashLookupOperator extends AbstractPageMappingToIteratorOperator {
    /**
     * Factory for {@link HashLookupOperator}. It's received {@link Block}s
     * are never closed, so we need to build them from a non-tracking factory.
     */
    public static class Factory implements Operator.OperatorFactory {
        private final Block[] keys;
        private final int[] blockMapping;

        public Factory(Block[] keys, int[] blockMapping) {
            this.keys = keys;
            this.blockMapping = blockMapping;
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new HashLookupOperator(driverContext.blockFactory(), keys, blockMapping);
        }

        @Override
        public String describe() {
            StringBuilder b = new StringBuilder();
            b.append("HashLookup[keys=[");
            for (int k = 0; k < keys.length; k++) {
                Block key = keys[k];
                if (k != 0) {
                    b.append(", ");
                }
                b.append("{type=").append(key.elementType());
                b.append(", positions=").append(key.getPositionCount());
                b.append(", size=").append(ByteSizeValue.ofBytes(key.ramBytesUsed())).append("}");
            }
            b.append("], mapping=").append(Arrays.toString(blockMapping)).append("]");
            return b.toString();
        }
    }

    private final BlockHash hash;
    private final int[] blockMapping;

    public HashLookupOperator(BlockFactory blockFactory, Block[] keys, int[] blockMapping) {
        this.blockMapping = blockMapping;
        List<BlockHash.GroupSpec> groups = new ArrayList<>(keys.length);
        for (int k = 0; k < keys.length; k++) {
            groups.add(new BlockHash.GroupSpec(k, keys[k].elementType()));
        }
        /*
         * Force PackedValuesBlockHash because it assigned ordinals in order
         * of arrival. We'll figure out how to adapt other block hashes to
         * do that soon. Soon we must figure out how to map ordinals to rows.
         * And, probably at the same time, handle multiple rows containing
         * the same keys.
         */
        this.hash = BlockHash.buildPackedValuesBlockHash(
            groups,
            blockFactory,
            (int) BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE.getBytes()
        );
        boolean success = false;
        try {
            final int[] lastOrd = new int[] { -1 };
            hash.add(new Page(keys), new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    // TODO support multiple rows with the same keys
                    for (int p = 0; p < groupIds.getPositionCount(); p++) {
                        int first = groupIds.getFirstValueIndex(p);
                        int end = groupIds.getValueCount(p) + first;
                        for (int i = first; i < end; i++) {
                            int ord = groupIds.getInt(i);
                            if (ord != lastOrd[0] + 1) {
                                throw new IllegalArgumentException("found a duplicate row");
                            }
                            lastOrd[0] = ord;
                        }
                    }
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    for (int p = 0; p < groupIds.getPositionCount(); p++) {
                        int ord = groupIds.getInt(p);
                        if (ord != lastOrd[0] + 1) {
                            throw new IllegalArgumentException("found a duplicate row");
                        }
                        lastOrd[0] = ord;
                    }
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
    protected ReleasableIterator<Page> receive(Page page) {
        Page mapped = page.projectBlocks(blockMapping);
        page.releaseBlocks();
        return appendBlocks(mapped, hash.lookup(mapped, BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE));
    }

    @Override
    public String toString() {
        return "HashLookup[hash=" + hash + ", mapping=" + Arrays.toString(blockMapping) + "]";
    }

    @Override
    public void close() {
        Releasables.close(super::close, hash);
    }
}
