/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;

/**
 * Removes duplicate values from multivalued positions.
 */
public class MultivalueDedupeBoolean {
    private final BooleanBlock block;
    private boolean seenTrue;
    private boolean seenFalse;

    public MultivalueDedupeBoolean(BooleanBlock block) {
        this.block = block;
    }

    /**
     * Dedupe values using an adaptive algorithm based on the size of the input list.
     */
    public BooleanBlock dedupeToBlock() {
        if (false == block.mayHaveMultivaluedFields()) {
            return block;
        }
        BooleanBlock.Builder builder = BooleanBlock.newBlockBuilder(block.getPositionCount());
        for (int p = 0; p < block.getPositionCount(); p++) {
            int count = block.getValueCount(p);
            int first = block.getFirstValueIndex(p);
            switch (count) {
                case 0 -> builder.appendNull();
                case 1 -> builder.appendBoolean(block.getBoolean(first));
                default -> {
                    readValues(first, count);
                    writeValues(builder);
                }
            }
        }
        return builder.build();
    }

    /**
     * Dedupe values and build a {@link LongBlock} suitable for passing
     * as the grouping block to a {@link GroupingAggregatorFunction}.
     * @param everSeen array tracking if the values {@code false} and {@code true} are ever seen
     */
    public LongBlock hash(boolean[] everSeen) {
        LongBlock.Builder builder = LongBlock.newBlockBuilder(block.getPositionCount());
        for (int p = 0; p < block.getPositionCount(); p++) {
            int count = block.getValueCount(p);
            int first = block.getFirstValueIndex(p);
            switch (count) {
                case 0 -> builder.appendNull();
                case 1 -> builder.appendLong(hashOrd(everSeen, block.getBoolean(first)));
                default -> {
                    readValues(first, count);
                    hashValues(everSeen, builder);
                }
            }
        }
        return builder.build();
    }

    private void readValues(int first, int count) {
        int end = first + count;

        seenFalse = false;
        seenTrue = false;
        for (int i = first; i < end; i++) {
            if (block.getBoolean(i)) {
                seenTrue = true;
                if (seenFalse) {
                    break;
                }
            } else {
                seenFalse = true;
                if (seenTrue) {
                    break;
                }
            }
        }
    }

    private void writeValues(BooleanBlock.Builder builder) {
        if (seenFalse) {
            if (seenTrue) {
                builder.beginPositionEntry();
                builder.appendBoolean(false);
                builder.appendBoolean(true);
                builder.endPositionEntry();
            } else {
                builder.appendBoolean(false);
            }
        } else if (seenTrue) {
            builder.appendBoolean(true);
        } else {
            throw new IllegalStateException("didn't see true of false but counted values");
        }
    }

    private void hashValues(boolean[] everSeen, LongBlock.Builder builder) {
        if (seenFalse) {
            if (seenTrue) {
                builder.beginPositionEntry();
                builder.appendLong(hashOrd(everSeen, false));
                builder.appendLong(hashOrd(everSeen, true));
                builder.endPositionEntry();
            } else {
                builder.appendLong(hashOrd(everSeen, false));
            }
        } else if (seenTrue) {
            builder.appendLong(hashOrd(everSeen, true));
        } else {
            throw new IllegalStateException("didn't see true of false but counted values");
        }
    }

    public static long hashOrd(boolean[] everSeen, boolean b) {
        if (b) {
            everSeen[1] = true;
            return 1;
        }
        everSeen[0] = true;
        return 0;
    }
}
