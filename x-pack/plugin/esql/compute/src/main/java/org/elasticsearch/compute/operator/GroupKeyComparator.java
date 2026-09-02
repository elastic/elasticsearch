/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.util.List;

/**
 * Compares grouping-key positions across blocks with ascending order and nulls last semantics.
 */
public class GroupKeyComparator {

    private final List<Integer> groupChannels;
    private final BytesRef scratchA;
    private final BytesRef scratchB;

    public GroupKeyComparator(List<Integer> sortChannels) {
        this.groupChannels = sortChannels;
        scratchA = new BytesRef();
        scratchB = new BytesRef();
    }

    public int compare(Page keys, int posA, int posB) {
        return compare(keys, posA, keys, posB);
    }

    public int compare(Page keysA, int posA, Page keysB, int posB) {
        for (int channel : groupChannels) {
            int cmp = compareAtPosition(keysA.getBlock(channel), posA, keysB.getBlock(channel), posB);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private int compareAtPosition(Block blockA, int posA, Block blockB, int posB) {
        assert blockA.elementType() == blockB.elementType();
        boolean nullA = blockA.isNull(posA);
        boolean nullB = blockB.isNull(posB);
        if (nullA || nullB) {
            return Boolean.compare(nullA, nullB);
        }
        int minCount = Math.min(blockA.getValueCount(posA), blockB.getValueCount(posB));
        int indexA = blockA.getFirstValueIndex(posA);
        int indexB = blockB.getFirstValueIndex(posB);
        for (int i = 0; i < minCount; i++) {
            int cmp = compareAtValueIndex(blockA, indexA, blockB, indexB);
            if (cmp != 0) {
                return cmp;
            }
            indexA++;
            indexB++;
        }
        return Integer.compare(blockA.getValueCount(posA), blockB.getValueCount(posB));
    }

    private int compareAtValueIndex(Block blockA, int indexA, Block blockB, int indexB) {
        return switch (blockA.elementType()) {
            case NULL -> 0;
            case BOOLEAN -> Boolean.compare(((BooleanBlock) blockA).getBoolean(indexA), ((BooleanBlock) blockB).getBoolean(indexB));
            case INT -> Integer.compare(((IntBlock) blockA).getInt(indexA), ((IntBlock) blockB).getInt(indexB));
            case LONG -> Long.compare(((LongBlock) blockA).getLong(indexA), ((LongBlock) blockB).getLong(indexB));
            case FLOAT -> Float.compare(((FloatBlock) blockA).getFloat(indexA), ((FloatBlock) blockB).getFloat(indexB));
            case DOUBLE -> Double.compare(((DoubleBlock) blockA).getDouble(indexA), ((DoubleBlock) blockB).getDouble(indexB));
            case BYTES_REF -> ((BytesRefBlock) blockA).getBytesRef(indexA, scratchA)
                .compareTo(((BytesRefBlock) blockB).getBytesRef(indexB, scratchB));
            default -> throw new IllegalArgumentException("unsupported element type for group key comparison: " + blockA.elementType());
        };
    }
}
