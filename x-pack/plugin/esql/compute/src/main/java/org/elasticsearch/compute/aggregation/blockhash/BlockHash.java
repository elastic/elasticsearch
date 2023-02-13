/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.core.Releasable;

import java.util.List;

/**
 * A specialized hash table implementation maps values of a {@link Block} to ids (in longs).
 * This class delegates to {@link LongHash} or {@link BytesRefHash}.
 *
 * @see LongHash
 * @see BytesRefHash
 */
public abstract sealed class BlockHash
    implements
        Releasable permits BooleanBlockHash,BytesRefBlockHash,DoubleBlockHash,IntBlockHash,LongBlockHash,PackedValuesBlockHash {

    /**
     * Add all values for the "group by" columns in the page to the hash and return
     * their ordinal in a LongBlock.
     */
    public abstract LongBlock add(Page page);

    /**
     * Returns a {@link Block} that contains all the keys that are inserted by {@link #add}.
     */
    public abstract Block[] getKeys();

    /**
     * Creates a specialized hash table that maps one or more {@link Block}s to ids.
     */
    public static BlockHash build(List<HashAggregationOperator.GroupSpec> groups, BigArrays bigArrays) {
        if (groups.size() == 1) {
            return newForElementType(groups.get(0).channel(), groups.get(0).elementType(), bigArrays);
        }
        return new PackedValuesBlockHash(groups, bigArrays);
    }

    /**
     * Creates a specialized hash table that maps a {@link Block} of the given input element type to ids.
     */
    private static BlockHash newForElementType(int channel, ElementType type, BigArrays bigArrays) {
        return switch (type) {
            case BOOLEAN -> new BooleanBlockHash(channel);
            case INT -> new IntBlockHash(channel, bigArrays);
            case LONG -> new LongBlockHash(channel, bigArrays);
            case DOUBLE -> new DoubleBlockHash(channel, bigArrays);
            case BYTES_REF -> new BytesRefBlockHash(channel, bigArrays);
            default -> throw new IllegalArgumentException("unsupported grouping element type [" + type + "]");
        };
    }

    protected static long hashOrdToGroup(long ord) {
        if (ord < 0) { // already seen
            return -1 - ord;
        }
        return ord;
    }
}
