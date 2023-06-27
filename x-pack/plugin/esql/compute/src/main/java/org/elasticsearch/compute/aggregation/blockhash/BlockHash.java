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
import org.elasticsearch.compute.data.IntVector;
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
public abstract sealed class BlockHash implements Releasable //
    permits BooleanBlockHash, BytesRefBlockHash, DoubleBlockHash, IntBlockHash, LongBlockHash,//
    PackedValuesBlockHash, BytesRefLongBlockHash, LongLongBlockHash {
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
     * The grouping ids that are not empty. We use this because some block hashes reserve
     * space for grouping ids and then don't end up using them. For example,
     * {@link BooleanBlockHash} does this by always assigning {@code false} to {@code 0}
     * and {@code true} to {@code 1}. It's only <strong>after</strong> collection when we
     * know if there actually were any {@code true} or {@code false} values received.
     */
    public abstract IntVector nonEmpty();

    /**
     * Creates a specialized hash table that maps one or more {@link Block}s to ids.
     */
    public static BlockHash build(List<HashAggregationOperator.GroupSpec> groups, BigArrays bigArrays) {
        if (groups.size() == 1) {
            return newForElementType(groups.get(0).channel(), groups.get(0).elementType(), bigArrays);
        }
        if (groups.size() == 2) {
            var g1 = groups.get(0);
            var g2 = groups.get(1);
            if (g1.elementType() == ElementType.LONG && g2.elementType() == ElementType.LONG) {
                return new LongLongBlockHash(bigArrays, g1.channel(), g2.channel());
            }
            if (g1.elementType() == ElementType.BYTES_REF && g2.elementType() == ElementType.LONG) {
                return new BytesRefLongBlockHash(bigArrays, g1.channel(), g2.channel(), false);
            }
            if (g1.elementType() == ElementType.LONG && g2.elementType() == ElementType.BYTES_REF) {
                return new BytesRefLongBlockHash(bigArrays, g2.channel(), g1.channel(), true);
            }
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

    public static long hashOrdToGroup(long ord) {
        if (ord < 0) { // already seen
            return -1 - ord;
        }
        return ord;
    }
}
