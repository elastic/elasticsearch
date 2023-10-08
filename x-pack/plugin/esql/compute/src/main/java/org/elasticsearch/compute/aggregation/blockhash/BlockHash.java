/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
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
public abstract sealed class BlockHash implements Releasable, SeenGroupIds //
    permits BooleanBlockHash, BytesRefBlockHash, DoubleBlockHash, IntBlockHash, LongBlockHash,//
    PackedValuesBlockHash, BytesRefLongBlockHash, LongLongBlockHash {

    protected final BigArrays bigArrays;
    protected final BlockFactory blockFactory;

    BlockHash(DriverContext driverContext) {
        bigArrays = driverContext.bigArrays();
        blockFactory = driverContext.blockFactory();
    }

    /**
     * Add all values for the "group by" columns in the page to the hash and
     * pass the ordinals to the provided {@link GroupingAggregatorFunction.AddInput}.
     */
    public abstract void add(Page page, GroupingAggregatorFunction.AddInput addInput);

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

    // TODO merge with nonEmpty
    @Override
    public abstract BitArray seenGroupIds(BigArrays bigArrays);

    /**
     * Creates a specialized hash table that maps one or more {@link Block}s to ids.
     * @param emitBatchSize maximum batch size to be emitted when handling combinatorial
     *                      explosion of groups caused by multivalued fields
     * @param allowBrokenOptimizations true ot allow optimizations with bad null handling. We will fix their
     *                                 null handling and remove this flag, but we need to disable these in
     *                                 production until we can. And this lets us continue to compile and
     *                                 test them.
     */
    public static BlockHash build(
        List<HashAggregationOperator.GroupSpec> groups,
        DriverContext driverContext,
        int emitBatchSize,
        boolean allowBrokenOptimizations
    ) {
        if (groups.size() == 1) {
            return newForElementType(groups.get(0).channel(), groups.get(0).elementType(), driverContext);
        }
        if (allowBrokenOptimizations && groups.size() == 2) {
            var g1 = groups.get(0);
            var g2 = groups.get(1);
            if (g1.elementType() == ElementType.LONG && g2.elementType() == ElementType.LONG) {
                return new LongLongBlockHash(driverContext, g1.channel(), g2.channel(), emitBatchSize);
            }
            if (g1.elementType() == ElementType.BYTES_REF && g2.elementType() == ElementType.LONG) {
                return new BytesRefLongBlockHash(driverContext, g1.channel(), g2.channel(), false, emitBatchSize);
            }
            if (g1.elementType() == ElementType.LONG && g2.elementType() == ElementType.BYTES_REF) {
                return new BytesRefLongBlockHash(driverContext, g2.channel(), g1.channel(), true, emitBatchSize);
            }
        }
        return new PackedValuesBlockHash(groups, driverContext, emitBatchSize);
    }

    /**
     * Creates a specialized hash table that maps a {@link Block} of the given input element type to ids.
     */
    private static BlockHash newForElementType(int channel, ElementType type, DriverContext driverContext) {
        return switch (type) {
            case BOOLEAN -> new BooleanBlockHash(channel, driverContext);
            case INT -> new IntBlockHash(channel, driverContext);
            case LONG -> new LongBlockHash(channel, driverContext);
            case DOUBLE -> new DoubleBlockHash(channel, driverContext);
            case BYTES_REF -> new BytesRefBlockHash(channel, driverContext);
            default -> throw new IllegalArgumentException("unsupported grouping element type [" + type + "]");
        };
    }

    /**
     * Convert the result of calling {@link LongHash} or {@link LongLongHash}
     * or {@link BytesRefHash} or similar to a group ordinal. These hashes
     * return negative numbers if the value that was added has already been
     * seen. We don't use that and convert it back to the positive ord.
     */
    public static long hashOrdToGroup(long ord) {
        if (ord < 0) { // already seen
            return -1 - ord;
        }
        return ord;
    }

    /**
     * Convert the result of calling {@link LongHash} or {@link LongLongHash}
     * or {@link BytesRefHash} or similar to a group ordinal, reserving {@code 0}
     * for null.
     */
    public static long hashOrdToGroupNullReserved(long ord) {
        return hashOrdToGroup(ord) + 1;
    }
}
