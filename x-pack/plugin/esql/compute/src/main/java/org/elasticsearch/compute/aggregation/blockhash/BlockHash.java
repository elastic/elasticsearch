/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.Int3Hash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.index.analysis.AnalysisRegistry;

import java.util.Iterator;
import java.util.List;

/**
 * Specialized hash table implementations that map rows to a <strong>set</strong>
 * of bucket IDs to which they belong to implement {@code GROUP BY} expressions.
 * <p>
 *     A row is always in at least one bucket so the results are never {@code null}.
 *     {@code null} valued key columns will map to some integer bucket id.
 *     If none of key columns are multivalued then the output is always an
 *     {@link IntVector}. If any of the key are multivalued then a row is
 *     in a bucket for each value. If more than one key is multivalued then
 *     the row is in the combinatorial explosion of all value combinations.
 *     Luckily for the number of values rows can only be in each bucket once.
 *     Unluckily, it's the responsibility of {@link BlockHash} to remove those
 *     duplicates.
 * </p>
 * <p>
 *     These classes typically delegate to some combination of {@link BytesRefHash},
 *     {@link LongHash}, {@link LongLongHash}, {@link Int3Hash}. They don't
 *     <strong>technically</strong> have to be hash tables, so long as they
 *     implement the deduplication semantics above and vend integer ids.
 * </p>
 * <p>
 *     The integer ids are assigned to offsets into arrays of aggregation states
 *     so its permissible to have gaps in the ints. But large gaps are a bad
 *     idea because they'll waste space in the aggregations that use these
 *     positions. For example, {@link BooleanBlockHash} assigns {@code 0} to
 *     {@code null}, {@code 1} to {@code false}, and {@code 1} to {@code true}
 *     and that's <strong>fine</strong> and simple and good because it'll never
 *     leave a big gap, even if we never see {@code null}.
 * </p>
 */
public abstract class BlockHash implements Releasable, SeenGroupIds {

    protected final BlockFactory blockFactory;

    BlockHash(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    /**
     * Add all values for the "group by" columns in the page to the hash and
     * pass the ordinals to the provided {@link GroupingAggregatorFunction.AddInput}.
     * <p>
     *     This call will not {@link GroupingAggregatorFunction.AddInput#close} {@code addInput}.
     * </p>
     */
    public abstract void add(Page page, GroupingAggregatorFunction.AddInput addInput);

    /**
     * Lookup all values for the "group by" columns in the page to the hash and return an
     * {@link Iterator} of the values. The sum of {@link IntBlock#getPositionCount} for
     * all blocks returned by the iterator will equal {@link Page#getPositionCount} but
     * will "target" a size of {@code targetBlockSize}.
     * <p>
     *     The returned {@link ReleasableIterator} may retain a reference to {@link Block}s
     *     inside the {@link Page}. Close it to release those references.
     * </p>
     */
    public abstract ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize);

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
     * @param isCategorize Whether this group is a CATEGORIZE() or not.
     *                     May be changed in the future when more stateful grouping functions are added.
     */
    public record GroupSpec(int channel, ElementType elementType, boolean isCategorize) {
        public GroupSpec(int channel, ElementType elementType) {
            this(channel, elementType, false);
        }
    }

    /**
     * Creates a specialized hash table that maps one or more {@link Block}s to ids.
     * @param emitBatchSize maximum batch size to be emitted when handling combinatorial
     *                      explosion of groups caused by multivalued fields
     * @param allowBrokenOptimizations true to allow optimizations with bad null handling. We will fix their
     *                                 null handling and remove this flag, but we need to disable these in
     *                                 production until we can. And this lets us continue to compile and
     *                                 test them.
     */
    public static BlockHash build(List<GroupSpec> groups, BlockFactory blockFactory, int emitBatchSize, boolean allowBrokenOptimizations) {
        if (groups.size() == 1) {
            return newForElementType(groups.get(0).channel(), groups.get(0).elementType(), blockFactory);
        }
        if (groups.stream().allMatch(g -> g.elementType == ElementType.BYTES_REF)) {
            switch (groups.size()) {
                case 2:
                    return new BytesRef2BlockHash(blockFactory, groups.get(0).channel, groups.get(1).channel, emitBatchSize);
                case 3:
                    return new BytesRef3BlockHash(
                        blockFactory,
                        groups.get(0).channel,
                        groups.get(1).channel,
                        groups.get(2).channel,
                        emitBatchSize
                    );
            }
        }
        if (allowBrokenOptimizations && groups.size() == 2) {
            var g1 = groups.get(0);
            var g2 = groups.get(1);
            if (g1.elementType() == ElementType.LONG && g2.elementType() == ElementType.LONG) {
                return new LongLongBlockHash(blockFactory, g1.channel(), g2.channel(), emitBatchSize);
            }
            if (g1.elementType() == ElementType.BYTES_REF && g2.elementType() == ElementType.LONG) {
                return new BytesRefLongBlockHash(blockFactory, g1.channel(), g2.channel(), false, emitBatchSize);
            }
            if (g1.elementType() == ElementType.LONG && g2.elementType() == ElementType.BYTES_REF) {
                return new BytesRefLongBlockHash(blockFactory, g2.channel(), g1.channel(), true, emitBatchSize);
            }
        }
        return new PackedValuesBlockHash(groups, blockFactory, emitBatchSize);
    }

    /**
     * Temporary method to build a {@link PackedValuesBlockHash}.
     */
    public static BlockHash buildPackedValuesBlockHash(List<GroupSpec> groups, BlockFactory blockFactory, int emitBatchSize) {
        return new PackedValuesBlockHash(groups, blockFactory, emitBatchSize);
    }

    /**
     * Builds a BlockHash for the Categorize grouping function.
     */
    public static BlockHash buildCategorizeBlockHash(
        List<GroupSpec> groups,
        AggregatorMode aggregatorMode,
        BlockFactory blockFactory,
        AnalysisRegistry analysisRegistry
    ) {
        if (groups.size() != 1) {
            throw new IllegalArgumentException("only a single CATEGORIZE group can used");
        }

        return new CategorizeBlockHash(blockFactory, groups.get(0).channel, aggregatorMode, analysisRegistry);
    }

    /**
     * Creates a specialized hash table that maps a {@link Block} of the given input element type to ids.
     */
    private static BlockHash newForElementType(int channel, ElementType type, BlockFactory blockFactory) {
        return switch (type) {
            case NULL -> new NullBlockHash(channel, blockFactory);
            case BOOLEAN -> new BooleanBlockHash(channel, blockFactory);
            case INT -> new IntBlockHash(channel, blockFactory);
            case LONG -> new LongBlockHash(channel, blockFactory);
            case DOUBLE -> new DoubleBlockHash(channel, blockFactory);
            case BYTES_REF -> new BytesRefBlockHash(channel, blockFactory);
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
