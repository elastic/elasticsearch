/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.util.List;

/**
 * A Block is a columnar representation of homogenous data. It has a position (row) count, and
 * various data retrieval methods for accessing the underlying data that is stored at a given
 * position.
 *
 * <p> Blocks can represent various shapes of underlying data. A Block can represent either sparse
 * or dense data. A Block can represent either single or multi valued data. A Block that represents
 * dense single-valued data can be viewed as a {@link Vector}.
 *
 * TODO: update comment
 * <p> All Blocks share the same set of data retrieval methods, but actual concrete implementations
 * effectively support a subset of these, throwing {@code UnsupportedOperationException} where a
 * particular data retrieval method is not supported. For example, a Block of primitive longs may
 * not support retrieval as an integer, {code getInt}. This greatly simplifies Block usage and
 * avoids cumbersome use-site casting.
 *
 * <p> Block are immutable and can be passed between threads.
 */
public interface Block extends NamedWriteable {

    /**
     * {@return an efficient dense single-value view of this block}.
     * Null, if the block is not dense single-valued. That is, if
     * mayHaveNulls returns true, or getTotalValueCount is not equal to getPositionCount.
     */
    Vector asVector();

    /** {@return The total number of values in this block not counting nulls.} */
    int getTotalValueCount();

    /** {@return The number of positions in this block.} */
    int getPositionCount();

    /** Gets the index of the first value for the given position. */
    int getFirstValueIndex(int position);

    /** Gets the number of values for the given position, possibly 0. */
    int getValueCount(int position);

    /**
     * {@return the element type of this block}
     */
    ElementType elementType();

    /**
     * Returns true if the value stored at the given position is null, false otherwise.
     *
     * @param position the position
     * @return true or false
     */
    boolean isNull(int position);

    /**
     * @return the number of null values in this block.
     */
    int nullValuesCount();

    /**
     * @return true if some values might be null. False, if all values are guaranteed to be not null.
     */
    boolean mayHaveNulls();

    /**
     * @return true if all values in this block are guaranteed to be null.
     */
    boolean areAllValuesNull();

    /**
     * Can this block have multivalued fields? Blocks that return {@code false}
     * will never return more than one from {@link #getValueCount}.
     */
    boolean mayHaveMultivaluedFields();

    /**
     * Creates a new block that only exposes the positions provided. Materialization of the selected positions is avoided.
     * @param positions the positions to retain
     * @return a filtered block
     */
    Block filter(int... positions);

    /**
     * How are multivalued fields ordered?
     * <p>Note that there isn't a {@code DESCENDING} because we don't have
     * anything that makes descending fields.</p>
     */
    enum MvOrdering {
        ASCENDING,
        UNORDERED;
    }

    /**
     * How are multivalued fields ordered?
     */
    MvOrdering mvOrdering();

    /**
     * Expand multivalued fields into one row per value. Returns the
     * block if there aren't any multivalued fields to expand.
     */
    Block expand();

    /**
     * {@return a constant null block with the given number of positions}.
     */
    static Block constantNullBlock(int positions) {
        return new ConstantNullBlock(positions);
    }

    interface Builder {

        /**
         * Appends a null value to the block.
         */
        Builder appendNull();

        /**
         * Begins a multivalued entry. Calling this for the first time will put
         * the builder into a mode that generates Blocks that return {@code true}
         * from {@link Block#mayHaveMultivaluedFields} which can force less
         * optimized code paths. So don't call this unless you are sure you are
         * emitting more than one value for this position.
         */
        Builder beginPositionEntry();

        /**
         * Ends the current multi-value entry.
         */
        Builder endPositionEntry();

        /**
         * Appends the all values of the given block into a the current position
         * in this builder.
         */
        Builder appendAllValuesToCurrentPosition(Block block);

        /**
         * Copy the values in {@code block} from {@code beginInclusive} to
         * {@code endExclusive} into this builder.
         */
        Builder copyFrom(Block block, int beginInclusive, int endExclusive);

        /**
         * How are multivalued fields ordered? This defaults to {@link Block.MvOrdering#UNORDERED}
         * but when you set it to {@link Block.MvOrdering#ASCENDING} some operators can optimize
         * themselves. This is a <strong>promise</strong> that is never checked. If you set this
         * to anything other than {@link Block.MvOrdering#UNORDERED} be sure the values are in
         * that order or other operators will make mistakes. The actual ordering isn't checked
         * at runtime.
         */
        Builder mvOrdering(Block.MvOrdering mvOrdering);

        /**
         * Builds the block. This method can be called multiple times.
         */
        Block build();
    }

    static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            IntBlock.ENTRY,
            LongBlock.ENTRY,
            DoubleBlock.ENTRY,
            BytesRefBlock.ENTRY,
            BooleanBlock.ENTRY,
            IntVectorBlock.ENTRY,
            LongVectorBlock.ENTRY,
            DoubleVectorBlock.ENTRY,
            BytesRefVectorBlock.ENTRY,
            BooleanVectorBlock.ENTRY,
            ConstantNullBlock.ENTRY,
            AggregatorStateBlock.ENTRY
        );
    }
}
