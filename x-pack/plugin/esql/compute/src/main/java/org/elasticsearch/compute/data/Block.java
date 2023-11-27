/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.util.List;

/**
 * A Block is a columnar representation of homogenous data. It has a position (row) count, and
 * various data retrieval methods for accessing the underlying data that is stored at a given
 * position.
 *
 * <p> Blocks can represent various shapes of underlying data. A Block can represent either sparse
 * or dense data. A Block can represent either single or multivalued data. A Block that represents
 * dense single-valued data can be viewed as a {@link Vector}.
 *
 * <p> Blocks are reference counted; to make a shallow copy of a block (e.g. if a {@link Page} contains
 * the same column twice), use {@link Block#incRef()}. Before a block is garbage collected,
 * {@link Block#close()} must be called to release a block's resources; it must also be called one
 * additional time for each time {@link Block#incRef()} was called. Calls to {@link Block#decRef()} and
 * {@link Block#close()} are equivalent.
 *
 * <p> Block are immutable and can be passed between threads as long as no two threads hold a reference to
 * the same block at the same time.
 */
public interface Block extends Accountable, BlockLoader.Block, NamedWriteable, RefCounted, Releasable {

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

    /** The block factory associated with this block. */
    BlockFactory blockFactory();

    /**
     * Tells if this block has been released. A block is released by calling its {@link Block#close()} or {@link Block#decRef()} methods.
     * @return true iff the block's reference count is zero.
     * */
    boolean isReleased();

    /**
     * @param position the position
     * @return true if the value stored at the given position is null, false otherwise
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
     * The new block may hold a reference to this block, increasing this block's reference count.
     * @param positions the positions to retain
     * @return a filtered block
     */
    Block filter(int... positions);

    /**
     * How are multivalued fields ordered?
     * Some operators can enable its optimization when mv_values are sorted ascending or de-duplicated.
     */
    enum MvOrdering {
        UNORDERED(false, false),
        DEDUPLICATED_UNORDERD(true, false),
        DEDUPLICATED_AND_SORTED_ASCENDING(true, true);

        private final boolean deduplicated;
        private final boolean sortedAscending;

        MvOrdering(boolean deduplicated, boolean sortedAscending) {
            this.deduplicated = deduplicated;
            this.sortedAscending = sortedAscending;
        }
    }

    /**
     * How are multivalued fields ordered?
     */
    MvOrdering mvOrdering();

    /**
     * Are multivalued fields de-duplicated in each position
     */
    default boolean mvDeduplicated() {
        return mayHaveMultivaluedFields() == false || mvOrdering().deduplicated;
    }

    /**
     * Are multivalued fields sorted ascending in each position
     */
    default boolean mvSortedAscending() {
        return mayHaveMultivaluedFields() == false || mvOrdering().sortedAscending;
    }

    /**
     * Expand multivalued fields into one row per value. Returns the
     * block if there aren't any multivalued fields to expand.
     */
    // TODO: We should use refcounting instead of either deep copies or returning the same identical block.
    Block expand();

    /**
     * {@return a constant null block with the given number of positions, using the non-breaking block factory}.
     * @deprecated use {@link BlockFactory#newConstantNullBlock}
     */
    // Eventually, this should use the GLOBAL breaking instance
    @Deprecated
    static Block constantNullBlock(int positions) {
        return constantNullBlock(positions, BlockFactory.getNonBreakingInstance());
    }

    /**
     * {@return a constant null block with the given number of positions}.
     * @deprecated use {@link BlockFactory#newConstantNullBlock}
     */
    @Deprecated
    static Block constantNullBlock(int positions, BlockFactory blockFactory) {
        return blockFactory.newConstantNullBlock(positions);
    }

    /**
     * Builds {@link Block}s. Typically, you use one of it's direct supinterfaces like {@link IntBlock.Builder}.
     * This is {@link Releasable} and should be released after building the block or if building the block fails.
     */
    interface Builder extends BlockLoader.Builder, Releasable {

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
         * but when you set it to {@link Block.MvOrdering#DEDUPLICATED_AND_SORTED_ASCENDING} some operators can optimize
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

        /**
         * Build many {@link Block}s at once, releasing any partially built blocks
         * if any fail.
         */
        static Block[] buildAll(Block.Builder... builders) {
            Block[] blocks = new Block[builders.length];
            try {
                for (int b = 0; b < blocks.length; b++) {
                    blocks[b] = builders[b].build();
                }
            } finally {
                if (blocks[blocks.length - 1] == null) {
                    Releasables.closeExpectNoException(blocks);
                }
            }
            return blocks;
        }
    }

    static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            IntBlock.ENTRY,
            LongBlock.ENTRY,
            DoubleBlock.ENTRY,
            BytesRefBlock.ENTRY,
            BooleanBlock.ENTRY,
            ConstantNullBlock.ENTRY
        );
    }
}
