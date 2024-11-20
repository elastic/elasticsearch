/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

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
     * The maximum number of values that can be added to one position via lookup.
     * TODO maybe make this everywhere?
     */
    long MAX_LOOKUP = 100_000;

    /**
     * We do not track memory for pages directly (only for single blocks),
     * but the page memory overhead can still be significant, especially for pages containing thousands of blocks.
     * For now, we approximate this overhead, per block, using this value.
     *
     * The exact overhead per block would be (more correctly) {@link RamUsageEstimator#NUM_BYTES_OBJECT_REF},
     * but we approximate it with {@link RamUsageEstimator#NUM_BYTES_OBJECT_ALIGNMENT} to avoid further alignments
     * to object size (at the end of the alignment, it would make no practical difference).
     */
    int PAGE_MEM_OVERHEAD_PER_BLOCK = RamUsageEstimator.NUM_BYTES_OBJECT_ALIGNMENT;

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
    // TODO: renaming this to owning blockFactory once we pass blockFactory for filter and expand
    BlockFactory blockFactory();

    /**
     * Before passing a Block to another Driver, it is necessary to switch the owning block factory to its parent, which is associated
     * with the global circuit breaker. This ensures that when the new driver releases this Block, it returns memory directly to the
     * parent block factory instead of the local block factory of this Block. This is important because the local block factory is
     * not thread safe and doesn't support simultaneous access by more than one thread.
     */
    void allowPassingToDifferentDriver();

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
     * @return true if some values might be null. False, if all values are guaranteed to be not null.
     */
    boolean mayHaveNulls();

    /**
     * @return true if all values in this block are guaranteed to be null.
     */
    boolean areAllValuesNull();

    /**
     * Can this block have multivalued fields? Blocks that return {@code false}
     * will never return more than one from {@link #getValueCount}. This may
     * return {@code true} for Blocks that do not have multivalued fields, but
     * it will always answer quickly.
     */
    boolean mayHaveMultivaluedFields();

    /**
     * Does this block have multivalued fields? Unlike {@link #mayHaveMultivaluedFields}
     * this will never return a false positive. In other words, if this returns
     * {@code true} then there <strong>are</strong> positions for which {@link #getValueCount}
     * will return more than 1. This will answer quickly if it can but may have
     * to check all positions.
     */
    boolean doesHaveMultivaluedFields();

    /**
     * Creates a new block that only exposes the positions provided.
     * @param positions the positions to retain
     * @return a filtered block
     * TODO: pass BlockFactory
     */
    Block filter(int... positions);

    /**
     * Build a {@link Block} with the same values as this {@linkplain Block}, but replacing
     * all values for which {@code mask.getBooleanValue(position)} returns
     * {@code false} with {@code null}. The {@code mask} vector must be at least
     * as long as this {@linkplain Block}.
     */
    Block keepMask(BooleanVector mask);

    /**
     * Builds an Iterator of new {@link Block}s with the same {@link #elementType}
     * as this Block whose values are copied from positions in this Block. It has the
     * same number of {@link #getPositionCount() positions} as the {@code positions}
     * parameter.
     * <p>
     *     For example, if this block contained {@code [a, b, [b, c]]}
     *     and were called with the block {@code [0, 1, 1, [1, 2]]} then the
     *     result would be {@code [a, b, b, [b, b, c]]}.
     * </p>
     * <p>
     *     This process produces {@code count(this) * count(positions)} values per
     *     positions which could be quite large. Instead of returning a single
     *     Block, this returns an Iterator of Blocks containing all of the promised
     *     values.
     * </p>
     * <p>
     *     The returned {@link ReleasableIterator} may retain a reference to the
     *     {@code positions} parameter. Close it to release those references.
     * </p>
     * <p>
     *     This block is built using the same {@link BlockFactory} as was used to
     *     build the {@code positions} parameter.
     * </p>
     */
    ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize);

    /**
     * How are multivalued fields ordered?
     * Some operators can enable its optimization when mv_values are sorted ascending or de-duplicated.
     */
    enum MvOrdering {
        UNORDERED(false, false),
        DEDUPLICATED_UNORDERD(true, false),
        DEDUPLICATED_AND_SORTED_ASCENDING(true, true),
        SORTED_ASCENDING(false, true);

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
     * Expand multivalued fields into one row per value. Returns the same block if there aren't any multivalued
     * fields to expand. The returned block needs to be closed by the caller to release the block's resources.
     * TODO: pass BlockFactory
     */
    Block expand();

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
         * An estimate of the number of bytes the {@link Block} created by
         * {@link #build} will use. This may overestimate the size but shouldn't
         * underestimate it.
         */
        long estimatedBytes();

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

    /**
     * Serialization type for blocks: 0 and 1 replace false/true used in pre-8.14
     */
    byte SERIALIZE_BLOCK_VALUES = 0;
    byte SERIALIZE_BLOCK_VECTOR = 1;
    byte SERIALIZE_BLOCK_ARRAY = 2;
    byte SERIALIZE_BLOCK_BIG_ARRAY = 3;
    byte SERIALIZE_BLOCK_ORDINAL = 3;
}
