/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

public interface HistogramBlock extends Block {

    /**
     * Exponential histograms are composite data types. This enum defines the components
     * that can be directly accessed, potentially avoiding loading the entire histogram from disk.
     * <br>
     * This enum can be safely serialized via {@link org.elasticsearch.common.io.stream.StreamOutput#writeEnum(Enum)}.
     */
    enum Component {
        /**
         * The minimum of all values summarized by the histogram, null if the histogram is empty.
         */
        MIN,

        /**
         * The maximum of all values summarized by the histogram, null if the histogram is empty.
         */
        MAX,

        /**
         * The sum of all values summarized by the histogram, 0.0 if the histogram is empty.
         */
        SUM,

        /**
         * The number of all values summarized by the histogram, 0 if the histogram is empty.
         */
        COUNT
    }

    /**
     * Returns a block holding the specified component of the histogram at each position.
     * The number of positions in the returned block will be exactly equal to the number of positions in this block.
     * If a position is null in this block, it will also be null in the returned block.
     * <br>
     * The caller is responsible for closing the returned block.
     *
     * @param component the component to extract
     * @return the block containing the specified component
     */
    DoubleBlock buildHistogramComponentBlock(Component component);

    /**
     * Returns the block holding the minimum of each histogram, null for empty or null histograms.
     * <p>
     * This is the block backing this histogram block: it is neither copied nor ref-counted on behalf of the caller, which must
     * not close it and must not use it after this block is closed.
     * </p>
     * <p>
     * The returned block is indexed by <em>value index</em>, not by position: the component of the histogram at value index
     * {@code i}, as given by {@link #getFirstValueIndex(int)}, is at index {@code i}. Its position count therefore only
     * equals the position count of this block if there are no multi-valued positions.
     * </p>
     */
    DoubleBlock getMinBlock();

    /** Returns the block holding the maximum of each histogram, with the same semantics as {@link #getMinBlock()}. */
    DoubleBlock getMaxBlock();

    /** Returns the block holding the sum of each histogram, with the same semantics as {@link #getMinBlock()}. */
    DoubleBlock getSumBlock();

    /**
     * Returns the block holding the number of values summarized by each histogram, with the same ownership semantics as
     * {@link #getMinBlock()}. Its type depends on the kind of histogram, hence the generic return type that subinterfaces refine.
     */
    Block getCountBlock();
}
