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
}
