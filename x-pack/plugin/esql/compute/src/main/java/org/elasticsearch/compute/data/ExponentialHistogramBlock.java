/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

public sealed interface ExponentialHistogramBlock extends Block permits ConstantNullBlock, ExponentialHistogramArrayBlock {

    /**
     * Returns the exponential histogram at the given value index.
     * Note that this method allocates. So when accessing many values, prefer to use {@link #accessor()} instead.
     *
     * @param valueIndex the index of the histogram to retrieve
     * @return the histogram at the given index
     */
    ExponentialHistogram getExponentialHistogram(int valueIndex);

    /**
     * Provides access to exponential histograms minimizing heap allocations.
     *
     * @return the accessor for this block, only valid as long as the block is not closed.
     */
    Accessor accessor();

    interface Accessor {
        /**
         *
         * Returns the exponential histogram at the given value index.
         * This method reuses the returned histogram instance to avoid allocations.
         * Therefore, the return value is only valid until the next call to this method.
         *
         * @param valueIndex the index of the histogram to retrieve
         * @return the histogram at the given index
         */
        ExponentialHistogram getExponentialHistogram(int valueIndex);
    }

    static boolean equals(ExponentialHistogramBlock blockA, ExponentialHistogramBlock blockB) {
        if (blockA == blockB) {
            return true;
        }
        return switch (blockA) {
            case null -> false;
            case ConstantNullBlock a -> a.equals(blockB);
            case ExponentialHistogramArrayBlock a -> switch (blockB) {
                case null -> false;
                case ConstantNullBlock b -> b.equals(a);
                case ExponentialHistogramArrayBlock b -> a.equalsAfterTypeCheck(b);
            };
        };
    }

}
