/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.index.mapper.BlockLoader;

/**
 * A block that holds {@link ExponentialHistogram} values.
 */
public sealed interface ExponentialHistogramBlock extends Block permits ConstantNullBlock, ExponentialHistogramArrayBlock {

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
     * Returns the {@link ExponentialHistogram} value at the given index.
     * In order to be allocation free, this method requires a scratch object to be passed in,
     * whose memory will be used to hold the state of the returned histogram.
     * Therefore, the return value of this method is only valid until either the block is closed
     * or the same scratch instance is passed to another call to this method on any block.
     *
     * @param valueIndex the index of the histogram to get
     * @param scratch the scratch to use as storage for the returned histogram
     * @return the exponential histogram at the given index
     */
    ExponentialHistogram getExponentialHistogram(int valueIndex, ExponentialHistogramScratch scratch);

    /**
     * Returns a block holding the specified component of the exponential histogram at each position.
     * The number of positions in the returned block will be exactly equal to the number of positions in this block.
     * If a position is null in this block, it will also be null in the returned block.
     * <br>
     * The caller is responsible for closing the returned block.
     *
     * @param component the component to extract
     * @return the block containing the specified component
     */
    Block buildExponentialHistogramComponentBlock(Component component);

    /**
     * Serializes the exponential histogram at the given index into the provided output, so that it can be read back
     *  via {@link ExponentialHistogramBlockBuilder#deserializeAndAppend(SerializedInput)}.
     *
     * @param valueIndex
     * @param out
     * @param scratch
     */
    void serializeExponentialHistogram(int valueIndex, SerializedOutput out, BytesRef scratch);

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

    /**
     * Builder for {@link ExponentialHistogramBlock}
     */
    sealed interface Builder extends Block.Builder, BlockLoader.ExponentialHistogramBuilder permits ExponentialHistogramBlockBuilder {

        /**
         * Copy the values in {@code block} from the given positon into this builder.
         */
        Builder copyFrom(ExponentialHistogramBlock block, int position);

        @Override
        ExponentialHistogramBlock build();
    }

    /**
     * Abstraction to use for writing individual values via {@link #serializeExponentialHistogram(int, SerializedOutput, BytesRef)}.
     */
    interface SerializedOutput {
        void appendDouble(double value);

        void appendLong(long value);

        void appendBytesRef(BytesRef bytesRef);
    }

    /**
     * Abstraction to use for reading individual serialized via
     * {@link ExponentialHistogramBlockBuilder#deserializeAndAppend(SerializedInput)}.
     */
    interface SerializedInput {
        double readDouble();

        long readLong();

        BytesRef readBytesRef(BytesRef scratch);
    }

}
