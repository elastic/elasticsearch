/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

/**
 * A block that holds {@link ExponentialHistogram} values.
 * Position access is done through {@link ExponentialHistogramBlockAccessor}.
 */
public sealed interface ExponentialHistogramBlock extends Block permits ConstantNullBlock, ExponentialHistogramArrayBlock {

    /**
     * Abstraction to use for writing individual values via {@link ExponentialHistogramBlockAccessor#serializeValue(int, SerializedOutput)}.
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

        BytesRef readBytesRef(BytesRef tempBytesRef);
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
