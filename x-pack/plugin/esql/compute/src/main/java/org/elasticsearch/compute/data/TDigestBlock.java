/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;

public sealed interface TDigestBlock extends HistogramBlock permits ConstantNullBlock, TDigestArrayBlock {

    static boolean equals(TDigestBlock blockA, TDigestBlock blockB) {
        if (blockA == blockB) {
            return true;
        }
        return switch (blockA) {
            case null -> false;
            case ConstantNullBlock a -> a.equals(blockB);
            case TDigestArrayBlock a -> switch (blockB) {
                case null -> false;
                case ConstantNullBlock b -> b.equals(a);
                case TDigestArrayBlock b -> a.equalsAfterTypeCheck(b);
            };
        };
    }

    void serializeTDigest(int valueIndex, SerializedTDigestOutput out, BytesRef scratch);

    /**
     * Builder for {@link TDigestBlock}
     */
    sealed interface Builder extends Block.Builder, BlockLoader.TDigestBuilder permits TDigestBlockBuilder {

        /**
         * Copy the values in {@code block} from the given positon into this builder.
         */
        TDigestBlock.Builder copyFrom(TDigestBlock block, int position);

        @Override
        TDigestBlock build();
    }

    TDigestHolder getTDigestHolder(int offset);

    interface SerializedTDigestOutput {
        void appendDouble(double value);

        void appendLong(long value);

        void appendBytesRef(BytesRef bytesRef);
    }

    interface SerializedTDigestInput {
        double readDouble();

        long readLong();

        BytesRef readBytesRef(BytesRef scratch);
    }

}
