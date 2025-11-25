/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoader;

public sealed interface TDigestBlock extends Block permits ConstantNullBlock, TDigestArrayBlock {

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

    TDigestHolder getTDigestHolder(int offset, BytesRef scratch);
}
