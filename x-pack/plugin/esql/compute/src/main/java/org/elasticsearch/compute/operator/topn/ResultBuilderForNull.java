/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;

public class ResultBuilderForNull implements ResultBuilder {
    private final BlockFactory blockFactory;
    private int positions;

    public ResultBuilderForNull(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    @Override
    public void decodeKey(BytesRef keys) {
        throw new AssertionError("somehow got a value for a null key");
    }

    @Override
    public void decodeValue(BytesRef values) {
        int size = TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(values);
        if (size != 0) {
            throw new IllegalArgumentException("null columns should always have 0 entries");
        }
        positions++;
    }

    @Override
    public Block build() {
        return blockFactory.newConstantNullBlock(positions);
    }

    @Override
    public String toString() {
        return "ValueExtractorForNull";
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
