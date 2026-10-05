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
import org.elasticsearch.compute.data.DocRefBlock;

import java.util.Arrays;

/**
 * Builds a {@link DocRefBlock} from rows {@link ValueExtractorForDocRef} wrote. The block's dictionary holds only the
 * origins of the rows in the block.
 */
class ResultBuilderForDocRef implements ResultBuilder {
    private final DocRefEncoder encoder;
    private final DocRefBlock.Builder builder;
    // registry ordinal to block ordinal, -1 until the first row with that origin
    private int[] blockOrdinals = new int[0];

    ResultBuilderForDocRef(BlockFactory blockFactory, TopNEncoder encoder, int positions) {
        this.encoder = (DocRefEncoder) encoder;
        this.builder = DocRefBlock.newBlockBuilder(blockFactory, positions);
    }

    @Override
    public void decodeKey(BytesRef keys, boolean asc) {
        throw new AssertionError("document references can't be a key");
    }

    @Override
    public void decodeValue(BytesRef values) {
        int registryOrdinal = encoder.decodeVInt(values);
        int segment = encoder.decodeInt(values);
        int doc = encoder.decodeInt(values);
        if (registryOrdinal >= blockOrdinals.length) {
            int oldLength = blockOrdinals.length;
            blockOrdinals = Arrays.copyOf(blockOrdinals, Math.max(registryOrdinal + 1, oldLength * 2));
            Arrays.fill(blockOrdinals, oldLength, blockOrdinals.length, -1);
        }
        int blockOrdinal = blockOrdinals[registryOrdinal];
        if (blockOrdinal < 0) {
            blockOrdinal = builder.addOrigin(encoder.origin(registryOrdinal));
            blockOrdinals[registryOrdinal] = blockOrdinal;
        }
        builder.append(blockOrdinal, segment, doc);
    }

    @Override
    public Block build() {
        // the rows of a TopN can repeat a document, as they can for _doc
        return builder.mayContainDuplicates(true).build();
    }

    @Override
    public long estimatedBytes() {
        return builder.estimatedBytes();
    }

    @Override
    public String toString() {
        return "ResultBuilderForDocRef";
    }

    @Override
    public void close() {
        builder.close();
    }
}
