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
import org.elasticsearch.compute.data.PackDimBlock;

/** Reconstructs sparse sort payloads, preserving the distinction between null and an empty record. */
final class ResultBuilderForPackDim implements ResultBuilder {
    private final PackDimBlock.Builder builder;

    ResultBuilderForPackDim(BlockFactory factory, int positions) {
        builder = factory.newPackDimBlockBuilder(positions);
    }

    @Override
    public void decodeKey(BytesRef keys, boolean asc) {
        throw new UnsupportedOperationException("packed dimensions are not sortable keys");
    }

    @Override
    public void decodeValue(BytesRef input) {
        var encoder = TopNEncoder.DEFAULT_UNSORTABLE;
        if (encoder.decodeVInt(input) == 0) {
            builder.appendNull();
            return;
        }
        int count = encoder.decodeInt(input);
        BytesRef[] names = new BytesRef[count];
        BytesRef[] values = new BytesRef[count];
        for (int i = 0; i < count; i++) {
            names[i] = encoder.decodeBytesRef(input, new BytesRef());
            values[i] = encoder.decodeBytesRef(input, new BytesRef());
        }
        builder.append(names, values);
    }

    @Override
    public Block build() {
        return builder.build();
    }

    @Override
    public long estimatedBytes() {
        return builder.estimatedBytes();
    }

    @Override
    public void close() {
        builder.close();
    }
}
