/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;

class ResultBuilderForBytesRef implements ResultBuilder {
    private final BytesRefBlock.Builder builder;

    private final boolean inKey;

    private final TopNEncoder encoder;

    private final BytesRef scratch = new BytesRef();

    /**
     * The value previously set by {@link #decodeKey}.
     */
    private BytesRef key;

    ResultBuilderForBytesRef(BlockFactory blockFactory, TopNEncoder encoder, boolean inKey, int initialSize) {
        this.encoder = encoder;
        this.inKey = inKey;
        this.builder = BytesRefBlock.newBlockBuilder(initialSize, blockFactory);
    }

    @Override
    public void decodeKey(BytesRef keys) {
        assert inKey;
        key = encoder.toSortable().decodeBytesRef(keys, scratch);
    }

    @Override
    public void decodeValue(BytesRef values) {
        int count = TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(values);
        switch (count) {
            case 0 -> {
                builder.appendNull();
            }
            case 1 -> builder.appendBytesRef(inKey ? key : readValueFromValues(values));
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < count; i++) {
                    builder.appendBytesRef(readValueFromValues(values));
                }
                builder.endPositionEntry();
            }
        }
    }

    private BytesRef readValueFromValues(BytesRef values) {
        return encoder.toUnsortable().decodeBytesRef(values, scratch);
    }

    @Override
    public BytesRefBlock build() {
        return builder.build();
    }

    @Override
    public String toString() {
        return "ResultBuilderForBytesRef[inKey=" + inKey + "]";
    }

    @Override
    public void close() {
        builder.close();
    }
}
