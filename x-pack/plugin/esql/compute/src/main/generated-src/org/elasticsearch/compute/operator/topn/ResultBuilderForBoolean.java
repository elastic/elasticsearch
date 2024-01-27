/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;

/**
 * Builds the resulting {@link BooleanBlock} for some column in a top-n.
 * This class is generated. Edit {@code X-ResultBuilder.java.st} instead.
 */
class ResultBuilderForBoolean implements ResultBuilder {
    private final BooleanBlock.Builder builder;

    private final boolean inKey;

    /**
     * The value previously set by {@link #decodeKey}.
     */
    private boolean key;

    ResultBuilderForBoolean(BlockFactory blockFactory, TopNEncoder encoder, boolean inKey, int initialSize) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE : encoder.toString();
        this.inKey = inKey;
        this.builder = blockFactory.newBooleanBlockBuilder(initialSize);
    }

    @Override
    public void decodeKey(BytesRef keys) {
        assert inKey;
        key = TopNEncoder.DEFAULT_SORTABLE.decodeBoolean(keys);
    }

    @Override
    public void decodeValue(BytesRef values) {
        int count = TopNEncoder.DEFAULT_UNSORTABLE.decodeVInt(values);
        switch (count) {
            case 0 -> {
                builder.appendNull();
            }
            case 1 -> builder.appendBoolean(inKey ? key : readValueFromValues(values));
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < count; i++) {
                    builder.appendBoolean(readValueFromValues(values));
                }
                builder.endPositionEntry();
            }
        }
    }

    private boolean readValueFromValues(BytesRef values) {
        return TopNEncoder.DEFAULT_UNSORTABLE.decodeBoolean(values);
    }

    @Override
    public BooleanBlock build() {
        return builder.build();
    }

    @Override
    public String toString() {
        return "ResultBuilderForBoolean[inKey=" + inKey + "]";
    }

    @Override
    public void close() {
        builder.close();
    }
}
