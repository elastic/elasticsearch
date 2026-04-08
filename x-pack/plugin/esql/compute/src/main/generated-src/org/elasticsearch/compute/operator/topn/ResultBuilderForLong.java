/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;

/**
 * Builds the resulting {@link LongBlock} for some column in a top-n.
 * This class is generated. Edit {@code X-ResultBuilder.java.st} instead.
 */
class ResultBuilderForLong implements ResultBuilder {
    private final LongBlock.Builder builder;

    private final boolean inKey;

    private final TopNEncoder encoder;

    /**
     * The value previously set by {@link #decodeKey}.
     */
    private long key;

    ResultBuilderForLong(BlockFactory blockFactory, TopNEncoder encoder, boolean inKey, int initialSize) {
        this.encoder = encoder;
        this.inKey = inKey;
        this.builder = blockFactory.newLongBlockBuilder(initialSize);
    }

    @Override
    public void decodeKey(PagedBytesCursor keys, boolean asc) {
        assert inKey;
        key = encoder.toSortable(asc).decodeLong(keys);
    }

    @Override
    public void decodeValue(PagedBytesCursor cursor) {
        int count = cursor.readVInt();
        switch (count) {
            case 0 -> {
                builder.appendNull();
            }
            case 1 -> builder.appendLong(inKey ? key : readValueFromValues(cursor));
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < count; i++) {
                    builder.appendLong(readValueFromValues(cursor));
                }
                builder.endPositionEntry();
            }
        }
    }

    private long readValueFromValues(PagedBytesCursor cursor) {
        return TopNEncoder.DEFAULT_UNSORTABLE.decodeLong(cursor);
    }

    @Override
    public void appendNull() {
        builder.appendNull();
    }

    @Override
    public void appendFromKey() {
        builder.appendLong(key);
    }

    @Override
    public LongBlock build() {
        return builder.build();
    }

    @Override
    public long estimatedBytes() {
        return builder.estimatedBytes();
    }

    @Override
    public String toString() {
        return "ResultBuilderForLong[inKey=" + inKey + "]";
    }

    @Override
    public void close() {
        builder.close();
    }
}
