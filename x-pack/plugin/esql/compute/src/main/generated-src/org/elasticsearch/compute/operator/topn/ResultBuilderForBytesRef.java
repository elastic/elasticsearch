/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;

/**
 * Builds the resulting {@link BytesRefBlock} for some column in a top-n.
 * This class is generated. Edit {@code X-ResultBuilder.java.st} instead.
 */
class ResultBuilderForBytesRef implements ResultBuilder {
    private final BytesRefBlock.Builder builder;

    private final boolean inKey;

    private final TopNEncoder encoder;

    private final PagedBytesCursor scratch = new PagedBytesCursor();

    /**
     * The value previously set by {@link #decodeKey}.
     */
    private PagedBytesCursor key;

    ResultBuilderForBytesRef(BlockFactory blockFactory, TopNEncoder encoder, boolean inKey, int initialSize) {
        this.encoder = encoder;
        this.inKey = inKey;
        this.builder = blockFactory.newBytesRefBlockBuilder(initialSize);
    }

    @Override
    public void decodeKey(PagedBytesCursor keys, boolean asc) {
        assert inKey;
        key = encoder.toSortable(asc).decodeBytesRef(keys, scratch);
    }

    @Override
    public void decodeValue(PagedBytesCursor cursor) {
        int count = cursor.readVInt();
        switch (count) {
            case 0 -> {
                builder.appendNull();
            }
            case 1 -> builder.append(inKey ? key : readValueFromValues(cursor));
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < count; i++) {
                    builder.append(readValueFromValues(cursor));
                }
                builder.endPositionEntry();
            }
        }
    }

    private PagedBytesCursor readValueFromValues(PagedBytesCursor cursor) {
        return encoder.toUnsortable().decodeBytesRef(cursor, scratch);
    }

    @Override
    public void appendNull() {
        builder.appendNull();
    }

    @Override
    public void appendFromKey() {
        builder.append(key);
    }

    @Override
    public BytesRefBlock build() {
        return builder.build();
    }

    @Override
    public long estimatedBytes() {
        return builder.estimatedBytes();
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
