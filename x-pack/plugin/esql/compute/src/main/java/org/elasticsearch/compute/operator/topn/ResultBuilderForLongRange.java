/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;

public class ResultBuilderForLongRange implements ResultBuilder {

    private final LongRangeBlockBuilder builder;

    ResultBuilderForLongRange(BlockFactory blockFactory, int positions) {
        this.builder = blockFactory.newLongRangeBlockBuilder(positions);
    }

    @Override
    public void decodeKey(PagedBytesCursor keys, boolean asc) {
        throw new AssertionError("LongRangeBlock can't be a key");
    }

    @Override
    public void decodeValue(PagedBytesCursor cursor) {
        int count = cursor.readVInt();
        if (count == 0) {
            builder.appendNull();
        } else {
            if (TopNEncoder.DEFAULT_UNSORTABLE.decodeBoolean(cursor)) {
                builder.from().appendLong(TopNEncoder.DEFAULT_UNSORTABLE.decodeLong(cursor));
            } else {
                builder.from().appendNull();
            }
            if (TopNEncoder.DEFAULT_UNSORTABLE.decodeBoolean(cursor)) {
                builder.to().appendLong(TopNEncoder.DEFAULT_UNSORTABLE.decodeLong(cursor));
            } else {
                builder.to().appendNull();
            }
        }
    }

    @Override
    public void appendNull() {
        builder.appendNull();
    }

    @Override
    public void appendFromKey() {
        throw new AssertionError("LongRangeBlock can't be a key");
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
    public String toString() {
        return "ResultBuilderForLongRange";
    }

    @Override
    public void close() {
        builder.close();
    }
}
