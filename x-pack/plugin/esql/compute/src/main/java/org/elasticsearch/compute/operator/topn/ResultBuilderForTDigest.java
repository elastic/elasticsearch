/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestBlockBuilder;

public class ResultBuilderForTDigest implements ResultBuilder {
    private final TDigestBlockBuilder builder;
    private final ReusableTopNEncoderCursorInput reusableCursorInput = new ReusableTopNEncoderCursorInput();

    ResultBuilderForTDigest(BlockFactory blockFactory, int positions) {
        this.builder = blockFactory.newTDigestBlockBuilder(positions);
    }

    @Override
    public void decodeKey(PagedBytesCursor keys, boolean asc) {
        throw new AssertionError("TDigest can't be a key");
    }

    @Override
    public void decodeValue(PagedBytesCursor cursor) {
        int count = cursor.readVInt();
        if (count == 0) {
            builder.appendNull();
            return;
        }
        assert count == 1 : "TDigest does not support multi values";
        reusableCursorInput.cursor = cursor;
        builder.deserializeAndAppend(reusableCursorInput);
    }

    @Override
    public void appendNull() {
        builder.appendNull();
    }

    @Override
    public void appendFromKey() {
        throw new AssertionError("TDigest can't be a key");
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
        return "ResultBuilderForExponentialHistogram";
    }

    @Override
    public void close() {
        builder.close();
    }

    private static final class ReusableTopNEncoderCursorInput implements TDigestBlock.SerializedTDigestInput {
        PagedBytesCursor cursor;

        @Override
        public double readDouble() {
            return TopNEncoder.DEFAULT_UNSORTABLE.decodeDouble(cursor);
        }

        @Override
        public long readLong() {
            return TopNEncoder.DEFAULT_UNSORTABLE.decodeLong(cursor);
        }

        @Override
        public BytesRef readBytesRef(BytesRef scratch) {
            return cursor.readBytesRef(cursor.readVInt(), scratch);
        }
    }
}
