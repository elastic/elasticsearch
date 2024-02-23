/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

class FixedLengthTopNEncoder extends SortableTopNEncoder {
    private final int length;

    FixedLengthTopNEncoder(int length) {
        this.length = length;
    }

    @Override
    public int encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        if (value.length != length) {
            throw new IllegalArgumentException("expected exactly [" + length + "] bytes but got [" + value.length + "]");
        }
        bytesRefBuilder.append(value);
        return length;
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        if (bytes.length < length) {
            throw new IllegalArgumentException("expected [" + length + "] bytes but only [" + bytes.length + "] remain");
        }
        scratch.bytes = bytes.bytes;
        scratch.offset = bytes.offset;
        scratch.length = length;
        bytes.offset += length;
        bytes.length -= length;
        return scratch;
    }

    @Override
    public String toString() {
        return "FixedLengthTopNEncoder[" + length + "]";
    }

    @Override
    public TopNEncoder toSortable() {
        return this;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }
}
