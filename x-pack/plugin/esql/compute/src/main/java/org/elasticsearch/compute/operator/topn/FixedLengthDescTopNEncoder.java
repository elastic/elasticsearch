/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

class FixedLengthDescTopNEncoder extends SortableDescTopNEncoder {
    private final int length;
    private final FixedLengthAscTopNEncoder ascEncoder;

    FixedLengthDescTopNEncoder(int length, FixedLengthAscTopNEncoder ascEncoder) {
        this.length = length;
        this.ascEncoder = ascEncoder;
    }

    @Override
    public void encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        if (value.length != length) {
            throw new IllegalArgumentException("expected exactly [" + length + "] bytes but got [" + value.length + "]");
        }
        int startOffset = bytesRefBuilder.length();
        bytesRefBuilder.append(value);
        bitwiseNot(bytesRefBuilder.bytes(), startOffset, bytesRefBuilder.length());
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
        bitwiseNot(scratch.bytes, scratch.offset, scratch.offset + length);
        return scratch;
    }

    @Override
    public String toString() {
        return "FixedLengthDesc[" + length + "]";
    }

    @Override
    public TopNEncoder toSortable(boolean asc) {
        return asc ? ascEncoder : this;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }
}
