/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;

class FixedLengthDescTopNEncoder extends SortableDescTopNEncoder {
    private final int length;
    private final FixedLengthAscTopNEncoder ascEncoder;

    FixedLengthDescTopNEncoder(int length, FixedLengthAscTopNEncoder ascEncoder) {
        this.length = length;
        this.ascEncoder = ascEncoder;
    }

    @Override
    public void encodeBytesRef(BytesRef value, PagedBytesBuilder builder) {
        if (value.length != length) {
            throw new IllegalArgumentException("expected exactly [" + length + "] bytes but got [" + value.length + "]");
        }
        builder.appendNot(value.bytes, value.offset, value.length);
    }

    @Override
    public PagedBytesCursor decodeBytesRef(PagedBytesCursor cursor, PagedBytesCursor scratch) {
        cursor.slice(length, scratch);
        scratch.bitwiseNot();
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
