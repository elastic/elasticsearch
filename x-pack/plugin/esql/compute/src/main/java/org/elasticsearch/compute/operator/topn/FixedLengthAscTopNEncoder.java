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

class FixedLengthAscTopNEncoder extends SortableAscTopNEncoder {
    private final int length;
    private final FixedLengthDescTopNEncoder descEncoder;

    FixedLengthAscTopNEncoder(int length) {
        this.length = length;
        this.descEncoder = new FixedLengthDescTopNEncoder(length, this);
    }

    @Override
    public void encodeBytesRef(BytesRef value, PagedBytesBuilder builder) {
        if (value.length != length) {
            throw new IllegalArgumentException("expected exactly [" + length + "] bytes but got [" + value.length + "]");
        }
        builder.append(value);
    }

    @Override
    public PagedBytesCursor decodeBytesRef(PagedBytesCursor cursor, PagedBytesCursor scratch) {
        return cursor.slice(length, scratch);
    }

    @Override
    public String toString() {
        return "FixedLengthAsc[" + length + "]";
    }

    @Override
    public TopNEncoder toSortable(boolean asc) {
        return asc ? this : descEncoder;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }
}
