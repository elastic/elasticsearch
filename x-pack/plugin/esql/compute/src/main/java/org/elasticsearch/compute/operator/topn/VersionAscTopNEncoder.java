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

class VersionAscTopNEncoder extends SortableAscTopNEncoder {
    private final VersionDescTopNEncoder descEncoder = new VersionDescTopNEncoder(this);

    static void refuseNul(BytesRef value) {
        int end = value.offset + value.length;
        for (int i = value.offset; i < end; i++) {
            if (value.bytes[i] == Utf8AscTopNEncoder.TERMINATOR) {
                throw new IllegalArgumentException("Can't sort versions containing nul");
            }
        }
    }

    @Override
    public void encodeBytesRef(BytesRef value, PagedBytesBuilder builder) {
        // TODO versions can contain nul so we need to delegate to the utf-8 encoder for the utf-8 parts of a version
        refuseNul(value);
        builder.append(value);
        builder.append(Utf8AscTopNEncoder.TERMINATOR);
    }

    @Override
    public PagedBytesCursor decodeBytesRef(PagedBytesCursor cursor, PagedBytesCursor scratch) {
        cursor.readTerminatedBytesRef(Utf8AscTopNEncoder.TERMINATOR, scratch.scratchBytes);
        scratch.init(scratch.scratchBytes);
        return scratch;
    }

    @Override
    public String toString() {
        return "VersionAsc";
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
