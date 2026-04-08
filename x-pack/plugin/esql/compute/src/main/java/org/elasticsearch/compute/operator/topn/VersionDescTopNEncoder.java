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

import static org.elasticsearch.compute.operator.topn.VersionAscTopNEncoder.refuseNul;

class VersionDescTopNEncoder extends SortableDescTopNEncoder {
    private final VersionAscTopNEncoder ascEncoder;

    VersionDescTopNEncoder(VersionAscTopNEncoder ascEncoder) {
        this.ascEncoder = ascEncoder;
    }

    @Override
    public void encodeBytesRef(BytesRef value, PagedBytesBuilder builder) {
        // TODO versions can contain nul so we need to delegate to the utf-8 encoder for the utf-8 parts of a version
        refuseNul(value);
        builder.appendNot(value.bytes, value.offset, value.length);
        builder.append((byte) ~Utf8AscTopNEncoder.TERMINATOR);
    }

    @Override
    public PagedBytesCursor decodeBytesRef(PagedBytesCursor cursor, PagedBytesCursor scratch) {
        cursor.readTerminatedBytesRef((byte) ~Utf8AscTopNEncoder.TERMINATOR, scratch.scratchBytes);
        BytesRef sb = scratch.scratchBytes;
        bitwiseNot(sb.bytes, sb.offset, sb.offset + sb.length);
        scratch.init(sb);
        return scratch;
    }

    @Override
    public String toString() {
        return "VersionDesc";
    }

    @Override
    public TopNEncoder toSortable(boolean asc) {
        return asc ? ascEncoder : this;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return ascEncoder;
    }
}
