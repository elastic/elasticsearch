/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import static org.elasticsearch.compute.operator.topn.VersionAscTopNEncoder.refuseNul;

class VersionDescTopNEncoder extends SortableDescTopNEncoder {
    private final VersionAscTopNEncoder ascEncoder;

    VersionDescTopNEncoder(VersionAscTopNEncoder ascEncoder) {
        this.ascEncoder = ascEncoder;
    }

    @Override
    public void encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        // TODO versions can contain nul so we need to delegate to the utf-8 encoder for the utf-8 parts of a version
        refuseNul(value);
        int length = bytesRefBuilder.length();
        bytesRefBuilder.append(value);
        bitwiseNot(bytesRefBuilder.bytes(), length, bytesRefBuilder.length());
        bytesRefBuilder.append((byte) ~Utf8AscTopNEncoder.TERMINATOR);
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        int i = bytes.offset;
        while (bytes.bytes[i] != (byte) ~Utf8AscTopNEncoder.TERMINATOR) {
            i++;
        }
        scratch.bytes = bytes.bytes;
        scratch.offset = bytes.offset;
        scratch.length = i - bytes.offset;
        bytes.offset += scratch.length + 1;
        bytes.length -= scratch.length + 1;
        bitwiseNot(scratch.bytes, scratch.offset, scratch.offset + scratch.length);
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
