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

import static org.elasticsearch.compute.operator.topn.Utf8AscTopNEncoder.CONTINUATION_BYTE;
import static org.elasticsearch.compute.operator.topn.Utf8AscTopNEncoder.TERMINATOR;
import static org.elasticsearch.compute.operator.topn.Utf8AscTopNEncoder.utf8CodeLength;

/**
 * Encodes utf-8 strings as {@code nul} terminated strings.
 */
final class Utf8DescTopNEncoder extends SortableDescTopNEncoder {
    private final Utf8AscTopNEncoder ascEncoder;

    Utf8DescTopNEncoder(Utf8AscTopNEncoder ascEncoder) {
        this.ascEncoder = ascEncoder;
    }

    @Override
    public void encodeBytesRef(BytesRef value, PagedBytesBuilder builder) {
        int end = value.offset + value.length;
        for (int i = value.offset; i < end; i++) {
            byte b = value.bytes[i];
            if ((b & CONTINUATION_BYTE) == 0) {
                b++;
            }
            builder.append((byte) ~b);
        }
        builder.append((byte) ~TERMINATOR);
    }

    @Override
    public PagedBytesCursor decodeBytesRef(PagedBytesCursor cursor, PagedBytesCursor scratch) {
        cursor.readTerminatedBytesRef((byte) ~TERMINATOR, scratch.scratchBytes);
        BytesRef sb = scratch.scratchBytes;
        int i = 0;
        while (i < sb.length) {
            int leadByte = ~sb.bytes[i] & 0xff;
            int numBytes = utf8CodeLength[leadByte];
            if (numBytes == 1) {
                sb.bytes[i] = (byte) (~sb.bytes[i] - 1);
            } else {
                for (int j = i; j < i + numBytes; j++) {
                    sb.bytes[j] = (byte) ~sb.bytes[j];
                }
            }
            i += numBytes;
        }
        scratch.init(sb);
        return scratch;
    }

    @Override
    public TopNEncoder toSortable(boolean asc) {
        return asc ? ascEncoder : this;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return ascEncoder;
    }

    @Override
    public String toString() {
        return "Utf8Desc";
    }
}
