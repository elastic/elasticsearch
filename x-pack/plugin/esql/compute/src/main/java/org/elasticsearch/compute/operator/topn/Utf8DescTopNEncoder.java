/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.Arrays;

import static org.elasticsearch.compute.operator.topn.Utf8AscTopNEncoder.CONTINUATION_BYTE;
import static org.elasticsearch.compute.operator.topn.Utf8AscTopNEncoder.TERMINATOR;

/**
 * Encodes utf-8 strings as {@code nul} terminated strings.
 */
final class Utf8DescTopNEncoder extends SortableDescTopNEncoder {
    private final Utf8AscTopNEncoder ascEncoder;

    Utf8DescTopNEncoder(Utf8AscTopNEncoder ascEncoder) {
        this.ascEncoder = ascEncoder;
    }

    @Override
    public void encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        /*
         * add one bit to every non-continuation byte so that there are no "0" bytes
         * in the encoded copy. The only "0" bytes are separators.
         */
        int end = value.offset + value.length;
        for (int i = value.offset; i < end; i++) {
            byte b = value.bytes[i];
            if ((b & CONTINUATION_BYTE) == 0) {
                b++;
            }
            bytesRefBuilder.append((byte) ~b);
        }
        bytesRefBuilder.append((byte) ~TERMINATOR);
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        scratch.bytes = bytes.bytes;
        scratch.offset = bytes.offset;
        int i = bytes.offset;
        decode: while (true) {
            int leadByte = ~bytes.bytes[i] & 0xff;
            int numBytes = utf8CodeLength[leadByte];
            switch (numBytes) {
                case 0:
                    break decode;
                case 1:
                    bytes.bytes[i] = (byte) (~bytes.bytes[i] - 1);
                    i++;
                    break;
                case 2:
                    bytes.bytes[i] = (byte) ~bytes.bytes[i];
                    bytes.bytes[i + 1] = (byte) ~bytes.bytes[i + 1];
                    i += 2;
                    break;
                case 3:
                    bytes.bytes[i] = (byte) ~bytes.bytes[i];
                    bytes.bytes[i + 1] = (byte) ~bytes.bytes[i + 1];
                    bytes.bytes[i + 2] = (byte) ~bytes.bytes[i + 2];
                    i += 3;
                    break;
                case 4:
                    bytes.bytes[i] = (byte) ~bytes.bytes[i];
                    bytes.bytes[i + 1] = (byte) ~bytes.bytes[i + 1];
                    bytes.bytes[i + 2] = (byte) ~bytes.bytes[i + 2];
                    bytes.bytes[i + 3] = (byte) ~bytes.bytes[i + 3];
                    i += 4;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid UTF8 header byte: 0x" + Integer.toHexString(leadByte));
            }
        }
        scratch.length = i - bytes.offset;
        bytes.offset = i + 1;
        bytes.length -= scratch.length + 1;
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

    // This section very inspired by Lucene's UnicodeUtil
    static final int[] utf8CodeLength;

    static {
        int v = Integer.MIN_VALUE;

        utf8CodeLength = Arrays.stream(
            new int[][] {
                // The next line differs from UnicodeUtil - the first entry is 0 because that's our terminator
                { 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
                { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
                { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
                { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
                { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
                { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
                { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
                { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
                // The next line differs from UnicodeUtil - the first entry is 1 because it's valid in our encoding.
                { 1, v, v, v, v, v, v, v, v, v, v, v, v, v, v, v },
                { v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, v },
                { v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, v },
                { v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, v },
                { 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
                { 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
                { 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3 },
                { 4, 4, 4, 4, 4, 4, 4, 4 /* , 5, 5, 5, 5, 6, 6, 0, 0 */ } }
        ).flatMapToInt(Arrays::stream).toArray();
    }
}
