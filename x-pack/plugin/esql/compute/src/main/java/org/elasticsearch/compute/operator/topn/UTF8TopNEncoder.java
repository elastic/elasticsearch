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

/**
 * Encodes utf-8 strings as {@code nul} terminated strings.
 * <p>
 *     Utf-8 can contain {@code nul} aka {@code 0x00} so we wouldn't be able
 *     to use that as a terminator. But we fix this by adding {@code 1} to all
 *     values less than the continuation byte. This removes some of the
 *     self-synchronizing nature of utf-8, but we don't need that here. When
 *     we decode we undo out munging so all consumers just get normal utf-8.
 * </p>
 */
final class UTF8TopNEncoder extends SortableTopNEncoder {

    private static final int CONTINUATION_BYTE = 0b1000_0000;
    static final byte TERMINATOR = 0x00;

    @Override
    public int encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        // add one bit to every byte so that there are no "0" bytes in the provided bytes. The only "0" bytes are
        // those defined as separators
        int end = value.offset + value.length;
        for (int i = value.offset; i < end; i++) {
            byte b = value.bytes[i];
            if ((b & CONTINUATION_BYTE) == 0) {
                b++;
            }
            bytesRefBuilder.append(b);
        }
        bytesRefBuilder.append(TERMINATOR);
        return value.length + 1;
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        scratch.bytes = bytes.bytes;
        scratch.offset = bytes.offset;
        scratch.length = 0;
        int i = bytes.offset;
        decode: while (true) {
            int leadByte = bytes.bytes[i] & 0xff;
            int numBytes = utf8CodeLength[leadByte];
            switch (numBytes) {
                case 0:
                    break decode;
                case 1:
                    bytes.bytes[i]--;
                    i++;
                    break;
                case 2:
                    i += 2;
                    break;
                case 3:
                    i += 3;
                    break;
                case 4:
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
    public TopNEncoder toSortable() {
        return this;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }

    @Override
    public String toString() {
        return "UTF8TopNEncoder";
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
