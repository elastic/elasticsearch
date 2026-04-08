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
final class Utf8AscTopNEncoder extends SortableAscTopNEncoder {
    static final int CONTINUATION_BYTE = 0b1000_0000;
    static final byte TERMINATOR = 0x00;

    private final Utf8DescTopNEncoder descEncoder = new Utf8DescTopNEncoder(this);

    @Override
    public void encodeBytesRef(BytesRef value, PagedBytesBuilder builder) {
        int end = value.offset + value.length;
        for (int i = value.offset; i < end; i++) {
            byte b = value.bytes[i];
            if ((b & CONTINUATION_BYTE) == 0) {
                b++;
            }
            builder.append(b);
        }
        builder.append(TERMINATOR);
    }

    @Override
    public PagedBytesCursor decodeBytesRef(PagedBytesCursor cursor, PagedBytesCursor scratch) {
        cursor.readTerminatedBytesRef(TERMINATOR, scratch.scratchBytes);
        BytesRef sb = scratch.scratchBytes;
        int i = 0;
        while (i < sb.length) {
            int leadByte = sb.bytes[i] & 0xff;
            int numBytes = utf8CodeLength[leadByte];
            if (numBytes == 1) {
                sb.bytes[i]--;
            }
            i += numBytes;
        }
        scratch.init(sb);
        return scratch;
    }

    @Override
    public TopNEncoder toSortable(boolean asc) {
        return asc ? this : descEncoder;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }

    @Override
    public String toString() {
        return "Utf8Asc";
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
