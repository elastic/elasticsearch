/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.Type;

import java.io.IOException;

/**
 * Decodes the pages of an {@code INT32} or {@code INT64} column into {@code long} values, converted as the mapped field type expects.
 * The conversion is applied to the values of a dictionary, so a dictionary encoded page converts each distinct value once.
 */
final class LongPageDecoder implements ColumnChunkReader.PageDecoder {

    private final boolean int32;
    private final boolean zeroExtend;
    private final Conversion conversion;

    private long[] dictionary;
    private long[] values = new long[0];

    LongPageDecoder(int physicalType, int convertedType, Conversion conversion) {
        this.conversion = conversion;
        if (physicalType != Type.INT32.getValue() && physicalType != Type.INT64.getValue()) {
            throw new IllegalArgumentException("numeric doc values need an INT32 or INT64 column, not " + Type.findByValue(physicalType));
        }
        this.int32 = physicalType == Type.INT32.getValue();
        // an unsigned 32-bit integer is stored in the bits of an int, so it must not be sign-extended
        this.zeroExtend = convertedType == ConvertedType.UINT_32.getValue();
    }

    /** The value of a row of the current page. Only valid for a row that is present. */
    long value(int row) {
        return values[row];
    }

    /**
     * The values of the rows of the current page, with a row for every row of the page (a row without a value holds {@code 0}). The
     * array belongs to the decoder and is replaced by the next page, so a caller reads from it before it loads another page.
     */
    long[] pageValues() {
        return values;
    }

    @Override
    public void startChunk() {
        dictionary = null;
    }

    @Override
    public void dictionary(byte[] bytes, int offset, int end, int numValues) throws IOException {
        dictionary = decodePlain(bytes, offset, end, numValues);
    }

    @Override
    public void page(byte[] bytes, int offset, int end, int numValues, int nonNull, boolean[] present, Encoding encoding)
        throws IOException {
        long[] decoded = decodeValues(bytes, offset, end, nonNull, encoding);
        if (present == null) {
            values = decoded;
            return;
        }
        long[] rows = new long[numValues];
        int next = 0;
        for (int row = 0; row < numValues; row++) {
            if (present[row]) {
                rows[row] = decoded[next++];
            }
        }
        values = rows;
    }

    private long[] decodeValues(byte[] bytes, int offset, int end, int count, Encoding encoding) throws IOException {
        if (encoding == Encoding.PLAIN) {
            return decodePlain(bytes, offset, end, count);
        }
        if (encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY) {
            if (dictionary == null) {
                throw new IOException("a dictionary encoded page comes before the dictionary page");
            }
            if (count == 0) {
                return new long[0];
            }
            int bitWidth = bytes[offset] & 0xFF;
            int[] indices = new int[count];
            RleBitPackedDecoder.decode(bytes, offset + 1, end, bitWidth, indices, count);
            long[] decoded = new long[count];
            for (int i = 0; i < count; i++) {
                if (indices[i] < 0 || indices[i] >= dictionary.length) {
                    throw new IOException(
                        "a dictionary index [" + indices[i] + "] is outside of the dictionary of [" + dictionary.length + "]"
                    );
                }
                decoded[i] = dictionary[indices[i]];
            }
            return decoded;
        }
        throw new IOException("the page encoding [" + encoding + "] is not supported yet");
    }

    private long[] decodePlain(byte[] bytes, int offset, int end, int count) throws IOException {
        int width = int32 ? 4 : 8;
        if ((long) count * width > end - offset) {
            throw new IOException("a plain page of [" + count + "] values has only [" + (end - offset) + "] bytes");
        }
        long[] decoded = new long[count];
        int position = offset;
        for (int i = 0; i < count; i++) {
            if (int32) {
                int value = (bytes[position] & 0xFF) | ((bytes[position + 1] & 0xFF) << 8) | ((bytes[position + 2] & 0xFF) << 16)
                    | ((bytes[position + 3] & 0xFF) << 24);
                decoded[i] = conversion.apply(zeroExtend ? value & 0xFFFFFFFFL : value);
            } else {
                long value = 0;
                for (int b = 7; b >= 0; b--) {
                    value = (value << 8) | (bytes[position + b] & 0xFF);
                }
                decoded[i] = conversion.apply(value);
            }
            position += width;
        }
        return decoded;
    }
}
