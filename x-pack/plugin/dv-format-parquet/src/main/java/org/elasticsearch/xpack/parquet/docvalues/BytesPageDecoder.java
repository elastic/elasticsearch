/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.format.Encoding;

import java.io.IOException;

/**
 * Decodes the pages of a {@code BYTE_ARRAY} column. A value is a slice of the buffer of its page, or of the buffer of the dictionary,
 * so decoding a page copies no value.
 */
final class BytesPageDecoder implements ColumnChunkReader.PageDecoder {

    private final BytesRef scratch = new BytesRef();

    // the dictionary of the chunk, as slices of the bytes of its page
    private byte[] dictionaryBuffer;
    private int[] dictionaryStarts;
    private int[] dictionaryLengths;

    // the rows of the current page, as slices of a buffer
    private byte[] buffer = new byte[0];
    private int[] starts = new int[0];
    private int[] lengths = new int[0];

    /** The value of a row of the current page. The result is reused by the next call. */
    BytesRef value(int row) {
        scratch.bytes = buffer;
        scratch.offset = starts[row];
        scratch.length = lengths[row];
        return scratch;
    }

    /** The bytes that the values of the current page are slices of. They belong to the decoder, and the next page replaces them. */
    byte[] pageBytes() {
        return buffer;
    }

    /** The start in {@link #pageBytes()} of the value of a row of the current page. Only valid for a row that is present. */
    int valueStart(int row) {
        return starts[row];
    }

    /** The length of the value of a row of the current page. Only valid for a row that is present. */
    int valueLength(int row) {
        return lengths[row];
    }

    @Override
    public void startChunk() {
        dictionaryBuffer = null;
        dictionaryStarts = null;
        dictionaryLengths = null;
    }

    @Override
    public void dictionary(byte[] bytes, int offset, int end, int numValues) throws IOException {
        dictionaryBuffer = bytes;
        dictionaryStarts = new int[numValues];
        dictionaryLengths = new int[numValues];
        readPlain(bytes, offset, end, numValues, dictionaryStarts, dictionaryLengths);
    }

    @Override
    public void page(byte[] bytes, int offset, int end, int numValues, int nonNull, boolean[] present, Encoding encoding)
        throws IOException {
        int[] valueStarts = new int[nonNull];
        int[] valueLengths = new int[nonNull];
        final byte[] valueBuffer;
        if (encoding == Encoding.PLAIN) {
            readPlain(bytes, offset, end, nonNull, valueStarts, valueLengths);
            valueBuffer = bytes;
        } else if (encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY) {
            if (dictionaryBuffer == null) {
                throw new IOException("a dictionary encoded page comes before the dictionary page");
            }
            if (nonNull > 0) {
                int bitWidth = bytes[offset] & 0xFF;
                int[] indices = new int[nonNull];
                RleBitPackedDecoder.decode(bytes, offset + 1, end, bitWidth, indices, nonNull);
                for (int i = 0; i < nonNull; i++) {
                    if (indices[i] < 0 || indices[i] >= dictionaryStarts.length) {
                        throw new IOException(
                            "a dictionary index [" + indices[i] + "] is outside of the dictionary of [" + dictionaryStarts.length + "]"
                        );
                    }
                    valueStarts[i] = dictionaryStarts[indices[i]];
                    valueLengths[i] = dictionaryLengths[indices[i]];
                }
            }
            valueBuffer = dictionaryBuffer;
        } else {
            throw new IOException("the page encoding [" + encoding + "] is not supported yet for a byte array column");
        }
        buffer = valueBuffer;
        if (present == null) {
            starts = valueStarts;
            lengths = valueLengths;
            return;
        }
        int[] rowStarts = new int[numValues];
        int[] rowLengths = new int[numValues];
        int next = 0;
        for (int row = 0; row < numValues; row++) {
            if (present[row]) {
                rowStarts[row] = valueStarts[next];
                rowLengths[row] = valueLengths[next];
                next++;
            }
        }
        starts = rowStarts;
        lengths = rowLengths;
    }

    /** Reads {@code count} values that are each a 4-byte little-endian length and that many bytes. */
    static void readPlain(byte[] bytes, int offset, int end, int count, int[] starts, int[] lengths) throws IOException {
        int position = offset;
        for (int i = 0; i < count; i++) {
            if (position + 4 > end) {
                throw new IOException("a plain byte array page ends after [" + i + "] of [" + count + "] values");
            }
            int length = (bytes[position] & 0xFF) | ((bytes[position + 1] & 0xFF) << 8) | ((bytes[position + 2] & 0xFF) << 16)
                | ((bytes[position + 3] & 0xFF) << 24);
            position += 4;
            if (length < 0 || position + (long) length > end) {
                throw new IOException("a plain byte array value of [" + length + "] bytes is cut off");
            }
            starts[i] = position;
            lengths[i] = length;
            position += length;
        }
    }
}
