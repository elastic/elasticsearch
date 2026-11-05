/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.format.Encoding;
import org.elasticsearch.lucene.queries.BinaryDocValuesContainsTermQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Predicate;

/**
 * A {@link ColumnChunkReader.PageDecoder} for a filter on a {@code BYTE_ARRAY} column: instead of the values of a page it keeps which
 * rows of the page match a term, as a bit set, so that finding the matching documents never builds a value or copies a byte.
 *
 * <p>A column with a dictionary pays less than a column without one. The term is tested once for each entry of the dictionary of a chunk,
 * and a row of a dictionary encoded page then matches when the entry of its index does. When no entry matches, the reader does not
 * decompress or decode the dictionary encoded pages of the chunk at all, see {@link #skipPage}. A page that is not dictionary encoded
 * ({@code PLAIN}, which is also what a writer falls back to when its dictionary gets too big) is tested value by value, straight from the
 * bytes of the page.
 *
 * <p>The values of a page are not kept, so this decoder cannot serve a value to a doc values iterator: a filter owns its own reader.
 */
final class BytesPageMatcher implements ColumnChunkReader.PageDecoder {

    /** What a value has to be to match the term. */
    enum Mode {
        /** The same bytes as the term. */
        EQUALS,
        /** Has the bytes of the term somewhere inside it. */
        CONTAINS,
        /** PROTOTYPE, only for the proof of concept: satisfies a predicate that the caller gives, instead of a term. */
        PREDICATE
    }

    private final BytesRef term;
    private final Mode mode;
    private final Predicate<BytesRef> predicate;
    // the value that the predicate tests, a slice of a page that the next call replaces
    private final BytesRef candidate = new BytesRef();

    // the dictionary of the chunk: whether each entry matches, or null while the chunk has no dictionary (yet)
    private boolean[] dictionaryMatches;
    private boolean dictionaryHasMatch;
    private int[] indices = new int[0];

    // the current page: bit r is set when row r has a value that matches
    private long[] bits = new long[0];
    private int words;
    private int matchCount;

    BytesPageMatcher(BytesRef term, Mode mode) {
        assert mode != Mode.PREDICATE;
        this.term = BytesRef.deepCopyOf(term);
        this.mode = mode;
        this.predicate = null;
    }

    /** PROTOTYPE, only for the proof of concept: the values that satisfy the predicate, which must not keep what it is given. */
    BytesPageMatcher(Predicate<BytesRef> predicate) {
        this.term = null;
        this.mode = Mode.PREDICATE;
        this.predicate = predicate;
    }

    /** Whether any row of the current page matches. A page that the reader skipped has no match. */
    boolean pageHasMatch() {
        return matchCount > 0;
    }

    /** Whether the row of the current page has a value that matches. */
    boolean matches(int row) {
        return (bits[row >> 6] & (1L << row)) != 0;
    }

    /** The first row at or after {@code fromRow} of the current page that matches, or {@code -1} when there is none. */
    int nextMatch(int fromRow) {
        int word = fromRow >> 6;
        if (word >= words) {
            return -1;
        }
        long bitsLeft = bits[word] & (-1L << fromRow);
        while (bitsLeft == 0) {
            if (++word >= words) {
                return -1;
            }
            bitsLeft = bits[word];
        }
        return (word << 6) + Long.numberOfTrailingZeros(bitsLeft);
    }

    @Override
    public void startChunk() {
        dictionaryMatches = null;
        dictionaryHasMatch = false;
    }

    @Override
    public void dictionary(byte[] bytes, int offset, int end, int numValues) throws IOException {
        int[] starts = new int[numValues];
        int[] lengths = new int[numValues];
        BytesPageDecoder.readPlain(bytes, offset, end, numValues, starts, lengths);
        boolean[] matching = new boolean[numValues];
        boolean any = false;
        for (int i = 0; i < numValues; i++) {
            matching[i] = test(bytes, starts[i], lengths[i]);
            any |= matching[i];
        }
        dictionaryMatches = matching;
        dictionaryHasMatch = any;
    }

    @Override
    public boolean skipPage(Encoding encoding) {
        if (dictionaryMatches != null && dictionaryHasMatch == false && isDictionary(encoding)) {
            words = 0;
            matchCount = 0;
            return true;
        }
        return false;
    }

    @Override
    public void page(byte[] bytes, int offset, int end, int numValues, int nonNull, boolean[] present, Encoding encoding)
        throws IOException {
        words = (numValues + 63) >>> 6;
        if (bits.length < words) {
            bits = new long[words];
        } else {
            Arrays.fill(bits, 0, words, 0L);
        }
        matchCount = 0;
        if (nonNull == 0) {
            return;
        }
        if (encoding == Encoding.PLAIN) {
            matchPlainValues(bytes, offset, end, nonNull, present);
        } else if (isDictionary(encoding)) {
            matchDictionaryIndices(bytes, offset, end, nonNull, present);
        } else {
            throw new IOException("the page encoding [" + encoding + "] is not supported yet for a byte array column");
        }
    }

    private void matchPlainValues(byte[] bytes, int offset, int end, int nonNull, boolean[] present) throws IOException {
        int position = offset;
        int row = 0;
        for (int i = 0; i < nonNull; i++) {
            // the validation of BytesPageDecoder#readPlain, without keeping the slices
            if (position + 4 > end) {
                throw new IOException("a plain byte array page ends after [" + i + "] of [" + nonNull + "] values");
            }
            int length = (bytes[position] & 0xFF) | ((bytes[position + 1] & 0xFF) << 8) | ((bytes[position + 2] & 0xFF) << 16)
                | ((bytes[position + 3] & 0xFF) << 24);
            position += 4;
            if (length < 0 || position + (long) length > end) {
                throw new IOException("a plain byte array value of [" + length + "] bytes is cut off");
            }
            if (present != null) {
                while (present[row] == false) {
                    row++;
                }
            }
            if (test(bytes, position, length)) {
                set(row);
            }
            position += length;
            row++;
        }
    }

    private void matchDictionaryIndices(byte[] bytes, int offset, int end, int nonNull, boolean[] present) throws IOException {
        if (dictionaryMatches == null) {
            throw new IOException("a dictionary encoded page comes before the dictionary page");
        }
        if (indices.length < nonNull) {
            indices = new int[nonNull];
        }
        RleBitPackedDecoder.decode(bytes, offset + 1, end, bytes[offset] & 0xFF, indices, nonNull);
        int row = 0;
        for (int i = 0; i < nonNull; i++) {
            int index = indices[i];
            if (index < 0 || index >= dictionaryMatches.length) {
                throw new IOException(
                    "a dictionary index [" + index + "] is outside of the dictionary of [" + dictionaryMatches.length + "]"
                );
            }
            if (present != null) {
                while (present[row] == false) {
                    row++;
                }
            }
            if (dictionaryMatches[index]) {
                set(row);
            }
            row++;
        }
    }

    private void set(int row) {
        bits[row >> 6] |= 1L << row;
        matchCount++;
    }

    private boolean test(byte[] bytes, int start, int length) {
        return switch (mode) {
            case EQUALS -> length == term.length
                && Arrays.equals(bytes, start, start + length, term.bytes, term.offset, term.offset + term.length);
            case CONTAINS -> BinaryDocValuesContainsTermQuery.contains(bytes, start, length, term);
            case PREDICATE -> {
                candidate.bytes = bytes;
                candidate.offset = start;
                candidate.length = length;
                yield predicate.test(candidate);
            }
        };
    }

    private static boolean isDictionary(Encoding encoding) {
        return encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY;
    }
}
