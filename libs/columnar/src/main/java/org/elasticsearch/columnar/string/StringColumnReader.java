/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.numeric.NumericColumnReader;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.columnar.substrate.MonotonicReader;

import java.io.IOException;
import java.util.List;

/**
 * Reads a string column written by {@link StringColumnWriter}.
 *
 * <p>Values are addressed by <b>value address</b> — a value's 0-based position in the column's block-encoded
 * store, in {@code [0, numValues)}. A document maps to its value addresses through {@link #iterator()}: a
 * single-valued column maps a document's rank straight to its value address.
 *
 * <p>The values sit in a {@link ValueStream}: addressed in blocks of a fixed count of values, compressed in
 * chunks of a fixed number of bytes, with a chunk closing only on a block boundary so no value spans two of
 * them. That is the byte-derived chunking in {@code docs/PLAN.md} — a block of long urls and a block of single
 * characters are the same count of values and nothing like the same amount of data, so the unit that is
 * compressed is bounded by bytes and the unit that is addressed by values.
 */
public final class StringColumnReader {

    private final StringColumnMetadata meta;
    private final ColumnIteratorReader iteratorReader;
    private final ValueStream.Reader values;

    /** Set on a dictionary column: the terms, and an ordinal into them for every value. */
    private final ValueStream.Reader dictionary;
    private final NumericColumnReader ordinals;
    /** Set on a dictionary column that any value escaped: their bytes, and where each one's is. */
    private final ValueStream.Reader exceptions;
    private final LongValues escapeRanks;

    private final BytesRef value = new BytesRef();

    /** Held so a summary can be read on demand; a merge reads it, an ordinary search never does. */
    private final IndexInput data;

    public StringColumnReader(StringColumnMetadata meta, IndexInput data) throws IOException {
        this.data = data;
        assert meta.multiValued() == false : "this surface carries one value per document";
        this.meta = meta;
        this.iteratorReader = new ColumnIteratorReader(meta.iterator(), data);
        if (meta.layout() == StringColumnLayout.DICTIONARY) {
            this.values = null;
            this.dictionary = meta.dictionary().open(data);
            this.ordinals = new NumericColumnReader(meta.ordinals(), data);
            if (meta.hasEscapes()) {
                this.exceptions = meta.exceptions().open(data);
                this.escapeRanks = MonotonicReader.open(
                    data,
                    meta.escapeRanks().meta(),
                    StringColumnWriter.escapeRankEntries(meta.numValues()),
                    meta.escapeRanks().dataOffset(),
                    meta.escapeRanks().dataLength()
                );
            } else {
                this.exceptions = null;
                this.escapeRanks = null;
            }
        } else {
            this.values = meta.numDocsWithField() == 0 ? null : meta.values().open(data);
            this.dictionary = null;
            this.ordinals = null;
            this.exceptions = null;
            this.escapeRanks = null;
        }
    }

    /** A fresh iterator over the documents that have a value; positioned by {@link ColumnIterator#rank()}. */
    public ColumnIterator iterator() throws IOException {
        return iteratorReader.iterator();
    }

    /**
     * The value address of a document's first value, given its rank. This surface carries one value per
     * document, so the rank is the address.
     */
    public long firstValueAddress(int rank) {
        return rank;
    }

    /** The number of values a document has, given its rank. This surface carries one per document. */
    public long valueCount(int rank) {
        return 1;
    }

    /**
     * The value at {@code valueAddress} in {@code [0, numValues)}. The returned {@link BytesRef} points into a
     * buffer this reader reuses, so it is only valid until the next call.
     */
    public BytesRef valueAt(long valueAddress) throws IOException {
        if (dictionary != null) {
            // The ordinals are one per value in the same order, so a value address addresses them directly.
            final long ordinal = ordinals.valueAt(valueAddress);
            if (ordinal == meta.dictionarySize()) {
                exceptions.get(escapeRankOf(valueAddress), value);
            } else {
                dictionary.get(ordinal, value);
            }
        } else {
            values.get(valueAddress, value);
        }
        return value;
    }

    /**
     * Where an escaped value's bytes are: how many escaped before it. The table gives that for the start
     * of its block and the ordinals in between give the rest.
     */
    private long escapeRankOf(long valueAddress) throws IOException {
        final long block = valueAddress / StringColumnWriter.ESCAPE_RANK_BLOCK;
        long rank = escapeRanks.get(block);
        for (long at = block * StringColumnWriter.ESCAPE_RANK_BLOCK; at < valueAddress; at++) {
            if (ordinals.valueAt(at) == meta.dictionarySize()) {
                rank++;
            }
        }
        return rank;
    }

    /** Whether this column recorded what it surveyed, so a merge need not read its values again. */
    public boolean hasSummary() {
        return meta.hasSummary();
    }

    /** The values the summary's counts are a share of. */
    public long summaryValues() {
        return meta.hasSummary() ? meta.summary().numValues() : 0;
    }

    /**
     * The summarised terms and how often each was seen. The counts are the survey's and so are lower
     * bounds, which is what makes a vocabulary combined from several of them under-state its coverage
     * rather than over-state it.
     */
    public void readSummary(List<BytesRef> terms, List<Long> counts) throws IOException {
        final StringColumnMetadata.Summary summary = meta.summary();
        final ValueStream.Reader source = summary.terms() == null ? dictionary : summary.terms().open(data);
        final int size = summary.terms() == null ? meta.dictionarySize() : Math.toIntExact(summary.terms().numValues());
        final BytesRef term = new BytesRef();
        for (int ordinal = 0; ordinal < size; ordinal++) {
            source.get(ordinal, term);
            terms.add(BytesRef.deepCopyOf(term));
        }
        // Cloned rather than read in place: the caller's own reads are interleaved with these.
        final IndexInput in = data.clone();
        in.seek(summary.countsOffset());
        for (int ordinal = 0; ordinal < size; ordinal++) {
            counts.add(in.readVLong());
        }
    }

    /** How many terms the dictionary holds, or zero on a column that stores its values. */
    public int dictionarySize() {
        return meta.dictionarySize();
    }

    /** Whether this column names its values with ordinals rather than storing them. */
    public boolean hasDictionary() {
        return dictionary != null;
    }

    /** How many values the dictionary did not name. */
    public long exceptionCount() {
        return meta.hasEscapes() ? meta.exceptions().numValues() : 0;
    }

    /** What this column's values would occupy stored plainly, which a decision about it is weighed against. */
    public long valueBytes() {
        return meta.valueBytes();
    }

    /** The term at {@code ordinal}, on a column that has a dictionary. */
    public BytesRef termAt(int ordinal, BytesRef dst) throws IOException {
        dictionary.get(ordinal, dst);
        return dst;
    }

    /**
     * The ordinal the value at {@code valueAddress} takes, or {@link #dictionarySize()} when it escaped.
     * Only meaningful on a column that has a dictionary.
     */
    public int ordinalAt(long valueAddress) throws IOException {
        return Math.toIntExact(ordinals.valueAt(valueAddress));
    }

    /** Values behind one offset in the byte stream. */
    public int blockSize() {
        return meta.layout() == StringColumnLayout.DICTIONARY ? meta.dictionary().valuesPerBlock() : meta.values().valuesPerBlock();
    }

    /** Total number of values across all documents. */
    public long numValues() {
        return meta.numValues();
    }

}
