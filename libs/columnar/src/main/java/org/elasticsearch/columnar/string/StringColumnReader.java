/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * Reads a string column written by {@link StringColumnWriter}.
 *
 * <p>Values are addressed by <b>value address</b> — a value's 0-based position in the column's block-encoded
 * store, in {@code [0, numValues)}. A document maps to its value addresses through {@link #iterator()}: a
 * single-valued column maps a document's rank straight to its value address.
 *
 * <p>A column either stores its values or names them with ordinals into a dictionary, and the two answer
 * every read and every filter differently. This holds what does not depend on that choice: how documents map
 * to ranks, how a page is built up and handed to a sink, and how a column in term order is bisected. What
 * does depend on it is {@link PlainStringColumnReader} and {@link DictionaryStringColumnReader}, and
 * {@link #open} picks between them from the metadata.
 */
public abstract sealed class StringColumnReader permits PlainStringColumnReader, DictionaryStringColumnReader {

    private final StringColumnMetadata meta;
    private final ColumnIteratorReader iteratorReader;
    private final int blockSize;

    /** Held so a summary can be read on demand; a merge reads it, an ordinary search never does. */
    protected final IndexInput data;

    /** Carried across page reads, which arrive in document order; see {@link #ranksOfAll}. */
    private ColumnIterator pageIterator;
    private int pageIteratorThrough = -1;

    protected final BytesRef value = new BytesRef();
    protected final BytesRef scratch = new BytesRef();

    /**
     * A page's own storage, reused between calls. Nothing here grows with the column or the dictionary,
     * only with the largest page asked for.
     */
    protected static final int MIN_PAGE_REPEAT = 2;
    protected int[] pageRanks = new int[0];
    protected int[] pageOrdinals = new int[0];
    protected BytesRef[] pageValues = new BytesRef[0];
    protected BytesRef[] pageDictionary = new BytesRef[0];
    private int[] pageStarts = new int[0];
    private int[] pageLengths = new int[0];
    private byte[] pageBytes = new byte[0];
    protected int pageBytesLength;

    // A page's slots found by the bytes in them, so a value the page already holds is one slot and not two.
    // Stamped by generation rather than cleared, as the ordinal slots of a dictionary column are.
    private int[] slotByHash = new int[0];
    private int[] slotStamp = new int[0];
    private int slotGeneration;
    private int slotMask;

    StringColumnReader(StringColumnMetadata meta, IndexInput data, int blockSize) throws IOException {
        assert meta.multiValued() == false : "this surface carries one value per document";
        this.meta = meta;
        this.data = data;
        this.blockSize = blockSize;
        this.iteratorReader = new ColumnIteratorReader(meta.iterator(), data);
    }

    /** A reader for {@code meta}, which decides whether the column has a dictionary to read through. */
    public static StringColumnReader open(StringColumnMetadata meta, IndexInput data) throws IOException {
        return switch (meta) {
            case StringColumnMetadata.Dictionary column -> new DictionaryStringColumnReader(column, data);
            case StringColumnMetadata.Plain column -> new PlainStringColumnReader(column, data);
        };
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
    public abstract BytesRef valueAt(long valueAddress) throws IOException;

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
        final ValueStream.Reader source = summary.terms() == null ? summarisedTerms() : summary.terms().open(data);
        final int size = summary.terms() == null ? summarisedTermCount() : Math.toIntExact(summary.terms().numValues());
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

    /** What a summary that stored no terms of its own is read from, which only a dictionary column has. */
    protected ValueStream.Reader summarisedTerms() {
        return null;
    }

    /** How many terms {@link #summarisedTerms} holds. */
    protected int summarisedTermCount() {
        return 0;
    }

    /** How many terms the dictionary holds, or zero on a column that stores its values. */
    public int dictionarySize() {
        return 0;
    }

    /**
     * Whether the column's values arrive in non-decreasing term order, as under an index sort on this
     * field. A term is then one contiguous run of ranks, which can be found by bisection.
     */
    public boolean valuesSorted() {
        return meta.valuesSorted();
    }

    /** Whether this column names its values with ordinals rather than storing them. */
    public boolean hasDictionary() {
        return false;
    }

    /** How many values the dictionary did not name. */
    public long escapeCount() {
        return 0;
    }

    /** What this column's values would occupy stored plainly, which a decision about it is weighed against. */
    public long valueBytes() {
        return meta.valueBytes();
    }

    /**
     * Documents whose value is {@code term}.
     *
     * <p>A column ranked by document id whose values are in order answers with the documents themselves.
     * Every other shape carries a check as a {@link TwoPhaseIterator} that {@link TwoPhaseIterator#unwrap}
     * returns. A caller working one range of documents at a time has to take that check and stop where its
     * range does: advancing the iterator alone looks for a match until it finds one, so a range holding
     * none pays for the rest of the column.
     */
    public DocIdSetIterator matchTerm(BytesRef term) throws IOException {
        return matching(term, term);
    }

    /** Documents holding a value that starts with {@code prefix}, answered as {@link #matchTerm} is. */
    public DocIdSetIterator matchPrefix(BytesRef prefix) throws IOException {
        return matching(prefix, null);
    }

    /**
     * Documents holding a value that has {@code term} somewhere inside it.
     *
     * <p>Order says nothing about what a value contains, so a column in term order is no help here and the
     * values are looked at either way.
     */
    public DocIdSetIterator matchContains(BytesRef term) throws IOException {
        return match(value -> ESVectorUtil.contains(value.bytes, value.offset, value.length, term.bytes, term.offset, term.length));
    }

    /**
     * Documents holding a value {@code matcher} accepts.
     *
     * <p>The test is opaque, so nothing about the order of the values narrows it and every distinct value has
     * to be offered. What the column's shape decides is how many that is: a dictionary column asks once a
     * term and lets every value naming it inherit the answer, a column of runs asks once a run, and a column
     * of neither asks once a value. A caller that knows more than a predicate says, a term or a prefix,
     * should say so through {@link #matchTerm} or {@link #matchPrefix} instead, which can bisect.
     *
     * <p>The {@link BytesRef} handed to {@code matcher} points into a buffer this reader reuses, so it is
     * only valid for the duration of the call.
     */
    public DocIdSetIterator match(Predicate<BytesRef> matcher) throws IOException {
        if (meta.numDocsWithField() == 0) {
            return DocIdSetIterator.empty();
        }
        return valueMatches(matcher);
    }

    /** Documents holding a value {@code matcher} accepts, for a column that knows how its values are reached. */
    protected abstract DocIdSetIterator valueMatches(Predicate<BytesRef> matcher) throws IOException;

    /**
     * Documents whose value equals {@code exact}, or starts with {@code prefix} when {@code exact} is null.
     *
     * <p>A column written in term order puts every match in one run, so bisecting the values finds its ends
     * and the answer costs the column's logarithm, whichever way the values are stored. Anything else is
     * answered by the column's own shape.
     */
    private DocIdSetIterator matching(BytesRef prefix, BytesRef exact) throws IOException {
        if (meta.numDocsWithField() == 0) {
            return DocIdSetIterator.empty();
        }
        if (meta.valuesSorted() && meta.multiValued() == false) {
            return documents(sortedRange(prefix, exact));
        }
        return unorderedMatches(prefix, exact);
    }

    /** The match a column with no order to bisect answers with: over ordinals, or over the values. */
    protected abstract DocIdSetIterator unorderedMatches(BytesRef prefix, BytesRef exact) throws IOException;

    /**
     * The ranks holding the term, or the prefix, in a column whose values arrive in order. Their ends are
     * found by bisection over the values, which needs only the order and no ordinals: a term costs a couple
     * of dozen block reads instead of a comparison per document.
     */
    private RankRange sortedRange(BytesRef prefix, BytesRef exact) throws IOException {
        final int count = meta.numDocsWithField();
        final BytesRef target = exact != null ? exact : prefix;
        final int first = firstAtLeast(target, count);
        if (first == count) {
            return RankRange.EMPTY;
        }
        if (matches(valueAt(first), prefix, exact) == false) {
            return RankRange.EMPTY;
        }
        // The run ends where the values stop carrying it, which is again a boundary in value order.
        int low = first;
        int high = count;
        while (low < high) {
            final int mid = (low + high) >>> 1;
            if (matches(valueAt(mid), prefix, exact)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return new RankRange(first, low);
    }

    /**
     * The ranks a match covers, as {@code [from, to)}. Turning it into documents is {@link #documents}' work.
     */
    protected record RankRange(int from, int to) {
        static final RankRange EMPTY = new RankRange(0, 0);

        boolean isEmpty() {
            return from >= to;
        }
    }

    /** The first rank whose value sorts at or after {@code target}, by bisection over ordered values. */
    private int firstAtLeast(BytesRef target, int count) throws IOException {
        int low = 0;
        int high = count;
        while (low < high) {
            final int mid = (low + high) >>> 1;
            if (valueAt(mid).compareTo(target) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    /** A value of the wrong length cannot be the term, and cannot be shorter than the prefix. */
    protected static boolean matches(BytesRef value, BytesRef prefix, BytesRef exact) {
        if (exact != null) {
            return value.length == exact.length && value.bytesEquals(exact);
        }
        if (value.length < prefix.length) {
            return false;
        }
        return Arrays.equals(
            value.bytes,
            value.offset,
            value.offset + prefix.length,
            prefix.bytes,
            prefix.offset,
            prefix.offset + prefix.length
        );
    }

    /**
     * The documents holding a contiguous range of ranks. A dense column ranks its documents by document id,
     * so the range is the answer. A sparse one drives its presence iterator instead of walking to each rank
     * in turn: the iterator skips, and a document is confirmed by testing its rank against the range, which
     * keeps the work a search does proportional to the documents it is asked about rather than to the
     * column.
     */
    private DocIdSetIterator documents(RankRange ranks) throws IOException {
        if (ranks.isEmpty()) {
            return DocIdSetIterator.empty();
        }
        if (meta.iterator().isDense()) {
            // Ranked by document id, so the range is already the documents.
            return DocIdSetIterator.range(ranks.from(), ranks.to());
        }
        final int firstRank = ranks.from();
        final int endRank = ranks.to();
        final ColumnIterator presence = iterator();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(presence) {
            @Override
            public boolean matches() {
                final int rank = presence.rank();
                return rank >= firstRank && rank < endRank;
            }

            @Override
            public float matchCost() {
                return 1f;
            }

        });
    }

    /**
     * Resolves the requested documents to their ranks, answering false when any of them has no value. A page
     * carries one entry a document and has no way to say a document has nothing, so a caller asking about
     * documents a sparse column skips is told to read them itself rather than handed a neighbour's value.
     */
    protected boolean ranksOfAll(int[] docs, int offset, int count) throws IOException {
        if (count == 0) {
            return true;
        }
        // Pages arrive in document order, so one iterator serves all of them and carries its position from
        // one to the next. Resolving ranks leaves it somewhere inside the page it was given, so a page that
        // does not start beyond the last one asks for a fresh iterator.
        if (pageIterator == null || docs[offset] <= pageIteratorThrough) {
            pageIterator = iterator();
        }
        pageIteratorThrough = docs[offset + count - 1];
        pageIterator.ranks(docs, offset, count, pageRanks);
        for (int i = 0; i < count; i++) {
            if (pageRanks[i] == ColumnIterator.NO_RANK) {
                return false;
            }
        }
        return true;
    }

    /**
     * Fills {@code ordinals} with the column ordinal of each requested rank, resolving no bytes. An ordinal
     * below {@link #dictionarySize()} names a dictionary entry and is stable for the whole column, which
     * lets a consumer accumulate into a dense array rather than a hash; the escape marker says the value is
     * in the exception stream and has to be resolved with
     * {@link DictionaryStringColumnReader#resolveEscape}.
     *
     * @return false when the column has no dictionary, so there are no ordinals to serve
     */
    public boolean readOrdinals(int[] docs, int offset, int count, int[] ordinals) throws IOException {
        return false;
    }

    /** The rank a document takes, so a consumer holding ordinals can reach an escaped value's address. */
    public void ranks(int[] docs, int offset, int count, int[] ranks) throws IOException {
        iterator().ranks(docs, offset, count, ranks);
    }

    /**
     * A page of values, for a consumer grouping or aggregating over them.
     *
     * <p>A dictionary column hands back ordinals where that is worth it: the page's distinct values are
     * resolved once each into a dictionary of its own, and every document becomes an index into it. The
     * consumer then compares ints and resolves a value once per distinct value rather than once per row.
     *
     * <p>Where a page repeats little - most of it escaped, or nearly every document differs - the ordinal
     * form is as long as the page and saves nothing, so the values are handed over directly. A column with
     * no dictionary always hands over values.
     *
     * @return false when the page cannot be served this way and the caller should read the documents one at
     *         a time; true when {@code sink} was called exactly once
     */
    public boolean readBlock(int[] docs, int offset, int count, StringBlockSink sink) throws IOException {
        if (meta.multiValued()) {
            // A document with several values needs a shape the sink has none for.
            return false;
        }
        if (count == 0) {
            sink.appendValues(pageValues, 0);
            return true;
        }
        growPage(count);
        if (ranksOfAll(docs, offset, count) == false) {
            return false;
        }
        return appendPage(count, sink);
    }

    /** Hands {@code count} resolved ranks to the sink, in whichever form the column's values take. */
    protected abstract boolean appendPage(int count, StringBlockSink sink) throws IOException;

    /** Copies a value into the page's own bytes, so the reader's buffer can be reused for the next one. */
    /**
     * Opens a page's dictionary for at most {@code slots} entries found by their bytes, which is what
     * {@link #pageSlotFor} then finds them by.
     */
    protected void startPageSlots(int slots) {
        int capacity = 16;
        while (capacity < slots * 2) {
            capacity <<= 1;
        }
        if (slotByHash.length < capacity) {
            slotByHash = new int[capacity];
            slotStamp = new int[capacity];
            slotGeneration = 0;
        }
        if (++slotGeneration == Integer.MAX_VALUE) {
            Arrays.fill(slotStamp, 0);
            slotGeneration = 1;
        }
        slotMask = capacity - 1;
    }

    /**
     * The slot holding {@code value}: one the page already has when it holds those bytes, and {@code next}
     * otherwise, the value having been appended there. A caller advances its own count when what it gets
     * back is the slot it offered.
     *
     * <p>An ordinal names a value, so a page hands back one slot a value however many documents carry it.
     * Coalescing only what arrives together is not enough for that: a value can return to a page after
     * another one, and stored twice it is two addresses that say nothing about the bytes being the same.
     */
    protected int pageSlotFor(BytesRef value, int next) {
        int i = value.hashCode() & slotMask;
        while (slotStamp[i] == slotGeneration) {
            final int slot = slotByHash[i];
            if (pageSlotHolds(slot, value)) {
                return slot;
            }
            i = (i + 1) & slotMask;
        }
        slotStamp[i] = slotGeneration;
        slotByHash[i] = next;
        appendToPage(next, value);
        return next;
    }

    /** Whether the bytes a slot already holds are {@code value}'s. */
    protected boolean pageSlotHolds(int slot, BytesRef value) {
        return pageLengths[slot] == value.length
            && Arrays.equals(
                pageBytes,
                pageStarts[slot],
                pageStarts[slot] + value.length,
                value.bytes,
                value.offset,
                value.offset + value.length
            );
    }

    protected void appendToPage(int slot, BytesRef value) {
        if (pageBytes.length < pageBytesLength + value.length) {
            pageBytes = ArrayUtil.grow(pageBytes, pageBytesLength + value.length);
        }
        System.arraycopy(value.bytes, value.offset, pageBytes, pageBytesLength, value.length);
        pageStarts[slot] = pageBytesLength;
        pageLengths[slot] = value.length;
        pageBytesLength += value.length;
    }

    /** Points {@code into} at the page's bytes, once they have stopped moving. */
    protected void point(BytesRef[] into, int count) {
        for (int i = 0; i < count; i++) {
            into[i].bytes = pageBytes;
            into[i].offset = pageStarts[i];
            into[i].length = pageLengths[i];
        }
    }

    protected void growPage(int count) {
        if (pageRanks.length >= count) {
            return;
        }
        pageRanks = new int[count];
        pageOrdinals = new int[count];
        pageStarts = new int[count];
        pageLengths = new int[count];
        pageValues = new BytesRef[count];
        pageDictionary = new BytesRef[count];
        for (int i = 0; i < count; i++) {
            pageValues[i] = new BytesRef();
            pageDictionary[i] = new BytesRef();
        }
    }

    /** Values behind one offset in the byte stream. */
    public int blockSize() {
        return blockSize;
    }

    /** Total number of values across all documents. */
    public long numValues() {
        return meta.numValues();
    }

}
