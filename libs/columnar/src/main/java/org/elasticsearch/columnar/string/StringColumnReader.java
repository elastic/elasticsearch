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
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.numeric.NumericColumnReader;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.columnar.substrate.MonotonicReader;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Reads a string column written by {@link StringColumnWriter}.
 *
 * <p>Values are addressed by <b>value address</b> — a value's 0-based position in the column's block-encoded
 * store, in {@code [0, numValues)}. A document maps to its value addresses through {@link #iterator()}: a
 * single-valued column maps a document's rank straight to its value address.
 *
 * <p>The values sit in a {@link ValueStream}, which addresses them in blocks and compresses them in chunks.
 */
public final class StringColumnReader {

    private final StringColumnMetadata meta;
    private final ColumnIteratorReader iteratorReader;
    private final ValueStream.Reader values;

    /** Set on a dictionary column: the terms, and an ordinal into them for every value. */
    private final ValueStream.Reader dictionary;
    private final NumericColumnReader ordinals;
    /** Set on a dictionary column that any value escaped: their bytes, and where each one's is. */
    private final ValueStream.Reader escapes;
    private final LongValues escapeRanks;

    /** How many terms the dictionary holds, and how many values it did not name; zero on a plain column. */
    private final int dictionarySize;
    private final long escapeCount;
    private final int blockSize;

    /** The last value {@link #escapeRankOf} answered, and its rank, so an ascending pass carries on. */
    private long escapeCursorAddress = -1;
    private long escapeCursorRank;

    private final BytesRef value = new BytesRef();
    private final BytesRef scratch = new BytesRef();

    /**
     * What one match decided about the last value it saw. A document holding the same value as the one
     * before it matches exactly as it did, so a run is decided once. Held per match rather than on the
     * reader, since what it remembers is the answer to one term.
     */
    private static final class LastSeen {
        private long identity = -1;
        private int length = -1;
        private boolean matched;
    }

    /**
     * A page's own storage, reused between calls. Nothing here grows with the column or the dictionary,
     * only with the largest page asked for.
     */
    private static final int MIN_PAGE_REPEAT = 2;
    private int[] pageRanks = new int[0];
    private int[] pageOrdinals = new int[0];
    private int[] pageStarts = new int[0];
    private int[] pageLengths = new int[0];
    private byte[] pageBytes = new byte[0];
    private int pageBytesLength;
    private BytesRef[] pageValues = new BytesRef[0];
    private BytesRef[] pageDictionary = new BytesRef[0];
    private int[] touched = new int[0];
    private int[] slotByOrdinal = new int[0];
    private int[] stampByOrdinal = new int[0];
    private int generation;
    private boolean directSlots;

    /** Held so a summary can be read on demand; a merge reads it, an ordinary search never does. */
    private final IndexInput data;

    public StringColumnReader(StringColumnMetadata meta, IndexInput data) throws IOException {
        this.data = data;
        assert meta.multiValued() == false : "this surface carries one value per document";
        this.meta = meta;
        this.iteratorReader = new ColumnIteratorReader(meta.iterator(), data);
        switch (meta) {
            case StringColumnMetadata.Dictionary column -> {
                this.values = null;
                this.dictionary = column.dictionary().open(data);
                this.ordinals = new NumericColumnReader(column.ordinals(), data);
                this.dictionarySize = column.dictionarySize();
                this.blockSize = column.dictionary().valuesPerBlock();
                if (column.hasEscapes()) {
                    this.escapes = column.escapes().open(data);
                    this.escapeCount = column.escapes().numValues();
                    this.escapeRanks = MonotonicReader.open(
                        data,
                        column.escapeRanks().meta(),
                        StringColumnWriter.escapeRankEntries(column.numValues()),
                        column.escapeRanks().dataOffset(),
                        column.escapeRanks().dataLength()
                    );
                } else {
                    this.escapes = null;
                    this.escapeCount = 0;
                    this.escapeRanks = null;
                }
            }
            case StringColumnMetadata.Plain column -> {
                this.values = column.numDocsWithField() == 0 ? null : column.values().open(data);
                this.dictionary = null;
                this.ordinals = null;
                this.escapes = null;
                this.escapeRanks = null;
                this.dictionarySize = 0;
                this.escapeCount = 0;
                this.blockSize = column.values().valuesPerBlock();
            }
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
            if (ordinal == dictionarySize) {
                escapes.get(escapeRankOf(valueAddress), value);
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
     *
     * <p>Values are asked for in ascending order, so counting carries on from the last value answered
     * rather than from the start of its block whenever that value is nearer. A pass over a column then
     * reads each ordinal once instead of once for every escape that shares a block with it.
     */
    private long escapeRankOf(long valueAddress) throws IOException {
        final long block = valueAddress / StringColumnWriter.ESCAPE_RANK_BLOCK;
        final long blockStart = block * StringColumnWriter.ESCAPE_RANK_BLOCK;
        long at;
        long rank;
        if (escapeCursorAddress >= blockStart && escapeCursorAddress <= valueAddress) {
            at = escapeCursorAddress;
            rank = escapeCursorRank;
        } else {
            at = blockStart;
            rank = escapeRanks.get(block);
        }
        for (; at < valueAddress; at++) {
            if (ordinals.valueAt(at) == dictionarySize) {
                rank++;
            }
        }
        escapeCursorAddress = valueAddress;
        escapeCursorRank = rank;
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
        final int size = summary.terms() == null ? dictionarySize : Math.toIntExact(summary.terms().numValues());
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
        return dictionarySize;
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
        return dictionary != null;
    }

    /** How many values the dictionary did not name. */
    public long escapeCount() {
        return escapeCount;
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

    /**
     * Documents whose value is {@code term}.
     *
     * <p>A column ranked by document id whose values are in order answers with the documents themselves,
     * and advancing it costs nothing. Every other shape answers with a check to run a document at a time,
     * carried as a {@link TwoPhaseIterator} that {@link TwoPhaseIterator#unwrap} returns. A caller working
     * one range of documents at a time has to take that check and stop where its range does: advancing the
     * plain iterator looks for a match until it finds one, so a range holding none of them pays for the
     * rest of the column.
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
     * values are looked at either way. What a dictionary column can do is look at each term once instead of
     * once a document: a term either contains it or does not, and every value naming that term inherits the
     * answer. Only a value that escaped the dictionary has to be searched on its own.
     */
    public DocIdSetIterator matchContains(BytesRef term) throws IOException {
        if (meta.numDocsWithField() == 0) {
            return DocIdSetIterator.empty();
        }
        if (hasDictionary() == false) {
            return containsInValues(term);
        }
        // One search a term rather than one a document. A term the dictionary holds is decided here, and a
        // column that let nothing escape is decided here entirely.
        final FixedBitSet matching = new FixedBitSet(dictionarySize);
        final BytesRef scratchTerm = new BytesRef();
        for (int ordinal = 0; ordinal < dictionarySize; ordinal++) {
            final BytesRef candidate = termAt(ordinal, scratchTerm);
            if (ESVectorUtil.contains(candidate.bytes, candidate.offset, candidate.length, term.bytes, term.offset, term.length)) {
                matching.set(ordinal);
            }
        }
        if (matching.cardinality() == 0 && escapeCount == 0) {
            return DocIdSetIterator.empty();
        }
        final ColumnIterator presence = iterator();
        final BytesRef value = new BytesRef();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(presence) {
            @Override
            public boolean matches() throws IOException {
                final int rank = presence.rank();
                final long first = firstValueAddress(rank);
                final long count = valueCount(rank);
                for (long i = 0; i < count; i++) {
                    final long address = first + i;
                    final int ordinal = ordinalAt(address);
                    if (ordinal != dictionarySize) {
                        if (matching.get(ordinal)) {
                            return true;
                        }
                        continue;
                    }
                    // Nothing names this value, so its own bytes are searched.
                    escapes.get(escapeRankOf(address), value);
                    if (ESVectorUtil.contains(value.bytes, value.offset, value.length, term.bytes, term.offset, term.length)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public float matchCost() {
                return 3f;
            }
        });
    }

    /** Searches the values themselves, for a column that stores them. */
    private DocIdSetIterator containsInValues(BytesRef term) throws IOException {
        final ColumnIterator presence = iterator();
        final BytesRef value = new BytesRef();
        final LastSeen lastSeen = new LastSeen();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(presence) {
            @Override
            public boolean matches() throws IOException {
                final int rank = presence.rank();
                final long first = firstValueAddress(rank);
                final long count = valueCount(rank);
                if (count == 1) {
                    // A value repeating the one before it contains what it contained.
                    final long identity = values.read(first, value);
                    if (identity == lastSeen.identity && value.length == lastSeen.length) {
                        return lastSeen.matched;
                    }
                    final boolean matched = ESVectorUtil.contains(
                        value.bytes,
                        value.offset,
                        value.length,
                        term.bytes,
                        term.offset,
                        term.length
                    );
                    lastSeen.identity = identity;
                    lastSeen.length = value.length;
                    lastSeen.matched = matched;
                    return matched;
                }
                for (long i = 0; i < count; i++) {
                    values.get(first + i, value);
                    if (ESVectorUtil.contains(value.bytes, value.offset, value.length, term.bytes, term.offset, term.length)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public float matchCost() {
                return 10f;
            }
        });
    }

    /**
     * Documents whose value equals {@code exact}, or starts with {@code prefix} when {@code exact} is null.
     *
     * <p>Three ways of answering it, in the order they are worth taking. A column written in term order puts
     * every match in one run, so bisecting the values finds its ends and the answer costs the column's
     * logarithm. A dictionary column matches over ordinals, comparing an int per value rather than bytes.
     * Anything else compares the values themselves, a window at a time.
     */
    private DocIdSetIterator matching(BytesRef prefix, BytesRef exact) throws IOException {
        if (meta.numDocsWithField() == 0) {
            return DocIdSetIterator.empty();
        }
        if (meta.valuesSorted() && meta.multiValued() == false) {
            return documents(sortedRange(prefix, exact));
        }
        if (hasDictionary()) {
            return ordinalMatches(prefix, exact);
        }
        return scanValues(prefix, exact);
    }

    /**
     * The ranks holding the term, or the prefix, in a column whose values arrive in order. Their ends are
     * found by bisection over the values, which needs only the order and no ordinals: a term costs a couple
     * of dozen block reads instead of a comparison per document.
     */
    private DocIdSetIterator sortedRange(BytesRef prefix, BytesRef exact) throws IOException {
        final int count = meta.numDocsWithField();
        final BytesRef target = exact != null ? exact : prefix;
        final int first = firstAtLeast(target, count);
        if (first == count) {
            return DocIdSetIterator.empty();
        }
        if (matches(valueAt(first), prefix, exact) == false) {
            return DocIdSetIterator.empty();
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
        return DocIdSetIterator.range(first, low);
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

    /**
     * Matches over the ordinals of a dictionary column. The dictionary is in term order, so a term is one
     * ordinal and a prefix is a range of them, both found by bisecting the dictionary rather than the
     * column. A value the dictionary holds never escaped, so when the term is in it the ordinals answer
     * completely; a term it does not hold can only be among the escapes, which are read as values.
     */
    private DocIdSetIterator ordinalMatches(BytesRef prefix, BytesRef exact) throws IOException {
        final int size = dictionarySize;
        final BytesRef target = exact != null ? exact : prefix;
        final int from = firstTermAtLeast(target, size);
        int to = from;
        final BytesRef term = new BytesRef();
        while (to < size && matches(termAt(to, term), prefix, exact)) {
            to++;
        }
        final int lowOrdinal = from;
        final int highOrdinal = to;
        // Nothing in the dictionary matches, and nothing escaped, so nothing can.
        if (lowOrdinal == highOrdinal && escapeCount == 0) {
            return DocIdSetIterator.empty();
        }
        final ColumnIterator presence = iterator();
        final BytesRef value = new BytesRef();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(presence) {
            private final OrdinalBlockMask mask = new OrdinalBlockMask(lowOrdinal, highOrdinal);

            @Override
            public boolean matches() throws IOException {
                return matchesRank(presence.rank(), value, prefix, exact, lowOrdinal, highOrdinal, null);
            }

            @Override
            public float matchCost() {
                return 3f;
            }

            /**
             * A window of documents confirmed a block of ordinals at a time. Whether an ordinal falls in the
             * matching range is one comparison against a range, so a block of them is one vectorized pass
             * rather than a value read and a comparison for every document.
             *
             * <p>A column that let values escape keeps the per-document path: an escaped ordinal says only
             * that the value is elsewhere, and its bytes still have to be read to know whether it matches.
             */
            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                if (escapeCount > 0) {
                    super.intoBitSet(upTo, bitSet, offset);
                    return;
                }
                int doc = presence.docID();
                while (doc < upTo && doc != DocIdSetIterator.NO_MORE_DOCS) {
                    final int rank = presence.rank();
                    if (mask.covers(rank) == false) {
                        mask.load(rank);
                    }
                    if (mask.matches(rank)) {
                        bitSet.set(doc - offset);
                    }
                    doc = presence.nextDoc();
                }
            }
        });
    }

    /**
     * The ordinals of one block, as a bit a value saying whether it falls in a range. Loaded a block at a
     * time so the range test is one vectorized pass over the block rather than one comparison a document,
     * and kept until a document lands outside it.
     */
    private final class OrdinalBlockMask {
        private final FixedBitSet matches;
        private final long lowOrdinal;
        private final long highOrdinal;
        private final int blockShift;
        private final int blockMask;
        private long loaded = -1;

        OrdinalBlockMask(int lowOrdinal, int highOrdinal) {
            this.lowOrdinal = lowOrdinal;
            // inRangeBitmask takes an inclusive upper bound, where the ordinal range is exclusive.
            this.highOrdinal = highOrdinal - 1L;
            this.blockShift = Integer.numberOfTrailingZeros(ordinals.blockSize());
            this.blockMask = ordinals.blockSize() - 1;
            this.matches = new FixedBitSet(ordinals.blockSize());
        }

        boolean covers(long valueAddress) {
            return (valueAddress >>> blockShift) == loaded;
        }

        void load(long valueAddress) throws IOException {
            final long blockIndex = valueAddress >>> blockShift;
            matches.clear();
            ESVectorUtil.inRangeBitmask(ordinals.block(blockIndex), lowOrdinal, highOrdinal, matches.getBits());
            loaded = blockIndex;
        }

        boolean matches(long valueAddress) {
            return matches.get((int) (valueAddress & blockMask));
        }
    }

    /** The first ordinal whose term sorts at or after {@code target}, by bisection over the dictionary. */
    private int firstTermAtLeast(BytesRef target, int size) throws IOException {
        final BytesRef term = new BytesRef();
        int low = 0;
        int high = size;
        while (low < high) {
            final int mid = (low + high) >>> 1;
            if (termAt(mid, term).compareTo(target) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    /**
     * Compares the values, for a column with no order to bisect and no ordinals to match instead. A
     * two-phase iterator, so a scorer fills a window at a time rather than asking one document at a time.
     */
    private DocIdSetIterator scanValues(BytesRef prefix, BytesRef exact) throws IOException {
        final ColumnIterator presence = iterator();
        final BytesRef value = new BytesRef();
        final LastSeen lastSeen = new LastSeen();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(presence) {
            @Override
            public boolean matches() throws IOException {
                return matchesRank(presence.rank(), value, prefix, exact, -1, -1, lastSeen);
            }

            @Override
            public float matchCost() {
                return 10f;
            }

        });
    }

    /**
     * Whether any of a document's values matches. When an ordinal range is given the ordinals answer for
     * every value the dictionary holds, and only an escaped one is resolved to its bytes.
     */
    private boolean matchesRank(
        int rank,
        BytesRef value,
        BytesRef prefix,
        BytesRef exact,
        int lowOrdinal,
        int highOrdinal,
        LastSeen lastSeen
    ) throws IOException {
        final long first = firstValueAddress(rank);
        final long count = valueCount(rank);
        // A document holding the same value as the one before it matches exactly as it did. On a column of
        // runs that answers most documents without looking at a value at all.
        if (lastSeen != null && count == 1) {
            final long identity = values.read(first, scratch);
            if (identity == lastSeen.identity && scratch.length == lastSeen.length) {
                return lastSeen.matched;
            }
            final boolean matched = matches(scratch, prefix, exact);
            lastSeen.identity = identity;
            lastSeen.length = scratch.length;
            lastSeen.matched = matched;
            return matched;
        }
        for (long i = 0; i < count; i++) {
            final long address = first + i;
            if (lowOrdinal >= 0) {
                final int ordinal = ordinalAt(address);
                if (ordinal != dictionarySize) {
                    if (ordinal >= lowOrdinal && ordinal < highOrdinal) {
                        return true;
                    }
                    continue;
                }
                // Escaped, so only its bytes say what it is.
            }
            if (matches(valueAt(address), prefix, exact)) {
                return true;
            }
        }
        return false;
    }

    /** A value of the wrong length cannot be the term, and cannot be shorter than the prefix. */
    private static boolean matches(BytesRef value, BytesRef prefix, BytesRef exact) {
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
     * column. A range of ranks is what bisecting the values leaves, and it is all this needs.
     */
    private DocIdSetIterator documents(DocIdSetIterator ranks) throws IOException {
        if (meta.iterator().isDense()) {
            return ranks;
        }
        final long cost = ranks.cost();
        if (cost == 0) {
            return DocIdSetIterator.empty();
        }
        final int firstRank = Math.toIntExact(ranks.nextDoc());
        final int endRank = Math.toIntExact(firstRank + cost);
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
    private boolean ranksOfAll(int[] docs, int offset, int count) throws IOException {
        iterator().ranks(docs, offset, count, pageRanks);
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
     * in the exception stream and has to be resolved with {@link #resolveEscape}.
     *
     * <p>An ordinal names a term of this column's own dictionary. Another column carries its own, so the
     * same ordinal in two of them is two different terms and counting them together would add up values
     * that are not the same.
     *
     * @return false when the column has no dictionary, so there are no ordinals to serve
     */
    public boolean readOrdinals(int[] docs, int offset, int count, int[] ordinals) throws IOException {
        if (hasDictionary() == false) {
            return false;
        }
        growPage(count);
        if (ranksOfAll(docs, offset, count) == false) {
            return false;
        }
        for (int i = 0; i < count; i++) {
            ordinals[i] = Math.toIntExact(this.ordinals.valueAt(pageRanks[i]));
        }
        return true;
    }

    /** The value behind the escape marker at {@code valueAddress}, for a consumer that took {@link #readOrdinals}. */
    public BytesRef resolveEscape(long valueAddress, BytesRef dst) throws IOException {
        escapes.get(escapeRankOf(valueAddress), dst);
        return dst;
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

        if (hasDictionary() == false) {
            // A run is stored once, so consecutive values of it answer with the same token and only the
            // first is copied into the page. A column sorted on this field is made of runs, and this is
            // where that pays: one entry a run rather than one a document, without comparing any bytes.
            pageBytesLength = 0;
            int runs = 0;
            long previous = -1;
            int previousLength = -1;
            for (int i = 0; i < count; i++) {
                final long identity = values.read(pageRanks[i], scratch);
                if (runs == 0 || identity != previous || scratch.length != previousLength) {
                    appendToPage(runs++, scratch);
                    previous = identity;
                    previousLength = scratch.length;
                }
                pageOrdinals[i] = runs - 1;
            }
            point(pageDictionary, runs);
            // As many entries as documents is no shorter as ordinals than as values.
            if ((long) runs * MIN_PAGE_REPEAT > count) {
                for (int i = 0; i < count; i++) {
                    pageValues[i] = pageDictionary[pageOrdinals[i]];
                }
                sink.appendValues(pageValues, count);
                return true;
            }
            sink.appendOrdinals(pageOrdinals, count, pageDictionary, runs);
            return true;
        }

        int escapedInPage = 0;
        for (int i = 0; i < count; i++) {
            final int ordinal = Math.toIntExact(ordinals.valueAt(pageRanks[i]));
            pageOrdinals[i] = ordinal;
            if (ordinal >= dictionarySize) {
                escapedInPage++;
            }
        }

        // The ordinals this page holds, each once and in order. Ordered because the dictionary is read
        // forward: resolving in whatever order the documents happen to be in would re-enter its blocks, and
        // a block is decoded every time it is re-entered.
        final int distinct = distinctOrdinals(count, dictionarySize);

        pageBytesLength = 0;
        int slot = 0;
        for (; slot < distinct; slot++) {
            dictionary.get(touched[slot], scratch);
            appendToPage(slot, scratch);
        }
        for (int i = 0; i < count; i++) {
            final int ordinal = pageOrdinals[i];
            if (ordinal < dictionarySize) {
                pageOrdinals[i] = slotOf(ordinal, distinct);
            } else {
                // Every escaped document is its own entry: nothing names it but its bytes.
                escapes.get(escapeRankOf(pageRanks[i]), scratch);
                appendToPage(slot, scratch);
                pageOrdinals[i] = slot++;
            }
        }
        point(pageDictionary, slot);

        // A page with as many entries as documents is no shorter as ordinals than as values.
        if ((long) (distinct + escapedInPage) * MIN_PAGE_REPEAT > count) {
            for (int i = 0; i < count; i++) {
                pageValues[i] = pageDictionary[pageOrdinals[i]];
            }
            sink.appendValues(pageValues, count);
            return true;
        }
        sink.appendOrdinals(pageOrdinals, count, pageDictionary, slot);
        return true;
    }

    /**
     * The distinct dictionary ordinals the page holds, ascending, left in {@link #touched}, returning how
     * many there are. A dictionary no larger than the page is indexed directly and stamped with the page it
     * was written for, so it never has to be cleared; a larger one is not indexed at all and the page's own
     * ordinals are sorted instead. Either way nothing here grows with the dictionary beyond the page.
     */
    private int distinctOrdinals(int count, int dictionarySize) {
        if (touched.length < count) {
            touched = new int[count];
        }
        int distinct = 0;
        if (dictionarySize <= count) {
            if (slotByOrdinal.length < dictionarySize) {
                slotByOrdinal = new int[dictionarySize];
                stampByOrdinal = new int[dictionarySize];
                generation = 0;
            }
            if (++generation == Integer.MAX_VALUE) {
                Arrays.fill(stampByOrdinal, 0);
                generation = 1;
            }
            for (int i = 0; i < count; i++) {
                final int ordinal = pageOrdinals[i];
                if (ordinal < dictionarySize && stampByOrdinal[ordinal] != generation) {
                    stampByOrdinal[ordinal] = generation;
                    touched[distinct++] = ordinal;
                }
            }
            Arrays.sort(touched, 0, distinct);
            for (int i = 0; i < distinct; i++) {
                slotByOrdinal[touched[i]] = i;
            }
            directSlots = true;
            return distinct;
        }
        for (int i = 0; i < count; i++) {
            if (pageOrdinals[i] < dictionarySize) {
                touched[distinct++] = pageOrdinals[i];
            }
        }
        Arrays.sort(touched, 0, distinct);
        int unique = 0;
        for (int i = 0; i < distinct; i++) {
            if (i == 0 || touched[i] != touched[i - 1]) {
                touched[unique++] = touched[i];
            }
        }
        directSlots = false;
        return unique;
    }

    /** Where the page put the value for {@code ordinal}, which {@link #distinctOrdinals} accounted for. */
    private int slotOf(int ordinal, int distinct) {
        return directSlots ? slotByOrdinal[ordinal] : Arrays.binarySearch(touched, 0, distinct, ordinal);
    }

    /** Copies a value into the page's own bytes, so the reader's buffer can be reused for the next one. */
    private void appendToPage(int slot, BytesRef value) {
        if (pageBytes.length < pageBytesLength + value.length) {
            pageBytes = ArrayUtil.grow(pageBytes, pageBytesLength + value.length);
        }
        System.arraycopy(value.bytes, value.offset, pageBytes, pageBytesLength, value.length);
        pageStarts[slot] = pageBytesLength;
        pageLengths[slot] = value.length;
        pageBytesLength += value.length;
    }

    /** Points {@code into} at the page's bytes, once they have stopped moving. */
    private void point(BytesRef[] into, int count) {
        for (int i = 0; i < count; i++) {
            into[i].bytes = pageBytes;
            into[i].offset = pageStarts[i];
            into[i].length = pageLengths[i];
        }
    }

    private void growPage(int count) {
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
