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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.numeric.NumericColumnReader;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.MonotonicReader;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Predicate;

/**
 * A column that names its values with ordinals into a dictionary of terms. An ordinal is stable for the
 * whole column, so a filter is decided over the dictionary once and then over ints, and a page can hand a
 * consumer ordinals instead of bytes.
 *
 * <p>A value the dictionary has no term for escapes it: the ordinal records that, and the bytes live in a
 * stream of their own. Everything here has to allow for that, since an escaped value can only be decided by
 * reading it.
 */
public final class DictionaryStringColumnReader extends StringColumnReader {

    /** The terms, and an ordinal into them for every value. */
    private final ValueStream.Reader dictionary;
    private final NumericColumnReader ordinals;
    /** Set when any value escaped the dictionary: their bytes, and where each one's is. */
    private final ValueStream.Reader escapes;
    private final LongValues escapeRanks;

    private final int dictionarySize;
    private final long escapeCount;

    /** The last value {@link #escapeRankOf} answered, and its rank, so an ascending pass carries on. */
    private long escapeCursorAddress = -1;
    private long escapeCursorRank;

    /** A page's view of the dictionary: which ordinals it holds and where each one's value landed. */
    private int[] touched = new int[0];
    private int[] slotByOrdinal = new int[0];
    private int[] stampByOrdinal = new int[0];
    private int generation;
    private boolean directSlots;

    DictionaryStringColumnReader(StringColumnMetadata.Dictionary column, IndexInput data) throws IOException {
        // The ordinals are what this column addresses in blocks; the dictionary keeps one term to a block.
        super(column, data, column.ordinals().blockSize());
        this.dictionary = column.dictionary().open(data);
        this.ordinals = new NumericColumnReader(column.ordinals(), data);
        this.dictionarySize = column.dictionarySize();
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

    @Override
    public boolean hasDictionary() {
        return true;
    }

    @Override
    public int dictionarySize() {
        return dictionarySize;
    }

    @Override
    public long escapeCount() {
        return escapeCount;
    }

    @Override
    protected ValueStream.Reader summarisedTerms() {
        return dictionary;
    }

    @Override
    protected int summarisedTermCount() {
        return dictionarySize;
    }

    @Override
    public BytesRef valueAt(long valueAddress) throws IOException {
        // The ordinals are one per value in the same order, so a value address addresses them directly.
        final long ordinal = ordinals.valueAt(valueAddress);
        if (ordinal == dictionarySize) {
            escapes.get(escapeRankOf(valueAddress), value);
        } else {
            termAt((int) ordinal, value);
        }
        return value;
    }

    /** The term at {@code ordinal}. The dictionary keeps an offset for each, so its bytes are read where they lie. */
    public BytesRef termAt(int ordinal, BytesRef dst) throws IOException {
        dictionary.get(ordinal, dst);
        return dst;
    }

    /**
     * Where an escaped value's bytes are: how many escaped before it. The table gives that for the start of
     * its block and the ordinals in between give the rest, counted from the last value answered when that
     * is nearer.
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

    /**
     * The ordinal the value at {@code valueAddress} takes, or {@link #dictionarySize()} when it escaped.
     */
    public int ordinalAt(long valueAddress) throws IOException {
        return Math.toIntExact(ordinals.valueAt(valueAddress));
    }

    /** The value behind the escape marker at {@code valueAddress}, for a consumer that took ordinals. */
    public BytesRef resolveEscape(long valueAddress, BytesRef dst) throws IOException {
        escapes.get(escapeRankOf(valueAddress), dst);
        return dst;
    }

    @Override
    public boolean readOrdinals(int[] docs, int offset, int count, int[] ordinals) throws IOException {
        growPage(count);
        if (ranksOfAll(docs, offset, count) == false) {
            return false;
        }
        final OrdinalBlockCursor cursor = new OrdinalBlockCursor();
        for (int i = 0; i < count; i++) {
            ordinals[i] = cursor.at(pageRanks[i]);
        }
        return true;
    }

    /**
     * Reads ordinals a block at a time. Documents arrive in order, so a page spans a handful of blocks and
     * each is addressed once and then indexed.
     */
    private final class OrdinalBlockCursor {
        private final int blockShift = Integer.numberOfTrailingZeros(ordinals.blockSize());
        private final int blockMask = ordinals.blockSize() - 1;
        private long loaded = -1;
        private long[] block;

        int at(long valueAddress) throws IOException {
            final long blockIndex = valueAddress >>> blockShift;
            if (blockIndex != loaded) {
                block = ordinals.block(blockIndex);
                loaded = blockIndex;
            }
            return Math.toIntExact(block[(int) (valueAddress & blockMask)]);
        }
    }

    /**
     * One test a term rather than one a document. A term the dictionary holds is decided here, and a
     * column that let nothing escape is decided here entirely. Only a value that escaped has to be
     * tested on its own.
     */
    @Override
    protected DocIdSetIterator valueMatches(Predicate<BytesRef> matcher) throws IOException {
        final FixedBitSet matching = new FixedBitSet(dictionarySize);
        final BytesRef scratchTerm = new BytesRef();
        for (int ordinal = 0; ordinal < dictionarySize; ordinal++) {
            if (matcher.test(termAt(ordinal, scratchTerm))) {
                matching.set(ordinal);
            }
        }
        if (matching.cardinality() == 0 && escapeCount == 0) {
            return DocIdSetIterator.empty();
        }
        final ColumnIterator presence = iterator();
        final BytesRef value = new BytesRef();
        final OrdinalBlockMask mask = new OrdinalBlockMask(matching, escapeCount > 0);
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(presence) {
            @Override
            public boolean matches() throws IOException {
                final int rank = presence.rank();
                final long first = firstValueAddress(rank);
                final long count = valueCount(rank);
                for (long i = 0; i < count; i++) {
                    final long address = first + i;
                    if (mask.covers(address) == false) {
                        mask.load(address);
                    }
                    if (mask.matches(address)) {
                        return true;
                    }
                    if (mask.escaped(address)) {
                        // Nothing names this value, so its own bytes are tested.
                        escapes.get(escapeRankOf(address), value);
                        if (matcher.test(value)) {
                            return true;
                        }
                    }
                }
                return false;
            }

            @Override
            public float matchCost() {
                return 3f;
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                if (escapeCount > 0) {
                    super.intoBitSet(upTo, bitSet, offset);
                    return;
                }
                collectFromOrdinals(presence, mask, upTo, bitSet, offset);
            }
        });
    }

    /**
     * Fills a window from the ordinals alone, testing a decoded block of them at a time. Sound only where
     * nothing escaped: an escaped ordinal says the value is elsewhere, so its bytes still decide it.
     */
    private void collectFromOrdinals(ColumnIterator presence, OrdinalBlockMask mask, int upTo, FixedBitSet bitSet, int offset)
        throws IOException {
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

    /**
     * Matches over the ordinals. The dictionary is in term order, so a term is one ordinal and a prefix is a
     * range of them, both found by bisecting the dictionary rather than the column. A value the dictionary
     * holds never escaped, so when the term is in it the ordinals answer completely; a term it does not hold
     * can only be among the escapes, which are read as values.
     */
    @Override
    protected DocIdSetIterator unorderedMatches(BytesRef prefix, BytesRef exact) throws IOException {
        final int size = dictionarySize;
        final BytesRef target = exact != null ? exact : prefix;
        final int from = firstTermAtLeast(target, size);
        final int lowOrdinal = from;
        final int highOrdinal = endOfRun(prefix, exact, from, size);
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
                return matchesRank(presence.rank(), value, prefix, exact, lowOrdinal, highOrdinal);
            }

            @Override
            public float matchCost() {
                return 3f;
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                if (escapeCount > 0) {
                    super.intoBitSet(upTo, bitSet, offset);
                    return;
                }
                collectFromOrdinals(presence, mask, upTo, bitSet, offset);
            }
        });
    }

    /**
     * Whether any of a document's values matches. The ordinals answer for every value the dictionary holds,
     * and only an escaped one is resolved to its bytes.
     */
    private boolean matchesRank(int rank, BytesRef value, BytesRef prefix, BytesRef exact, int lowOrdinal, int highOrdinal)
        throws IOException {
        final long first = firstValueAddress(rank);
        final long count = valueCount(rank);
        for (long i = 0; i < count; i++) {
            final long address = first + i;
            final int ordinal = ordinalAt(address);
            if (ordinal != dictionarySize) {
                if (ordinal >= lowOrdinal && ordinal < highOrdinal) {
                    return true;
                }
                continue;
            }
            // Escaped, so only its bytes say what it is.
            if (matches(valueAt(address), prefix, exact)) {
                return true;
            }
        }
        return false;
    }

    /** The first ordinal whose term sorts at or after {@code target}, by bisection over the dictionary. */
    /**
     * The end of the run the target covers, as {@code [from, to)}.
     *
     * <p>A dictionary holds each term once, so a term is a run of one and the term at its start decides it. A
     * prefix covers as many terms as carry it, and the dictionary being in term order puts them in one run
     * whose end is a boundary in that order like its start, so it is bisected rather than walked. A prefix
     * that most of the vocabulary carries would otherwise cost a term read apiece.
     */
    private int endOfRun(BytesRef prefix, BytesRef exact, int from, int size) throws IOException {
        final BytesRef term = new BytesRef();
        if (exact != null) {
            return from < size && matches(termAt(from, term), prefix, exact) ? from + 1 : from;
        }
        int low = from;
        int high = size;
        while (low < high) {
            final int mid = (low + high) >>> 1;
            if (matches(termAt(mid, term), prefix, exact)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

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

    @Override
    protected boolean appendPage(int count, StringBlockSink sink) throws IOException {
        int escapedInPage = 0;
        final OrdinalBlockCursor cursor = new OrdinalBlockCursor();
        for (int i = 0; i < count; i++) {
            final int ordinal = cursor.at(pageRanks[i]);
            pageOrdinals[i] = ordinal;
            if (ordinal >= dictionarySize) {
                escapedInPage++;
            }
        }

        // The ordinals this page holds, each once and in order, so a slot can be found by bisecting them.
        final int distinct = distinctOrdinals(count, dictionarySize);

        pageBytesLength = 0;
        int slot = 0;
        for (; slot < distinct; slot++) {
            dictionary.get(touched[slot], scratch);
            appendToPage(slot, scratch);
        }
        startPageSlots(escapedInPage);
        for (int i = 0; i < count; i++) {
            final int ordinal = pageOrdinals[i];
            if (ordinal < dictionarySize) {
                pageOrdinals[i] = slotOf(ordinal, distinct);
            } else {
                // Nothing names an escaped value but its bytes, so two documents holding the same ones are
                // found to share a slot by those bytes. They cannot be found among the terms: a value
                // escaped because the vocabulary does not hold it.
                escapes.get(escapeRankOf(pageRanks[i]), scratch);
                final int found = pageSlotFor(scratch, slot);
                if (found == slot) {
                    slot++;
                }
                pageOrdinals[i] = found;
            }
        }
        point(pageDictionary, slot);

        // A page with as many entries as documents is no shorter as ordinals than as values.
        if ((long) slot * MIN_PAGE_REPEAT > count) {
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

    /**
     * The ordinals of one block, as a bit a value saying whether it is wanted. Loaded a block at a time so a
     * range is one vectorized pass over the block rather than one comparison a document, and kept until a
     * document lands outside it.
     */
    private final class OrdinalBlockMask {
        private final FixedBitSet matches;
        /** The ordinals wanted when they do not form a range, and null when {@code lowOrdinal} bounds them. */
        private final FixedBitSet selected;
        /** Where a block holds values no term names, kept only when a caller has to decide those itself. */
        private final FixedBitSet escapedAt;
        private final long lowOrdinal;
        private final long highOrdinal;
        private final int blockShift;
        private final int blockMask;
        private long loaded = -1;

        /** For a filter the dictionary answers as a range of ordinals, which is tested a block at a time. */
        OrdinalBlockMask(int lowOrdinal, int highOrdinal) {
            this(null, lowOrdinal, highOrdinal, false);
        }

        /**
         * For a filter whose terms are scattered through the dictionary. There is no range to test, but the
         * ordinals are still read a block at a time rather than one per value.
         */
        OrdinalBlockMask(FixedBitSet selected, boolean markEscapes) {
            this(selected, 0, 0, markEscapes);
        }

        private OrdinalBlockMask(FixedBitSet selected, int lowOrdinal, int highOrdinal, boolean markEscapes) {
            this.selected = selected;
            this.lowOrdinal = lowOrdinal;
            // inRangeBitmask takes an inclusive upper bound, where the ordinal range is exclusive.
            this.highOrdinal = highOrdinal - 1L;
            this.blockShift = Integer.numberOfTrailingZeros(ordinals.blockSize());
            this.blockMask = ordinals.blockSize() - 1;
            this.matches = new FixedBitSet(ordinals.blockSize());
            this.escapedAt = markEscapes ? new FixedBitSet(ordinals.blockSize()) : null;
        }

        boolean covers(long valueAddress) {
            return (valueAddress >>> blockShift) == loaded;
        }

        void load(long valueAddress) throws IOException {
            final long blockIndex = valueAddress >>> blockShift;
            matches.clear();
            final long[] block = ordinals.block(blockIndex);
            if (selected == null) {
                ESVectorUtil.inRangeBitmask(block, lowOrdinal, highOrdinal, matches.getBits());
            } else {
                if (escapedAt != null) {
                    escapedAt.clear();
                }
                for (int i = 0; i < block.length; i++) {
                    final long ordinal = block[i];
                    if (ordinal < dictionarySize) {
                        if (selected.get((int) ordinal)) {
                            matches.set(i);
                        }
                    } else if (escapedAt != null && ordinal == dictionarySize) {
                        escapedAt.set(i);
                    }
                }
            }
            loaded = blockIndex;
        }

        boolean matches(long valueAddress) {
            return matches.get((int) (valueAddress & blockMask));
        }

        /** Whether nothing names the value at {@code valueAddress}, so its own bytes have to decide it. */
        boolean escaped(long valueAddress) {
            return escapedAt != null && escapedAt.get((int) (valueAddress & blockMask));
        }
    }
}
