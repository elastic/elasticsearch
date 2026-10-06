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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.numeric.NumericColumnReader;
import org.elasticsearch.columnar.substrate.ColumnInputs;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.MonotonicReader;

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
 *
 * <p>A null is named too, by {@link StringColumnMetadata.Dictionary#NULL_ORDINAL}, so the terms run from
 * {@link StringColumnMetadata.Dictionary#FIRST_TERM_ORDINAL} up and {@link #escapeOrdinal} follows them. A
 * null is therefore in no term's ordinal range and among no term's escapes, which is what lets a filter
 * resolved against the dictionary answer from the ordinals alone without ever confusing a null with the
 * empty term.
 */
public final class DictionaryStringColumnReader extends StringColumnReader {

    /** The terms, and an ordinal into them for every value. */
    private final ValueStream.Reader dictionary;
    private final NumericColumnReader ordinals;
    /** Set when any value escaped the dictionary: their bytes, and where each one's is. */
    private final ValueStream.Reader escapes;
    private final LongValues escapeRanks;
    /** Values between entries in {@link #escapeRanks}, as the column recorded it. */
    private final int escapeRankBlockSize;

    private final int dictionarySize;
    /** The ordinal marking a value no term names, one past the last term. */
    private final int escapeOrdinal;
    private final long escapeCount;

    /** The last value {@link #escapeRankOf} answered, and its rank, so an ascending pass carries on. */
    private long escapeCursorAddress = -1;
    private long escapeCursorRank;

    private final PageTerms pageTerms = new PageTerms();

    DictionaryStringColumnReader(StringColumnMetadata.Dictionary column, ColumnInputs inputs) throws IOException {
        // The ordinals are what this column addresses in blocks; the dictionary keeps one term to a block.
        super(column, inputs, column.ordinals().blockSize());
        this.dictionary = column.dictionary().open(inputs);
        this.ordinals = new NumericColumnReader(column.ordinals(), inputs);
        this.dictionarySize = column.dictionarySize();
        this.escapeOrdinal = column.escapeOrdinal();
        if (column.hasEscapes()) {
            this.escapes = column.escapes().open(inputs);
            this.escapeCount = column.escapes().numValues();
            this.escapeRankBlockSize = column.escapeRankBlockSize();
            this.escapeRanks = MonotonicReader.open(
                inputs.navigation(),
                column.escapeRanks().meta(),
                StringColumnWriter.escapeRankEntries(column.numValues(), escapeRankBlockSize),
                column.escapeRanks().dataOffset(),
                column.escapeRanks().dataLength()
            );
        } else {
            this.escapes = null;
            this.escapeCount = 0;
            this.escapeRankBlockSize = 0;
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

    /**
     * The ordinal marking a value no term names, one past the last term. Nothing between
     * {@link StringColumnMetadata.Dictionary#FIRST_TERM_ORDINAL} and this names anything but a term, so a
     * caller sizing a table over the terms can pin it against this rather than rebuild the arithmetic.
     */
    public int escapeOrdinal() {
        return escapeOrdinal;
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

    /** Whether the slot is null, which the ordinal already read to resolve the value settles outright. */
    @Override
    public boolean isNullSlot(long valueAddress) throws IOException {
        return ordinals.valueAt(valueAddress) == StringColumnMetadata.Dictionary.NULL_ORDINAL;
    }

    @Override
    public BytesRef valueAt(long valueAddress) throws IOException {
        // The ordinals are one per value in the same order, so a value address addresses them directly.
        final long ordinal = ordinals.valueAt(valueAddress);
        if (ordinal == StringColumnMetadata.Dictionary.NULL_ORDINAL) {
            return null;
        }
        if (ordinal == escapeOrdinal) {
            escapes.get(escapeRankOf(valueAddress), value);
        } else {
            termAt((int) ordinal, value);
        }
        return value;
    }

    /** Every ordinal but the one naming a null: a term's, or the one marking an escaped value. */
    @Override
    protected SlotWindow nonNullSlots() {
        return new SlotWindow(SlotBlocks.of(ordinals), StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL, escapeOrdinal);
    }

    /**
     * The length of the value at {@code valueAddress}, read off the term the ordinal names, or off the
     * escaped bytes where the slot escaped.
     */
    @Override
    public int byteLengthAt(long valueAddress) throws IOException {
        final int ordinal = ordinalAt(valueAddress);
        assert ordinal != StringColumnMetadata.Dictionary.NULL_ORDINAL : "a null slot holds no value to measure";
        if (ordinal == escapeOrdinal) {
            escapes.get(escapeRankOf(valueAddress), lengthScratch);
            return lengthScratch.length;
        }
        // A term's bytes are stored as they are, so reading one decodes nothing.
        return termAt(ordinal, lengthScratch).length;
    }

    /** Where {@link #byteLengthAt} reads a value it only measures. */
    private final BytesRef lengthScratch = new BytesRef();

    /**
     * The term at {@code ordinal}. The dictionary keeps an offset for each, so its bytes are read where they
     * lie. The {@link #dictionarySize()} terms take the ordinals from
     * {@link StringColumnMetadata.Dictionary#FIRST_TERM_ORDINAL} up, so the term a caller wants the
     * {@code i}th of is at {@code FIRST_TERM_ORDINAL + i}.
     */
    public BytesRef termAt(int ordinal, BytesRef dst) throws IOException {
        assert ordinal >= StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL
            && ordinal < dictionarySize + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL
            : "ordinal [" + ordinal + "] names no term in a dictionary of " + dictionarySize;
        dictionary.get(ordinal - StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL, dst);
        return dst;
    }

    /**
     * Where an escaped value's bytes are: how many escaped before it. The table gives that for the start of
     * its block and the ordinals in between give the rest, counted from the last value answered when that
     * is nearer.
     */
    private long escapeRankOf(long valueAddress) throws IOException {
        final long block = valueAddress / escapeRankBlockSize;
        final long blockStart = block * escapeRankBlockSize;
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
            if (ordinals.valueAt(at) == escapeOrdinal) {
                rank++;
            }
        }
        escapeCursorAddress = valueAddress;
        escapeCursorRank = rank;
        return rank;
    }

    /**
     * The ordinal the value at {@code valueAddress} takes: a term's,
     * {@link StringColumnMetadata.Dictionary#NULL_ORDINAL} when the slot is null, or {@link #escapeOrdinal}
     * when it escaped.
     */
    public int ordinalAt(long valueAddress) throws IOException {
        return Math.toIntExact(ordinals.valueAt(valueAddress));
    }

    /**
     * The extreme value, decided over ordinals. The dictionary is in term order, so the largest ordinal a document
     * holds names its largest value and the smallest its smallest - the terms are never read to find out, and only the
     * one that wins is resolved.
     *
     * <p>Two slots do not order that way. A null is no value, so it is passed over. An escaped value sorts wherever
     * its bytes do, which its ordinal - one past every term - does not say, so a document holding one is decided by
     * comparing bytes after all.
     */
    @Override
    public BytesRef extreme(int rank, boolean max, BytesRef dst) throws IOException {
        final long first = firstValueAddress(rank);
        final long count = valueCount(rank);
        int best = -1;
        for (long i = 0; i < count; i++) {
            final int ordinal = ordinalAt(first + i);
            if (ordinal == StringColumnMetadata.Dictionary.NULL_ORDINAL) {
                continue;
            }
            if (ordinal == escapeOrdinal) {
                return super.extreme(rank, max, dst);
            }
            if (best < 0 || (max ? ordinal > best : ordinal < best)) {
                best = ordinal;
            }
        }
        return best < 0 ? null : termAt(best, dst);
    }

    /** The value behind the escape marker at {@code valueAddress}, for a consumer that took ordinals. */
    public BytesRef resolveEscape(long valueAddress, BytesRef dst) throws IOException {
        escapes.get(escapeRankOf(valueAddress), dst);
        return dst;
    }

    @Override
    public boolean readOrdinals(int[] docs, int offset, int count, int[] ordinals) throws IOException {
        if (pageable() == false) {
            // One ordinal a document, and every ordinal a term's: neither holds on a column whose documents
            // carry several slots, nor on one where a slot may be the reserved null, which names no term.
            return false;
        }
        growPageDocs(count);
        growPageValues(count);
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
        // Indexed by column ordinal, so a block of them selects straight into it. The reserved null keeps
        // its bit clear throughout, which is what stops a null answering for whatever the matcher accepts.
        final FixedBitSet matching = new FixedBitSet(dictionarySize + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL);
        final BytesRef scratchTerm = new BytesRef();
        for (int i = 0; i < dictionarySize; i++) {
            final int ordinal = StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL + i;
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
        final SlotFold fold = new SlotFold();
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
                collectFromOrdinals(presence, mask, fold, upTo, bitSet, offset);
            }
        });
    }

    /**
     * Fills a window from the ordinals alone, a block of them at a time. Valid only where no escaped value can match,
     * since the escape ordinal says nothing about the bytes behind it.
     */
    private void collectFromOrdinals(
        ColumnIterator presence,
        OrdinalBlockMask mask,
        SlotFold fold,
        int upTo,
        FixedBitSet bitSet,
        int offset
    ) throws IOException {
        if (presence.docID() < upTo) {
            fold.collect(presence, mask::into, upTo, bitSet, offset);
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
        // Bisected in column ordinals, so the range these produce needs no shifting to test a block of them
        // against — and cannot reach the reserved null, which sorts below every term by construction.
        final int end = dictionarySize + StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL;
        final BytesRef target = exact != null ? exact : prefix;
        final int from = firstTermAtLeast(target, end);
        final int lowOrdinal = from;
        final int highOrdinal = endOfRun(prefix, exact, from, end);
        // A value escapes only when no term names it, so an escaped value is never a term the dictionary
        // holds. An exact term that is in the dictionary is decided by the ordinals alone. A prefix, or an
        // exact term the dictionary does not hold, can still be carried by an escaped value.
        final boolean escapesCanMatch = escapeCount > 0 && (exact == null || lowOrdinal == highOrdinal);
        // Nothing in the dictionary matches, and no escape can, so nothing can.
        if (lowOrdinal == highOrdinal && escapesCanMatch == false) {
            return DocIdSetIterator.empty();
        }
        // The approximation is the slots whose ordinal is in the run, and the escaped ones when an escape can
        // carry the target; a block of ordinals is tested at once. When no escape can, the ordinals settle it.
        final SlotWindow window = escapesCanMatch
            ? new SlotWindow(SlotBlocks.of(ordinals), lowOrdinal, highOrdinal - 1L, escapeOrdinal, escapeOrdinal)
            : new SlotWindow(SlotBlocks.of(ordinals), lowOrdinal, highOrdinal - 1L);
        final Slots candidates = slotsHeld(window);
        final BytesRef value = new BytesRef();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(candidates) {
            @Override
            public boolean matches() throws IOException {
                if (escapesCanMatch == false) {
                    return true;
                }
                final long first = candidates.firstSlot();
                final long count = candidates.slotCount();
                for (long i = 0; i < count; i++) {
                    final long address = first + i;
                    if (window.holds(address) == false) {
                        continue;
                    }
                    if (ordinalAt(address) != escapeOrdinal) {
                        return true;
                    }
                    // Escaped, so only its bytes say what it is.
                    escapes.get(escapeRankOf(address), value);
                    if (StringColumnReader.matches(value, prefix, exact)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public float matchCost() {
                return escapesCanMatch ? 3f : 0f;
            }

            @Override
            public int docIDRunEnd() throws IOException {
                // Settled by the window, so every document of a run it holds matches.
                return escapesCanMatch == false ? candidates.docIDRunEnd() : super.docIDRunEnd();
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                if (escapesCanMatch) {
                    super.intoBitSet(upTo, bitSet, offset);
                } else {
                    candidates.intoBitSet(upTo, bitSet, offset);
                }
            }
        });
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
    private int endOfRun(BytesRef prefix, BytesRef exact, int from, int end) throws IOException {
        final BytesRef term = new BytesRef();
        if (exact != null) {
            return from < end && matches(termAt(from, term), prefix, exact) ? from + 1 : from;
        }
        int low = from;
        int high = end;
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

    private int firstTermAtLeast(BytesRef target, int end) throws IOException {
        final BytesRef term = new BytesRef();
        int low = StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL;
        int high = end;
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
    protected boolean appendPage(int docCount, StringBlockSink sink) throws IOException {
        // Where the page's values are, as addresses. One a document where the column holds one apiece, and otherwise
        // a document's run of them with its nulls left out, which are no value a page can carry.
        final int values;
        final boolean oneApiece = pageOfOneApiece();
        if (pageable()) {
            // One value a document: a rank is its value's address, and a document without a value holds none.
            values = oneApiece ? docCount : compactPresentRanks(docCount);
            growPageValues(Math.max(values, 1));
            for (int i = 0; i < values; i++) {
                pageValueAddresses[i] = pageRanks[i];
            }
        } else {
            values = countPageValues(docCount);
            growPageValues(Math.max(values, 1));
            int at = 0;
            for (int i = 0; i < docCount; i++) {
                if (pageRanks[i] == ColumnIterator.NO_RANK) {
                    continue;
                }
                final long first = firstValueAddress(pageRanks[i]);
                final long held = valueCount(pageRanks[i]);
                for (long slotOf = 0; slotOf < held; slotOf++) {
                    final long address = first + slotOf;
                    if (isNullSlot(address) == false) {
                        pageValueAddresses[at++] = address;
                    }
                }
            }
            assert at == values : "addressed " + at + " values, counted " + values;
        }
        final int[] counts = oneApiece ? null : pageValueCounts;

        int escapedInPage = 0;
        final OrdinalBlockCursor cursor = new OrdinalBlockCursor();
        for (int i = 0; i < values; i++) {
            final int ordinal = cursor.at(pageValueAddresses[i]);
            pageOrdinals[i] = ordinal;
            if (ordinal >= escapeOrdinal) {
                escapedInPage++;
            }
        }

        // The terms this page holds, each once and in term order: a term's place among them is its slot.
        final int distinct = pageTerms.collect(values);

        // The page takes a slot for each of those and at least one more if anything escaped. Where that alone is
        // too many for ordinals to be worth it, the page is going to be handed over as values whatever the escaped
        // ones turn out to hold, so none of it needs naming.
        final long fewestSlots = distinct + (escapedInPage > 0 ? 1 : 0);
        if (fewestSlots * MIN_PAGE_REPEAT > values) {
            try (StringBlockSink.Values out = sink.values(values, counts, docCount)) {
                for (int i = 0; i < values; i++) {
                    final int ordinal = pageOrdinals[i];
                    if (ordinal < escapeOrdinal) {
                        termAt(ordinal, scratch);
                    } else {
                        escapes.get(escapeRankOf(pageValueAddresses[i]), scratch);
                    }
                    out.append(scratch);
                }
                out.finish();
            }
            return true;
        }

        pageBytesLength = 0;
        int slot = 0;
        for (; slot < distinct; slot++) {
            termAt(pageTerms.ordinalAt(slot), scratch);
            appendToPage(slot, scratch);
        }
        startPageSlots(escapedInPage);
        for (int i = 0; i < values; i++) {
            final int ordinal = pageOrdinals[i];
            if (ordinal < escapeOrdinal) {
                pageOrdinals[i] = pageTerms.slotOf(ordinal);
            } else {
                // Nothing names an escaped value but its bytes, so two documents holding the same ones are
                // found to share a slot by those bytes. They cannot be found among the terms: a value
                // escaped because the vocabulary does not hold it.
                escapes.get(escapeRankOf(pageValueAddresses[i]), scratch);
                final int found = pageSlotFor(scratch, slot);
                if (found == slot) {
                    slot++;
                }
                pageOrdinals[i] = found;
            }
        }
        point(pageDictionary, slot);

        // A page with as many entries as values is no shorter as ordinals than as values.
        if ((long) slot * MIN_PAGE_REPEAT > values) {
            for (int i = 0; i < values; i++) {
                pageValues[i] = pageDictionary[pageOrdinals[i]];
            }
            appendGathered(sink, values, counts, docCount);
            return true;
        }
        sink.appendOrdinals(pageOrdinals, values, counts, docCount, pageDictionary, slot);
        return true;
    }

    /**
     * The terms a page holds and the slot the page gives each, found by the term's ordinal. The slots follow term
     * order. A dictionary no larger than the page is indexed directly and a larger one is hashed, so neither grows
     * past the page, and both are stamped with the page they were filled for rather than cleared between pages.
     */
    private final class PageTerms {
        /** The page's distinct term ordinals, ascending: the ordinal at an index is the term in that slot. */
        private int[] ordinals = new int[0];

        /** Indexed by term, for a dictionary no larger than the page. */
        private int[] slotByTerm = new int[0];
        private int[] stampByTerm = new int[0];

        /** Open addressing over a power of two entries at most half full, for a larger dictionary. */
        private int[] hashedOrdinal = new int[0];
        private int[] hashedSlot = new int[0];
        private int[] hashedStamp = new int[0];
        private int hashShift;

        private int generation;
        private boolean direct;

        /** Finds the distinct terms among {@code pageOrdinals[0..count)}, gives each its slot, and answers how many. */
        int collect(int count) {
            if (ordinals.length < count) {
                charge((long) (count - ordinals.length) * Integer.BYTES);
                ordinals = new int[count];
            }
            direct = dictionarySize <= count;
            if (direct) {
                growDirect();
            } else {
                growHashed(count);
            }
            if (++generation == Integer.MAX_VALUE) {
                Arrays.fill(stampByTerm, 0);
                Arrays.fill(hashedStamp, 0);
                generation = 1;
            }
            int distinct = 0;
            for (int i = 0; i < count; i++) {
                final int ordinal = pageOrdinals[i];
                if (ordinal < escapeOrdinal && mark(ordinal)) {
                    ordinals[distinct++] = ordinal;
                }
            }
            Arrays.sort(ordinals, 0, distinct);
            for (int slot = 0; slot < distinct; slot++) {
                place(ordinals[slot], slot);
            }
            return distinct;
        }

        int ordinalAt(int slot) {
            return ordinals[slot];
        }

        /** The slot of a term {@link #collect} found in the page. */
        int slotOf(int ordinal) {
            return direct ? slotByTerm[termOf(ordinal)] : hashedSlot[find(ordinal)];
        }

        /** Marks a term as held by the page and answers whether this is the first time. */
        private boolean mark(int ordinal) {
            if (direct) {
                final int term = termOf(ordinal);
                if (stampByTerm[term] == generation) {
                    return false;
                }
                stampByTerm[term] = generation;
                return true;
            }
            final int at = find(ordinal);
            if (hashedStamp[at] == generation) {
                return false;
            }
            hashedStamp[at] = generation;
            hashedOrdinal[at] = ordinal;
            return true;
        }

        private void place(int ordinal, int slot) {
            if (direct) {
                slotByTerm[termOf(ordinal)] = slot;
            } else {
                hashedSlot[find(ordinal)] = slot;
            }
        }

        /** The entry holding {@code ordinal} for this page, or the free one it would take. */
        private int find(int ordinal) {
            final int mask = hashedOrdinal.length - 1;
            int at = (ordinal * 0x9E3779B9) >>> hashShift;
            while (hashedStamp[at] == generation && hashedOrdinal[at] != ordinal) {
                at = (at + 1) & mask;
            }
            return at;
        }

        private int termOf(int ordinal) {
            return ordinal - StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL;
        }

        private void growDirect() {
            if (slotByTerm.length < dictionarySize) {
                charge(2L * (dictionarySize - slotByTerm.length) * Integer.BYTES);
                slotByTerm = new int[dictionarySize];
                stampByTerm = new int[dictionarySize];
            }
        }

        private void growHashed(int count) {
            // At least two entries a value, so a probe is short.
            final int capacity = Math.max(16, Integer.highestOneBit(count) << 2);
            if (hashedOrdinal.length < capacity) {
                charge(3L * (capacity - hashedOrdinal.length) * Integer.BYTES);
                hashedOrdinal = new int[capacity];
                hashedSlot = new int[capacity];
                hashedStamp = new int[capacity];
                hashShift = Integer.SIZE - Integer.numberOfTrailingZeros(capacity);
            }
        }
    }

    /**
     * The ordinals of one block, as a bit a value saying whether its term is among those wanted. Loaded a block
     * at a time and kept until a document lands outside it.
     */
    private final class OrdinalBlockMask {
        private final FixedBitSet matches;
        /** The ordinals wanted. */
        private final FixedBitSet selected;
        /** Where a block holds values no term names, kept only when a caller has to decide those itself. */
        private final FixedBitSet escapedAt;
        private final int blockShift;
        private final int blockMask;
        private long loaded = -1;

        OrdinalBlockMask(FixedBitSet selected, boolean markEscapes) {
            this.selected = selected;
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
            if (escapedAt != null) {
                escapedAt.clear();
            }
            for (int i = 0; i < block.length; i++) {
                final long ordinal = block[i];
                if (ordinal < escapeOrdinal) {
                    // The reserved null indexes a bit no term ever set, so it selects nothing.
                    if (selected.get((int) ordinal)) {
                        matches.set(i);
                    }
                } else if (escapedAt != null && ordinal == escapeOrdinal) {
                    escapedAt.set(i);
                }
            }
            loaded = blockIndex;
        }

        boolean matches(long valueAddress) {
            return matches.get((int) (valueAddress & blockMask));
        }

        /** Sets the bit {@code slot - offset} in {@code dest} of every matching slot in {@code [from, to)}. */
        void into(long from, long to, FixedBitSet dest, long offset) throws IOException {
            while (from < to) {
                if (covers(from) == false) {
                    load(from);
                }
                final long blockStart = (from >>> blockShift) << blockShift;
                final long upTo = Math.min(to, blockStart + blockMask + 1);
                FixedBitSet.orRange(matches, (int) (from - blockStart), dest, (int) (from - offset), (int) (upTo - from));
                from = upTo;
            }
        }

        /** Whether nothing names the value at {@code valueAddress}, so its own bytes have to decide it. */
        boolean escaped(long valueAddress) {
            return escapedAt != null && escapedAt.get((int) (valueAddress & blockMask));
        }
    }
}
