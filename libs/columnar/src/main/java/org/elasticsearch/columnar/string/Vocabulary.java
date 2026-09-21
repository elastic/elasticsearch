/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntroSelector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The terms a column repeats often enough to be worth naming, found in one pass over its values with the
 * memory the caller's {@link DictionaryPolicy} allows.
 *
 * <p>The pass is Misra-Gries: terms are counted in a table bounded by the policy's byte budget, and when it
 * is full every term is charged the same number of occurrences and those that reach zero leave. A term is
 * only displaced by terms that between them occur more often, so the values most of the column holds
 * survive however late they first appear. The cost is exactness: every count is a lower bound.
 */
public final class Vocabulary {

    /** An id the survey saw but left out; its values escape like any unknown term. */
    public static final int DROPPED = -1;

    /** A value the table could not admit, so it has no id at all. */
    private static final int ABSENT = -1;

    private Vocabulary() {}

    /**
     * The terms a dictionary holds, in term order, and what share of the column they account for.
     *
     * @param terms           the surveyed terms, addressed by id
     * @param dictionaryIds   the kept ids in term order, so an ordinal comparison is a term comparison
     * @param summaryIds      the ids left behind for a later merge, in term order: a superset of the kept
     *                        ids, since what a merge needs to know is not what this column's dictionary
     *                        holds
     * @param ordinalOfId     an ordinal per surveyed id, or {@link #DROPPED} for one that was not kept
     * @param coverage        the share of the column's raw bytes these terms account for, as a lower bound
     * @param dictionaryBytes the term bytes the kept terms occupy
     * @param columnBytes     the value bytes the whole column occupies
     * @param counts          how often each id was seen, as a lower bound, or null when unknown
     */
    public record Terms(
        BytesRefHash terms,
        int[] dictionaryIds,
        int[] summaryIds,
        int[] ordinalOfId,
        double coverage,
        long dictionaryBytes,
        long columnBytes,
        long[] counts
    ) {
        /** Whether this vocabulary knows how often it saw each of its terms. */
        public boolean counted() {
            return counts != null;
        }

        /** How often the term at {@code ordinal} was seen, as a lower bound. */
        public long countOf(int ordinal) {
            return counts[dictionaryIds[ordinal]];
        }

        public int size() {
            return dictionaryIds.length;
        }

        /** How often the summarised term at {@code ordinal} was seen, as a lower bound. */
        public long summaryCountOf(int ordinal) {
            return counts[summaryIds[ordinal]];
        }

        public int summarySize() {
            return summaryIds.length;
        }

        /** Whether the summary holds no more than the dictionary, so the dictionary can stand for it. */
        public boolean summaryIsDictionary() {
            return summaryIds.length == dictionaryIds.length;
        }
    }

    /**
     * A vocabulary worked out from what other columns recorded rather than from values: the union of their
     * dictionaries, or the sum of their summaries. Either way their values need not be read again to
     * discover what they contain.
     *
     * @param sortedTerms the vocabulary, in term order
     * @param coverage    the share of the merged column's values these terms hold; one for a union of
     *                    dictionaries that let nothing escape, and otherwise an under-estimate
     */
    public static Terms known(List<BytesRef> sortedTerms, long columnBytes, double coverage, long[] countsPerTerm) {
        final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(Counter.newCounter())));
        final int[] dictionaryIds = new int[sortedTerms.size()];
        final int[] ordinalOfId = new int[sortedTerms.size()];
        final long[] counts = countsPerTerm == null ? null : new long[sortedTerms.size()];
        long dictionaryBytes = 0;
        for (int ordinal = 0; ordinal < sortedTerms.size(); ordinal++) {
            int id = terms.add(sortedTerms.get(ordinal));
            if (id < 0) {
                id = -1 - id;
            }
            dictionaryIds[ordinal] = id;
            ordinalOfId[id] = ordinal;
            dictionaryBytes += TermQuota.cost(sortedTerms.get(ordinal));
            if (counts != null) {
                counts[id] = countsPerTerm[ordinal];
            }
        }
        return new Terms(terms, dictionaryIds, dictionaryIds, ordinalOfId, coverage, dictionaryBytes, columnBytes, counts);
    }

    /**
     * A vocabulary worked out from what the merged segments summarised rather than from their values.
     * {@code counted} is the summed summaries, whose counts are lower bounds because a term a segment held
     * once is in that segment's summary only if it fitted; the coverage this returns is therefore a lower
     * bound too.
     *
     * <p>Both quotas are applied here rather than by the caller, so a merged column selects its dictionary
     * and its summary the same way a flush does. A merge that reused its dictionary selection for its
     * summary would drop the terms held once per input, which is the case this whole change is about, and
     * the loss would compound over generations of merges.
     *
     * @param numValues the values the merged column holds, which {@code counted} is a share of
     */
    public static Terms combined(
        Map<BytesRef, Long> counted,
        long columnBytes,
        long numValues,
        DictionaryPolicy dictionaryPolicy,
        SummaryPolicy summaryPolicy
    ) {
        final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(Counter.newCounter())));
        final long[] occurrences = new long[counted.size()];
        for (Map.Entry<BytesRef, Long> entry : counted.entrySet()) {
            int id = terms.add(entry.getKey());
            if (id < 0) {
                id = -1 - id;
            }
            occurrences[id] = entry.getValue();
        }
        final TermSelection selection = new TermSelection(terms, occurrences);
        final int[] dictionaryIds = selection.thatFit(TermQuota.forDictionary(dictionaryPolicy, columnBytes));
        final int[] summaryIds = selection.thatFit(TermQuota.forMergedSummary(summaryPolicy));
        if (dictionaryIds.length == 0 && summaryIds.length == 0) {
            return null;
        }
        final int[] ordinalOfId = new int[terms.size()];
        Arrays.fill(ordinalOfId, DROPPED);
        long coveredValues = 0;
        long dictionaryBytes = 0;
        final BytesRef scratch = new BytesRef();
        for (int ordinal = 0; ordinal < dictionaryIds.length; ordinal++) {
            final int id = dictionaryIds[ordinal];
            ordinalOfId[id] = ordinal;
            terms.get(id, scratch);
            coveredValues += occurrences[id];
            dictionaryBytes += TermQuota.cost(scratch);
        }
        return new Terms(
            terms,
            dictionaryIds,
            summaryIds,
            ordinalOfId,
            numValues == 0 ? 0.0 : (double) coveredValues / numValues,
            dictionaryBytes,
            columnBytes,
            occurrences
        );
    }

    /**
     * Surveys {@code values}, returning the terms worth a dictionary entry, or null when the column holds
     * nothing worth naming.
     */
    public static Terms survey(StringColumnValues values, DictionaryPolicy dictionaryPolicy, SummaryPolicy summaryPolicy)
        throws IOException {
        final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(Counter.newCounter())));
        int[] counts = new int[64];
        long tableBytes = 0;
        long columnBytes = 0;
        // A column that arrives in term order repeats each value in a run, so the term a value takes is
        // almost always the one before it. Comparing against that costs a length check and settles it
        // without a hash probe; only a run boundary pays for one.
        final BytesRefBuilder previous = new BytesRefBuilder();
        int previousId = ABSENT;
        boolean hasPrevious = false;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            for (int i = 0, count = values.valueCount(); i < count; i++) {
                values.nextValue();
                final BytesRef value = values.value();
                if (value == null) {
                    // A null is named by an ordinal of its own, so it is not a term worth a dictionary entry
                    // and its bytes are not bytes the column would otherwise store. Counting it would credit
                    // the empty term with occurrences it does not have, and could win it an entry — or
                    // displace a real term — on the strength of values that are not empty strings.
                    continue;
                }
                // NOTE: empty strings occupy an ordinal slot and a plain-path entry, so they count
                // as one virtual byte to keep the denominator positive and the metric meaningful.
                columnBytes += TermQuota.cost(value);
                if (hasPrevious && previous.get().bytesEquals(value)) {
                    if (previousId != ABSENT) {
                        counts[previousId]++;
                    }
                    continue;
                }
                int id = terms.find(value);
                if (id < 0) {
                    if (tableBytes + value.length > dictionaryPolicy.maxBytes()) {
                        if (terms.size() > 0) {
                            final long[] freed = { 0 };
                            counts = evictLeastFrequent(terms, counts, freed);
                            tableBytes -= freed[0];
                        }
                        if (tableBytes + value.length > dictionaryPolicy.maxBytes()) {
                            // Nothing could be displaced: either every term held occurs at least as often as
                            // this one, or the table is empty and the value alone is larger than the bound.
                            // Remembered as absent, so the rest of its run is turned away as cheaply.
                            previous.copyBytes(value);
                            previousId = ABSENT;
                            hasPrevious = true;
                            continue;
                        }
                    }
                    id = terms.add(value);
                    if (id < 0) {
                        id = -1 - id;
                    }
                    counts = ArrayUtil.grow(counts, id + 1);
                    tableBytes += value.length;
                }
                counts[id]++;
                // Copied only here, so a run costs one copy rather than one per value.
                previous.copyBytes(value);
                previousId = id;
                hasPrevious = true;
            }
        }
        if (terms.size() == 0) {
            return null;
        }
        final TermSelection selection = new TermSelection(terms, counts);
        final int[] dictionaryIds = selection.thatFit(TermQuota.forDictionary(dictionaryPolicy, columnBytes));
        final int[] summaryIds = selection.thatFit(TermQuota.forSummary(summaryPolicy));
        // NOTE: a column where nothing repeats earns no dictionary entry but still has terms worth leaving
        // for a merge, which may hold them often enough across segments. Returning null here would put the
        // merge back to reading values, which is what the summary exists to avoid.
        if (dictionaryIds.length == 0 && summaryIds.length == 0) {
            return null;
        }
        // Indexed by id, so a term the survey saw but did not keep is told apart from ordinal zero.
        final int[] ordinalOfId = new int[terms.size()];
        Arrays.fill(ordinalOfId, DROPPED);
        long coveredBytes = 0;
        long keptBytes = 0;
        final BytesRef scratch = new BytesRef();
        for (int ordinal = 0; ordinal < dictionaryIds.length; ordinal++) {
            final int id = dictionaryIds[ordinal];
            ordinalOfId[id] = ordinal;
            terms.get(id, scratch);
            coveredBytes += counts[id] * TermQuota.cost(scratch);
            keptBytes += TermQuota.cost(scratch);
        }
        return new Terms(
            terms,
            dictionaryIds,
            summaryIds,
            ordinalOfId,
            (double) coveredBytes / columnBytes,
            keptBytes,
            columnBytes,
            selection.occurrences()
        );
    }

    /**
     * Charges every tracked term the same number of occurrences and drops those that fall to zero,
     * reporting the bytes the dropped terms held. Survivors keep what is left of their counts, so a term
     * seen many times is not displaced by one seen once.
     */
    private static int[] evictLeastFrequent(BytesRefHash terms, int[] counts, long[] freed) {
        final int size = terms.size();
        assert size > 0 : "nothing to evict; an empty table cannot make room";
        // Taking the charge to be the median rather than one frees half the table at a stroke, so a column
        // of mostly distinct values makes room a few times rather than once per value it cannot fit. The
        // bound is unchanged: a round of decrements absorbs as many occurrences as there are terms held, so
        // across the column they can absorb at most one term's worth of n/k, which is the error a count
        // already carries.
        final int decrement = Math.max(1, medianCount(counts, size));

        final BytesRef scratch = new BytesRef();
        final List<BytesRef> survivors = new ArrayList<>();
        final IntArrayList survivorCounts = new IntArrayList();
        for (int id = 0; id < size; id++) {
            terms.get(id, scratch);
            if (counts[id] > decrement) {
                survivors.add(BytesRef.deepCopyOf(scratch));
                survivorCounts.add(counts[id] - decrement);
            } else {
                freed[0] += scratch.length;
            }
        }
        if (survivors.size() == size) {
            // Nothing fell to zero, so the counts are simply reduced where they are.
            for (int id = 0; id < size; id++) {
                counts[id] -= decrement;
            }
            return counts;
        }
        terms.clear();
        terms.reinit();
        // The ids are handed out afresh, so a count left over from the old numbering would be read as a new
        // term's.
        final int[] rebuilt = new int[Math.max(counts.length, survivors.size() + 1)];
        for (int i = 0; i < survivors.size(); i++) {
            int id = terms.add(survivors.get(i));
            if (id < 0) {
                id = -1 - id;
            }
            rebuilt[id] = survivorCounts.get(i);
        }
        return rebuilt;
    }

    /** The median of the first {@code size} counts, by selection: only the middle one is needed. */
    private static int medianCount(int[] counts, int size) {
        final int[] scratch = ArrayUtil.copyOfSubArray(counts, 0, size);
        final int middle = size / 2;
        new IntroSelector() {
            private int pivot;

            @Override
            protected void swap(int i, int j) {
                final int tmp = scratch[i];
                scratch[i] = scratch[j];
                scratch[j] = tmp;
            }

            @Override
            protected void setPivot(int i) {
                pivot = scratch[i];
            }

            @Override
            protected int comparePivot(int j) {
                return Integer.compare(pivot, scratch[j]);
            }
        }.select(0, size, middle);
        return scratch[middle];
    }

    /**
     * Orders {@code ids} by {@code first}, and by their terms where it does not separate them. Comparing by
     * term last leaves the order total, so the same column always yields the same dictionary.
     */
}
