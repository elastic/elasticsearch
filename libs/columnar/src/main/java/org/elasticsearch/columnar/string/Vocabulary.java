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
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntroSorter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntBinaryOperator;

/**
 * The terms a column repeats often enough to be worth naming, found in one pass over its values with the
 * memory the caller's {@link DictionaryPolicy} allows.
 *
 * <p>The pass is Misra-Gries: terms are counted in a table bounded by the policy's byte budget, and when
 * the table is full every term is charged the same number of occurrences and those that reach zero leave.
 * A term is therefore only displaced by terms that between them occur more often than it does, so the
 * values most of the column holds survive however late they are first seen — admitting whatever arrived
 * first would keep the leading values rather than the common ones. What this costs is exactness: a count
 * is a lower bound, under-stating by at most the total charged away.
 */
public final class Vocabulary {

    /** An id the survey saw but left out; its values escape like any unknown term. */
    public static final int DROPPED = -1;

    private Vocabulary() {}

    /**
     * The terms a dictionary will hold, in term order, and what share of the column they account for.
     *
     * @param terms          the surveyed terms, addressed by id
     * @param sortedIds      the kept ids in term order, so an ordinal comparison is a term comparison
     * @param ordinalOfId    an ordinal per surveyed id, or {@link #DROPPED} for one that was not kept
     * @param coverage       the share of the column's values these terms account for, as a lower bound
     * @param dictionaryBytes the term bytes the kept terms occupy
     * @param columnBytes    the value bytes the whole column occupies
     * @param counts         how often each id was seen, as a lower bound, or null when unknown
     */
    public record Terms(
        BytesRefHash terms,
        int[] sortedIds,
        int[] ordinalOfId,
        double coverage,
        long dictionaryBytes,
        long columnBytes,
        int[] counts
    ) {
        /** Whether this vocabulary knows how often it saw each of its terms. */
        public boolean counted() {
            return counts != null;
        }

        /** How often the term at {@code ordinal} was seen, as a lower bound. */
        public int countOf(int ordinal) {
            return counts[sortedIds[ordinal]];
        }

        public int size() {
            return sortedIds.length;
        }
    }

    /**
     * A vocabulary worked out from what other columns recorded rather than from values: the union of their
     * dictionaries, or the sum of their summaries. Either way their values need not be read again to
     * discover what they contain.
     *
     * @param sortedTerms the vocabulary, in term order
     * @param coverage    the share of the merged column's values these terms hold. One for a union of
     *                    dictionaries that let nothing escape, and otherwise an under-estimate, since the
     *                    counts a summary carries are themselves lower bounds.
     */
    public static Terms known(List<BytesRef> sortedTerms, long columnBytes, double coverage, long[] countsPerTerm) {
        final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(Counter.newCounter())));
        final int[] sortedIds = new int[sortedTerms.size()];
        final int[] ordinalOfId = new int[sortedTerms.size()];
        final int[] counts = countsPerTerm == null ? null : new int[sortedTerms.size()];
        long dictionaryBytes = 0;
        for (int ordinal = 0; ordinal < sortedTerms.size(); ordinal++) {
            int id = terms.add(sortedTerms.get(ordinal));
            if (id < 0) {
                id = -1 - id;
            }
            sortedIds[ordinal] = id;
            ordinalOfId[id] = ordinal;
            dictionaryBytes += sortedTerms.get(ordinal).length;
            if (counts != null) {
                counts[id] = (int) Math.min(Integer.MAX_VALUE, countsPerTerm[ordinal]);
            }
        }
        return new Terms(terms, sortedIds, ordinalOfId, coverage, dictionaryBytes, columnBytes, counts);
    }

    /**
     * Surveys {@code values}, returning the terms worth a dictionary entry, or null when the column holds
     * nothing worth naming.
     */
    public static Terms survey(StringColumnValues values, DictionaryPolicy policy, long numValues) throws IOException {
        final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(Counter.newCounter())));
        int[] counts = new int[64];
        long tableBytes = 0;
        long columnBytes = 0;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            for (int i = 0, count = values.valueCount(); i < count; i++) {
                final BytesRef value = values.nextValue();
                columnBytes += value.length;
                int id = terms.find(value);
                if (id < 0) {
                    if (tableBytes + value.length > policy.maxBytes()) {
                        if (terms.size() > 0) {
                            final long[] freed = { 0 };
                            counts = evictLeastFrequent(terms, counts, freed);
                            tableBytes -= freed[0];
                        }
                        if (tableBytes + value.length > policy.maxBytes()) {
                            // Nothing could be displaced: either every term held occurs at least as often as
                            // this one, or the table is empty and the value alone is larger than the bound.
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
            }
        }
        if (terms.size() == 0) {
            return null;
        }
        // The pass admits every term that fits, which on a column with a long tail means the budget goes to
        // terms seen once. Keeping only the most frequent that fit the column's budget leaves a dictionary
        // that costs a fraction of what it describes, and the terms dropped here escape.
        final int[] sortedIds = keepMostFrequent(terms, counts, policy.budgetFor(columnBytes));
        if (sortedIds.length == 0) {
            return null;
        }
        // Indexed by id, so a term the survey saw but did not keep is told apart from ordinal zero.
        final int[] ordinalOfId = new int[terms.size()];
        Arrays.fill(ordinalOfId, DROPPED);
        long covered = 0;
        long keptBytes = 0;
        final BytesRef scratch = new BytesRef();
        for (int ordinal = 0; ordinal < sortedIds.length; ordinal++) {
            final int id = sortedIds[ordinal];
            ordinalOfId[id] = ordinal;
            covered += counts[id];
            terms.get(id, scratch);
            keptBytes += scratch.length;
        }
        return new Terms(terms, sortedIds, ordinalOfId, (double) covered / numValues, keptBytes, columnBytes, counts);
    }

    /**
     * The ids worth a dictionary entry, in term order: the most frequent terms whose bytes fit
     * {@code budget}. Terms seen equally often are ordered by term, so the same column always yields the
     * same dictionary.
     */
    private static int[] keepMostFrequent(BytesRefHash terms, int[] counts, long budget) {
        final int size = terms.size();
        final int[] ids = new int[size];
        for (int id = 0; id < size; id++) {
            ids[id] = id;
        }
        sort(ids, 0, size, terms, (a, b) -> Integer.compare(counts[b], counts[a]));
        int keptCount = 0;
        long bytes = 0;
        final BytesRef scratch = new BytesRef();
        for (int i = 0; i < size; i++) {
            // A term seen once is worth one value's coverage and costs its own bytes plus, once there are
            // enough of them, a wider ordinal on every value in the column. It is cheaper to let it escape.
            // The counts are lower bounds, so a term dropped here was seen at most twice.
            if (counts[ids[i]] <= 1) {
                break;
            }
            terms.get(ids[i], scratch);
            if (bytes + scratch.length > budget) {
                break;
            }
            bytes += scratch.length;
            keptCount++;
        }
        final int[] kept = ArrayUtil.copyOfSubArray(ids, 0, keptCount);
        // Back into term order, which is the order the dictionary is written and searched in.
        sort(kept, 0, keptCount, terms, null);
        return kept;
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
        final int[] sorted = ArrayUtil.copyOfSubArray(counts, 0, size);
        Arrays.sort(sorted);
        final int decrement = Math.max(1, sorted[size / 2]);

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

    /**
     * Orders {@code ids} by {@code first}, and by their terms where it does not separate them. Comparing by
     * term last leaves the order total, so the same column always yields the same dictionary.
     */
    private static void sort(int[] ids, int from, int to, BytesRefHash terms, IntBinaryOperator first) {
        new IntroSorter() {
            private final BytesRef left = new BytesRef();
            private final BytesRef right = new BytesRef();
            private int pivotId;

            @Override
            protected void swap(int i, int j) {
                final int tmp = ids[i];
                ids[i] = ids[j];
                ids[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                return compareIds(ids[i], ids[j]);
            }

            @Override
            protected void setPivot(int i) {
                pivotId = ids[i];
            }

            @Override
            protected int comparePivot(int j) {
                return compareIds(pivotId, ids[j]);
            }

            private int compareIds(int a, int b) {
                if (first != null) {
                    final int cmp = first.applyAsInt(a, b);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                terms.get(a, left);
                terms.get(b, right);
                return left.compareTo(right);
            }
        }.sort(from, to);
    }
}
