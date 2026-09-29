/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.IntroSorter;

import java.util.function.IntBinaryOperator;

/**
 * Ranks a surveyed column's terms by what they are worth, and answers each {@link TermQuota} asked of it
 * with the terms that quota pays for.
 *
 * <p>The ranking is done once, in the constructor, because every answer is a prefix of it and ranking is
 * the expensive part. What each consumer asks for is stated by its quota rather than here.
 *
 * <p>Answers come back in term order, which is the order a dictionary is written and searched in, and ties
 * are broken by term, so the same column always yields the same answer however its values arrived.
 */
final class TermSelection {

    private final BytesRefHash terms;
    private final long[] counts;
    private final int[] byFrequency;

    TermSelection(BytesRefHash terms, int[] counts) {
        this(terms, widen(counts, terms.size()));
    }

    /**
     * Counts summed across merged columns pass what an int holds long before a column does, and a ranking
     * that clamped them would order the two largest terms by term rather than by how often they are held.
     */
    TermSelection(BytesRefHash terms, long[] counts) {
        this.terms = terms;
        this.counts = counts;
        this.byFrequency = idsByFrequency(terms, counts);
    }

    /**
     * How often the survey saw each id. A merge sums these across its inputs and can pass what an int holds,
     * and the summary they are written to carries them as vlongs, so they are counted as longs throughout.
     */
    long[] occurrences() {
        return counts;
    }

    /** The most valuable ids whose bytes {@code quota} pays for, in term order. */
    int[] thatFit(TermQuota quota) {
        int keptCount = 0;
        long bytes = 0;
        final BytesRef scratch = new BytesRef();
        for (int id : byFrequency) {
            if (counts[id] < quota.minCount()) {
                break;
            }
            terms.get(id, scratch);
            if (bytes + TermQuota.cost(scratch) > quota.budget()) {
                break;
            }
            bytes += TermQuota.cost(scratch);
            keptCount++;
        }
        final int[] kept = ArrayUtil.copyOfSubArray(byFrequency, 0, keptCount);
        sort(kept, 0, keptCount, terms, null);
        return kept;
    }

    private static long[] widen(int[] counts, int size) {
        final long[] widened = new long[size];
        for (int id = 0; id < size; id++) {
            widened[id] = counts[id];
        }
        return widened;
    }

    /** Every id the survey saw, most frequent first, so all the answers share one ranking. */
    private static int[] idsByFrequency(BytesRefHash terms, long[] counts) {
        final int size = terms.size();
        final int[] ids = new int[size];
        for (int id = 0; id < size; id++) {
            ids[id] = id;
        }
        sort(ids, 0, size, terms, (a, b) -> Long.compare(counts[b], counts[a]));
        return ids;
    }

    /**
     * Sorts {@code ids} by {@code first}, and by term where that does not decide, so an ordering is total
     * and a column does not depend on the order its values happened to arrive in.
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
