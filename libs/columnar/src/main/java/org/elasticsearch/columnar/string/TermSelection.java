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

/**
 * Ranks a surveyed column's terms by what they are worth, and answers each {@link TermQuota} asked of it
 * with the terms that quota pays for.
 *
 * <p>The ranking is done once, in the constructor, because every answer walks it in order and ranking is
 * the expensive part. Terms rank by what they name per byte they cost, since the budget is spent on bytes
 * and buys named values. What each consumer asks for is stated by its quota rather than here.
 *
 * <p>Answers come back in term order, which is the order a dictionary is written and searched in, and ties
 * are broken by term, so the same column always yields the same answer however its values arrived.
 */
final class TermSelection {

    private final BytesRefHash terms;
    private final long[] counts;
    private final TermOrdering ordering;
    private final int[] byDensity;

    /**
     * Counts summed across merged columns pass what an int holds long before a column does, and a ranking
     * that clamped them would order the two largest terms by term rather than by how often they are held.
     */
    TermSelection(BytesRefHash terms, long[] counts) {
        this.terms = terms;
        this.counts = counts;
        this.ordering = new TermOrdering(terms);
        this.byDensity = idsByDensity(ordering, terms, counts);
    }

    /**
     * How often the survey saw each id. A merge sums these across its inputs and can pass what an int holds,
     * and the summary they are written to carries them as vlongs, so they are counted as longs throughout.
     */
    long[] occurrences() {
        return counts;
    }

    BytesRefHash terms() {
        return terms;
    }

    /** What {@code ids} cost a budget, charged the same way one term is by {@link TermQuota#cost}. */
    long bytesOf(int[] ids) {
        long bytes = 0;
        final BytesRef scratch = new BytesRef();
        for (int id : ids) {
            terms.get(id, scratch);
            bytes += TermQuota.cost(scratch);
        }
        return bytes;
    }

    /**
     * An upper bound on the values a dictionary of no more than {@code budget} term bytes could name. The
     * relaxed knapsack optimum, plus every occurrence {@link #occurrences} does not account for, since
     * those counts are lower bounds. No minimum count applies: relaxing it only widens the sets covered.
     *
     * @param budget    the term bytes a dictionary may hold
     * @param numValues the non-null values in the column, which the answer is capped at
     * @return the most values any dictionary within {@code budget} could name, never below the truth
     */
    long bestCoverage(long budget, long numValues) {
        long counted = 0;
        for (int id = 0; id < terms.size(); id++) {
            counted = clampSum(counted, counts[id]);
        }
        final long unaccounted = Math.max(0, numValues - counted);
        final long best = clampSum(bestWithPartialTerms(budget), unaccounted);
        return Math.min(numValues, best);
    }

    /**
     * The most occurrences a budget could hold when a term may be taken in part, which is what makes this
     * an over-estimate of any real dictionary. Counted in longs: a count can pass what a double holds
     * exactly, and rounding one down would put the answer below the truth.
     */
    private long bestWithPartialTerms(long budget) {
        long spent = 0;
        long held = 0;
        final BytesRef scratch = new BytesRef();
        for (int id : byDensity) {
            terms.get(id, scratch);
            final long cost = TermQuota.cost(scratch);
            // NOTE: a term the whole budget cannot buy is in no dictionary this bounds, so it is not part
            // of the problem being relaxed. Crediting a fraction of it would bound the column by a term it
            // can never name.
            if (cost > budget) {
                continue;
            }
            if (spent + cost <= budget) {
                spent += cost;
                held = clampSum(held, counts[id]);
                continue;
            }
            final long left = budget - spent;
            if (left > 0) {
                held = clampSum(held, fractionOfCount(counts[id], left, cost));
            }
            break;
        }
        return held;
    }

    /** The {@code part / whole} share of {@code count}, rounded up and split so neither product overflows. */
    private static long fractionOfCount(long count, long part, long whole) {
        final long quotient = count / whole;
        final long remainder = count % whole;
        return clampSum(quotient * part, Math.ceilDiv(remainder * part, whole));
    }

    /** A sum that stops at {@link Long#MAX_VALUE} rather than wrapping, which still over-states. */
    private static long clampSum(long left, long right) {
        final long sum = left + right;
        return ((left ^ sum) & (right ^ sum)) < 0 ? Long.MAX_VALUE : sum;
    }

    /**
     * The ids {@code quota} admits, in term order, taken greedily by density and stepping over one the
     * budget cannot afford rather than stopping there.
     *
     * <p>Greedy by density is a heuristic over whole terms and not their optimum, so what this achieves is
     * read as evidence that a dictionary exists and never as proof that none does. A larger budget can also
     * trade several low density terms for one high density term it could not previously afford, so
     * selections under different budgets need not nest and a larger budget does not promise more terms.
     * What it does promise is coverage: the terms it admits never name fewer values.
     */
    int[] thatFit(TermQuota quota) {
        final int[] admitted = new int[byDensity.length];
        int keptCount = 0;
        long bytes = 0;
        final BytesRef scratch = new BytesRef();
        for (int id : byDensity) {
            // NOTE: density ranks a short term held once ahead of a long one held often, so neither the
            // count a quota asks for nor the bytes it has left fall away along the ranking. A term either
            // refuses is stepped over; ending the walk there would refuse every term behind it. The walk
            // is therefore always a full one, and the budget is spent exactly rather than nearly.
            if (counts[id] < quota.minCount()) {
                continue;
            }
            terms.get(id, scratch);
            if (bytes + TermQuota.cost(scratch) > quota.budget()) {
                continue;
            }
            bytes += TermQuota.cost(scratch);
            admitted[keptCount++] = id;
        }
        final int[] kept = ArrayUtil.copyOfSubArray(admitted, 0, keptCount);
        ordering.byTerm(kept, 0, keptCount);
        return kept;
    }

    /**
     * The ids two quotas admit together: {@code repeated} first, so they are exactly what it admits alone,
     * then the terms below its minimum count into whatever room is left, bounded also by {@code tail}.
     */
    int[] thatFit(TermQuota repeated, TermQuota tail) {
        final int[] thatRepeat = thatFit(repeated);
        final long room = Math.min(tail.budget(), repeated.budget() - bytesOf(thatRepeat));
        final int[] admitted = new int[byDensity.length];
        System.arraycopy(thatRepeat, 0, admitted, 0, thatRepeat.length);
        int keptCount = thatRepeat.length;
        long bytes = 0;
        final BytesRef scratch = new BytesRef();
        for (int id : byDensity) {
            if (counts[id] >= repeated.minCount() || counts[id] < tail.minCount()) {
                continue;
            }
            terms.get(id, scratch);
            final long cost = TermQuota.cost(scratch);
            if (bytes + cost <= room) {
                bytes += cost;
                admitted[keptCount++] = id;
            }
        }
        final int[] kept = ArrayUtil.copyOfSubArray(admitted, 0, keptCount);
        ordering.byTerm(kept, 0, keptCount);
        return kept;
    }

    /**
     * Cross multiplication orders by count per byte without dividing, across 128 bits since the products
     * can pass what a long holds. An order that wrapped would stop the walk in {@link #bestCoverage} being
     * the optimum of its relaxation, which is the one place a greedy walk here is exact.
     */
    private static int[] idsByDensity(TermOrdering ordering, BytesRefHash terms, long[] counts) {
        final int size = terms.size();
        final int[] ids = new int[size];
        final int[] lengths = new int[size];
        final BytesRef term = new BytesRef();
        for (int id = 0; id < size; id++) {
            ids[id] = id;
            terms.get(id, term);
            lengths[id] = (int) TermQuota.cost(term);
        }
        ordering.sort(ids, 0, size, (a, b) -> compareProducts(counts[b], lengths[a], counts[a], lengths[b]));
        return ids;
    }

    private static int compareProducts(long left, long leftBy, long right, long rightBy) {
        // NOTE: compared over both halves of the 128 bit product. A merged count times a term length
        // passes what a long holds, and a wrapped comparison would rank by something other than density,
        // which is the one thing the greedy walk in bestWithPartialTerms needs to be the optimum.
        final long highLeft = Math.multiplyHigh(left, leftBy);
        final long highRight = Math.multiplyHigh(right, rightBy);
        return highLeft != highRight ? Long.compare(highLeft, highRight) : Long.compareUnsigned(left * leftBy, right * rightBy);
    }
}
