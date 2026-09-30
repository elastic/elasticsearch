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

    /** The most valuable ids whose bytes {@code quota} pays for, in term order. */
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
     * Cross multiplication orders by count per byte without dividing. The products cannot overflow: a count
     * is bounded by the values one merged column holds and a length by a {@link BytesRef}, so the largest
     * either can reach leaves the product far inside a long.
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
        ordering.sort(ids, 0, size, (a, b) -> Long.compare(counts[b] * lengths[a], counts[a] * lengths[b]));
        return ids;
    }

}
