/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

/**
 * An upper bound on the values a dictionary could name on a column, and the cap it was taken against. A
 * bound taken under a smaller cap says nothing about a larger one. {@link #UNKNOWN} is not a bound of zero.
 *
 * @param namedValues the most values a dictionary within {@code cap} could name here, never below the truth
 * @param numValues   the non-null values {@code namedValues} is a share of
 * @param cap         the dictionary byte cap the bound was taken against, or zero when unknown
 */
public record BestCoverage(long namedValues, long numValues, long cap) {

    public static final BestCoverage UNKNOWN = new BestCoverage(0, 0, 0);

    /**
     * A bound, or {@link #UNKNOWN} where {@code cap} says there was none to take.
     *
     * @param namedValues the most values a dictionary within {@code cap} could name
     * @param numValues   the non-null values that is a share of
     * @param cap         the dictionary byte cap it was taken against, or zero for no bound at all
     * @return the bound, or {@link #UNKNOWN}
     */
    public static BestCoverage of(long namedValues, long numValues, long cap) {
        return cap <= 0 ? UNKNOWN : new BestCoverage(namedValues, numValues, cap);
    }

    /** Whether a bound was taken at all. An unknown bound refuses nothing; a bound of zero refuses all. */
    public boolean known() {
        return cap > 0;
    }

    /**
     * Whether this still bounds a dictionary allowed {@code maxBytes}. A larger cap buys terms this bound
     * never considered, so only a cap no larger than its own keeps it true.
     *
     * @param maxBytes the cap the dictionary being decided on is held to
     * @return whether the bound may be used to refuse that dictionary
     */
    public boolean validFor(long maxBytes) {
        return known() && maxBytes <= cap;
    }

    /** The bound as a share of the column, for reporting. {@link DictionaryPolicy#rulesOut} decides on it. */
    public double share() {
        return Vocabulary.share(namedValues, numValues);
    }

    /**
     * The tighter of two bounds on the same column. Both hold, so the smaller share is the useful one.
     *
     * @param other a bound on the same column, possibly unknown
     * @return the smaller of the two by share, or whichever of them is known
     */
    public BestCoverage tighter(BestCoverage other) {
        if (known() == false) {
            return other;
        }
        if (other.known() == false) {
            return this;
        }
        return other.share() < share() ? other : this;
    }

    /**
     * The bound on a column holding both. A merged dictionary names no more of either input than that
     * input's own bound, so the numerators sum, and the sum holds only under the weaker of the two caps.
     *
     * @param other the other input's bound
     * @return the summed bound, or {@link #UNKNOWN} if either side is unknown
     */
    public BestCoverage plus(BestCoverage other) {
        if (known() == false || other.known() == false) {
            return UNKNOWN;
        }
        return new BestCoverage(namedValues + other.namedValues, numValues + other.numValues, Math.min(cap, other.cap));
    }
}
