/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import java.math.BigDecimal;

/**
 * When a string column is worth storing as a dictionary, and how large that dictionary may get.
 *
 * <p>The size is bounded in bytes rather than in terms, because a term count says nothing about what a
 * dictionary costs: eight thousand log levels and eight thousand URLs are three orders of magnitude apart.
 * Long terms therefore yield a smaller dictionary, which is the intended behaviour. What the survey may hold
 * while choosing is a budget of its own, the larger of this cap and the summary's, at
 * {@link SummaryPolicy#surveyBudgetBytes}. The width of an ordinal does not enter into it: a block is packed
 * to the widest ordinal it actually contains.
 *
 * <p>Whether to keep it is then two questions. {@link #minCoverage} asks what share of the column's values
 * a read can answer through an ordinal. The rest escape, and escaped values share one ordinal, so a filter
 * for a term the dictionary names rules them all out by it while a filter for one that escaped has to read
 * and compare their bytes. Read the other way the bar is a budget for the rest: a dictionary admitted at a
 * coverage of {@code b} leaves at most {@code 1 - b} of the column's values in that stream.
 * {@link #maxShareOfColumn} asks whether the dictionary is small against the data it describes, since a
 * dictionary as large as the values it stands in for has bought nothing, however well it covers them.
 *
 * @param maxBytes         the most term bytes a dictionary may hold
 * @param minCoverage      the share of a column's values the dictionary must name, so that at most
 *                         one minus this share of them escape
 * @param maxShareOfColumn the largest share of the column's value bytes the dictionary may occupy
 */
public record DictionaryPolicy(int maxBytes, double minCoverage, double maxShareOfColumn) {

    /** Never builds a dictionary. */
    public static final DictionaryPolicy NONE = new DictionaryPolicy(0, 1.0, 0.0);

    public DictionaryPolicy {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("dictionary bounds must not be negative");
        }
        // NOTE: a comparison against NaN is false whichever way it is written, so a range test alone admits
        // it and {@link #rulesOut} then fails on a value no exact arithmetic can represent.
        if (Double.isNaN(minCoverage) || minCoverage < 0.0 || minCoverage > 1.0) {
            throw new IllegalArgumentException("minCoverage must be a share, got " + minCoverage);
        }
        if (Double.isNaN(maxShareOfColumn) || maxShareOfColumn < 0.0) {
            throw new IllegalArgumentException("maxShareOfColumn must not be negative, got " + maxShareOfColumn);
        }
    }

    public boolean enabled() {
        return maxBytes > 0;
    }

    /**
     * The term bytes a dictionary for a column of {@code columnBytes} may hold. A dictionary is bounded
     * both absolutely and relative to what it describes, and the tighter of the two governs: filling the
     * absolute bound with rare terms would leave a dictionary as large as the column it stands in for.
     */
    public long budgetFor(long columnBytes) {
        return Math.min(maxBytes, (long) (maxShareOfColumn * columnBytes));
    }

    /**
     * Whether a vocabulary is worth keeping as this column's dictionary. Two questions, and it must pass
     * both: {@link #minCoverage}, the share of values an ordinal can answer, and {@link #maxShareOfColumn},
     * the bytes it may spend saying so.
     *
     * <p>Selection has already held the terms to {@link #maxBytes}, so what is asked here is coverage and
     * relative size, not the absolute cap. The coverage is a ratio of counts that themselves under-state,
     * rounded to a double, so it can sit a hair either side of the truth. That is safe in this direction:
     * admitting a dictionary marginally below the bar costs an ordinal, which is why {@link #rulesOut},
     * where the mistake cannot be undone, compares exactly instead.
     *
     * @param coverage        the share of values the vocabulary names, from counts that under-state
     * @param dictionaryBytes the term bytes it would occupy
     * @param columnBytes     the value bytes of the column it would describe
     * @return whether the dictionary is worth building
     */
    public boolean worthKeeping(double coverage, long dictionaryBytes, long columnBytes) {
        return coverage >= minCoverage && dictionaryBytes <= maxShareOfColumn * columnBytes;
    }

    /**
     * Whether {@code bound} proves no dictionary here is worth keeping. The same bar {@link #worthKeeping}
     * applies, read in the other direction, and the only one of the two that may refuse a dictionary.
     *
     * <p>False wherever it cannot be sure. A bound taken under a smaller cap than this policy allows does
     * not describe the dictionary being decided on, and an unknown bound describes nothing at all; neither
     * is a bound of zero, and reading either as one would refuse every dictionary.
     *
     * @param bound an upper bound on what a dictionary could name on the column
     * @return whether the bound rules a dictionary out
     */
    public boolean rulesOut(BestCoverage bound) {
        return bound.validFor(maxBytes) && couldReach(bound) == false;
    }

    /**
     * Whether {@code bound} leaves {@link #minCoverage} reachable. Compared exactly where
     * {@link #worthKeeping} may round, because admitting a dictionary a hair below the bar costs an
     * ordinal, while refusing one the values could have built cannot be undone.
     */
    private boolean couldReach(BestCoverage bound) {
        if (bound.numValues() == 0) {
            return false;
        }
        // NOTE: a count past what a double holds rounds on the way in, so the bar cannot be worked out in
        // doubles and then repaired: flooring it lets a refusal through that the values clear, and rounding
        // it up refuses coverage that is genuinely reachable.
        return BigDecimal.valueOf(bound.namedValues())
            .compareTo(new BigDecimal(minCoverage).multiply(BigDecimal.valueOf(bound.numValues()))) >= 0;
    }
}
