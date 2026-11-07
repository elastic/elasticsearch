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
 * How large a vocabulary a column may leave behind for the merge that reads it next.
 *
 * <p>This is not {@link DictionaryPolicy}, though both bound term bytes, because the two answer to
 * different readers. A dictionary serves this column, so it is also held to a share of it: one as large as
 * the values it stands in for has bought nothing. A vocabulary serves the merge, which reads it against a
 * column many times larger, so a share of this column says nothing useful about it and only the absolute
 * bound applies.
 *
 * @param maxBytes               the most term bytes a column may leave behind, or zero to leave none
 * @param mergeBudgetMultiplier how much more than {@link #surveyBudgetBytes} a merge may hold, at least one
 */
public record SummaryPolicy(int maxBytes, int mergeBudgetMultiplier) {

    /**
     * How much more than one column's vocabulary a merge may hold. A term can be worth keeping to the
     * merged column at a count no input reached on its own, so a merge bounded as tightly as a single
     * column would forget those before it had read enough to recognise them. Four is a judgement rather
     * than a derived value: it is the memory a merge may spend to keep counting.
     */
    private static final int DEFAULT_MERGE_BUDGET_MULTIPLIER = 4;

    /** Leaves nothing behind, so a merge reads the values instead. */
    public static final SummaryPolicy NONE = sized(0);

    public SummaryPolicy {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must not be negative, got " + maxBytes);
        }
        if (mergeBudgetMultiplier < 1) {
            throw new IllegalArgumentException("mergeBudgetMultiplier must be at least one, got " + mergeBudgetMultiplier);
        }
    }

    /**
     * A policy bounding term bytes at {@code maxBytes}, letting a merge hold the shipped multiple of that.
     *
     * @param maxBytes the most term bytes a column may leave behind, or zero to leave none
     * @return the policy
     */
    public static SummaryPolicy sized(int maxBytes) {
        return new SummaryPolicy(maxBytes, DEFAULT_MERGE_BUDGET_MULTIPLIER);
    }

    public boolean enabled() {
        return maxBytes > 0;
    }

    /**
     * The most term bytes a column may hold in memory while it works out its vocabulary. A column remembers
     * at least what a dictionary could name on it, since a term it forgets is one the dictionary can no
     * longer be offered, and at least what it is allowed to leave behind, since a term it forgets is one the
     * next merge will not be told about.
     *
     * @param dictionaryPolicy the policy the column's dictionary is held to
     * @return the bound on the terms held while surveying, in bytes
     */
    public long surveyBudgetBytes(DictionaryPolicy dictionaryPolicy) {
        return Math.max(dictionaryPolicy.maxBytes(), maxBytes);
    }

    /**
     * The same bound for a merge, which combines its inputs' summaries rather than surveying values. Larger
     * than {@link #surveyBudgetBytes} by a fixed multiple, because a term is worth keeping to the merged
     * column at a count no input reached on its own, and the merge cannot know which those are until every
     * input has been read.
     *
     * <p>A multiple of one column's bound rather than a share of the inputs, so the bytes a merge retains do
     * not grow with the number of segments merged. It still reads every input: what is held flat is the
     * memory, not the work. Terms trimmed at the bound are ones the merge can no longer count, and
     * occurrences it cannot count are credited to a weightless term, so raising
     * {@link #mergeBudgetMultiplier} buys a tighter {@link BestCoverage} at the memory it holds.
     *
     * @param dictionaryPolicy the policy the merged column's dictionary would be held to
     * @return the bound on the combined terms held while merging, in bytes
     */
    public long mergeBudgetBytes(DictionaryPolicy dictionaryPolicy) {
        return mergeBudgetMultiplier * surveyBudgetBytes(dictionaryPolicy);
    }
}
