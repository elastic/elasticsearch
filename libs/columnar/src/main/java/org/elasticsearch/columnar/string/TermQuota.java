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
 * What one consumer of a survey asks of it: how many term bytes it will pay for, and how rare a term it
 * will still admit.
 *
 * <p>The two consumers ask differently, and this is where the difference is stated rather than spread
 * through the code that answers it.
 *
 * @param budget   the most term bytes the answer may hold
 * @param minCount how often a term must have been seen to be admitted at all
 */
record TermQuota(long budget, int minCount) {

    /**
     * What a dictionary asks. It admits no term held once, which would cost its own bytes to buy a single
     * ordinal, and it is bounded by a share of the column as well as by the policy's cap, since a
     * dictionary as large as the values it stands in for has bought nothing.
     */
    static TermQuota forDictionary(DictionaryPolicy dictionaryPolicy, long columnBytes) {
        return new TermQuota(dictionaryPolicy.budgetFor(columnBytes), 2);
    }

    /**
     * What a summary asks. It admits every term the survey saw, since one held once in this column may be
     * held once per segment and many times by the merged column, and only the cap bounds it: a share of
     * this column says nothing about the merge that reads it.
     */
    static TermQuota forSummary(SummaryPolicy summaryPolicy) {
        return new TermQuota(summaryPolicy.maxBytes(), 1);
    }

    /**
     * What a merged column's summary asks. A flush admits a term held once because it has no way of knowing
     * whether the other segments hold it; a merge has already put every term to that question, once per
     * input, and a term still held once has failed it. Admitting those would summarise a column of values
     * unique to the index at the size of the column itself, at every generation.
     */
    static TermQuota forMergedSummary(SummaryPolicy summaryPolicy) {
        return new TermQuota(summaryPolicy.maxBytes(), 2);
    }
}
