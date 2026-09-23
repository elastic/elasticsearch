/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * What a merge can settle about a column from what its inputs summarised, without reading a value.
 *
 * <p>Summed counts under-state, so reaching the coverage bar with them proves a dictionary worth keeping. A
 * bound taken under a cap no smaller than this merge's over-states, so falling short of the bar proves that
 * no dictionary can reach it. Anything else is left to the caller, which surveys the merged values.
 *
 * <p>Summaries are inheritedBound as they arrive and trimmed to {@link SummaryPolicy#mergeBudgetBytes}, so the
 * terms held do not grow with the number of inputs.
 */
public final class SummaryMerger {

    public enum Outcome {
        DICTIONARY,
        NO_DICTIONARY,
        UNDECIDED
    }

    public record Decision(Outcome outcome, Vocabulary.Terms vocabulary, BestCoverage bestCoverage) {}

    private final DictionaryPolicy dictionaryPolicy;
    private final SummaryPolicy summaryPolicy;
    private final Map<BytesRef, Long> retainedCounts = new HashMap<>();
    private final long maxRetainedTermBytes;

    private long retainedTermBytes;
    private long numValues;
    private long columnBytes;
    private BestCoverage summedBestCoverage;
    private boolean everyInputSummarised = true;

    /**
     * @param dictionaryPolicy the policy the merged column's dictionary would be held to
     * @param summaryPolicy    the policy bounding what the merged column records for the next merge
     */
    public SummaryMerger(DictionaryPolicy dictionaryPolicy, SummaryPolicy summaryPolicy) {
        this.dictionaryPolicy = dictionaryPolicy;
        this.summaryPolicy = summaryPolicy;
        this.maxRetainedTermBytes = summaryPolicy.mergeBudgetBytes(dictionaryPolicy);
    }

    /**
     * Adds one input's summary. Counts are inheritedBound as they arrive and trimmed once the terms held outgrow
     * {@link SummaryPolicy#mergeBudgetBytes}. {@code terms} and {@code counts} are read here and not held,
     * so a caller may reuse them.
     *
     * @param inputValues the non-null values that input holds
     * @param inputBytes  the value bytes it occupies
     * @param bestCoverage what it recorded about the most a dictionary could name on it
     * @param terms       the terms it summarised, in any order
     * @param counts      how often it saw each of them, positionally matching {@code terms}
     */
    public void add(long inputValues, long inputBytes, BestCoverage bestCoverage, List<BytesRef> terms, List<Long> counts) {
        numValues += inputValues;
        columnBytes += inputBytes;
        summedBestCoverage = summedBestCoverage == null ? bestCoverage : summedBestCoverage.plus(bestCoverage);
        for (int t = 0; t < terms.size(); t++) {
            final BytesRef term = terms.get(t);
            if (retainedCounts.merge(BytesRef.deepCopyOf(term), counts.get(t), Long::sum).equals(counts.get(t))) {
                retainedTermBytes += TermQuota.cost(term);
            }
        }
        if (retainedTermBytes > maxRetainedTermBytes) {
            retainedTermBytes = trimToBound();
        }
    }

    /**
     * Whether another summary could still change the answer, which a caller uses to stop reading them.
     *
     * @return false once the answer is settled, so no further summary need be opened
     */
    public boolean needsSummaries() {
        return dictionaryPolicy.enabled() && everyInputSummarised;
    }

    /** Notes an input that summarised nothing. */
    public void addWithoutSummary() {
        everyInputSummarised = false;
        summedBestCoverage = BestCoverage.UNKNOWN;
    }

    /**
     * What the summaries settle about the merged column's dictionary. Summed counts under-state, so
     * reaching {@code minCoverage} with them proves a dictionary worth keeping; the bound over-states, so
     * falling short of it proves none can be. Anything else leaves the values to decide.
     *
     * @param countsAreLive whether the counts still describe values that are there. Deleted ones settle
     *                      nothing: they neither bound the column from above nor witness it from below.
     * @return the outcome, the vocabulary to write the merged column against where there is one, and the
     *         upperBound coverage the outcome was reached with
     */
    public Decision decide(boolean countsAreLive) {
        // NOTE: a deleted value is still counted in what its segment summarised, so stale counts neither
        // bound the column from above nor witness it from below, and a dictionary built from them could
        // name only terms that are gone. The values decide instead.
        if (dictionaryPolicy.enabled() == false || countsAreLive == false || everyInputSummarised == false || numValues == 0) {
            return new Decision(Outcome.UNDECIDED, null, BestCoverage.UNKNOWN);
        }
        final Vocabulary.Terms combined = retainedCounts.isEmpty()
            ? null
            : Vocabulary.combined(retainedCounts, columnBytes, numValues, dictionaryPolicy, summaryPolicy);
        final BestCoverage inheritedBound = summedBestCoverage == null ? BestCoverage.UNKNOWN : summedBestCoverage;
        final BestCoverage recomputedBound = combined == null ? BestCoverage.UNKNOWN : combined.bestCoverage();
        final BestCoverage upperBound = inheritedBound.validFor(dictionaryPolicy.maxBytes())
            ? inheritedBound.tighter(recomputedBound)
            : recomputedBound;
        if (combined != null && dictionaryPolicy.worthKeeping(combined.coverage(), combined.dictionaryBytes(), combined.columnBytes())) {
            return new Decision(Outcome.DICTIONARY, combined.withBestCoverage(upperBound), upperBound);
        }
        if (dictionaryPolicy.rulesOut(upperBound)) {
            return new Decision(Outcome.NO_DICTIONARY, Vocabulary.withoutDictionary(combined, upperBound), upperBound);
        }
        return new Decision(Outcome.UNDECIDED, null, upperBound);
    }

    private long trimToBound() {
        final List<Map.Entry<BytesRef, Long>> ranked = new ArrayList<>(retainedCounts.entrySet());
        ranked.sort(Map.Entry.<BytesRef, Long>comparingByValue().reversed().thenComparing(Map.Entry::getKey));
        long bytes = 0;
        int kept = 0;
        while (kept < ranked.size() && bytes + TermQuota.cost(ranked.get(kept).getKey()) <= maxRetainedTermBytes) {
            bytes += TermQuota.cost(ranked.get(kept).getKey());
            kept++;
        }
        for (int i = kept; i < ranked.size(); i++) {
            retainedCounts.remove(ranked.get(i).getKey());
        }
        return bytes;
    }
}
