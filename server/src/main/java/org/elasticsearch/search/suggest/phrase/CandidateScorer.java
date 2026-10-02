/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.CandidateSet;

import java.io.IOException;

final class CandidateScorer {
    /**
     * Upper bound on the number of scoring steps (recursion entries plus fully scored combinations) a single request
     * may perform in {@link #findCandidates}. The search space grows combinatorially with the number of tokens,
     * {@code max_errors} and the number of candidates per term, none of which are otherwise bounded tightly enough to
     * prevent a single request from consuming CPU indefinitely.
     * <p>
     * This bounds the <em>width</em> of the search (how many combinations are tried). It does not bound its
     * <em>depth</em>, which is one recursion frame per token and is limited separately by
     * {@link NoisyChannelSpellChecker#MAX_TOKEN_LIMIT}.
     * <p>
     * Sizing: under default settings the largest possible search is 9 tokens (one below the default
     * {@code token_limit} of 10) with {@code max_errors} 0.5, i.e. up to 5 corrected positions, and 5 candidates per
     * term. That is {@code sum(C(9,j) * 5^j, j=0..5)}, roughly 484,000 scored combinations and around 600,000 steps
     * including the recursion entries. The budget is therefore about 15-20x the default worst case, leaving ample
     * headroom for legitimate queries while turning an otherwise unbounded computation into one that fails with a
     * client error after a bounded amount of work.
     */
    static final long MAX_SCORED_PATHS = 10_000_000L;

    private final WordScorer scorer;
    private final int maxNumCorrections;
    private final int gramSize;
    private final long maxScoredPaths;

    /**
     * Number of scoring steps performed by the current {@link #findBestCandiates} call. A {@code CandidateScorer} is
     * created per request and the recursion in {@link #findCandidates} needs a single shared counter, so this lives on
     * the instance rather than being threaded through the recursive calls.
     */
    private long scoredPaths;

    CandidateScorer(WordScorer scorer, int maxNumCorrections, int gramSize) {
        this(scorer, maxNumCorrections, gramSize, MAX_SCORED_PATHS);
    }

    // Visible for testing: allows exercising the budget without performing MAX_SCORED_PATHS scoring steps.
    CandidateScorer(WordScorer scorer, int maxNumCorrections, int gramSize, long maxScoredPaths) {
        this.scorer = scorer;
        this.maxNumCorrections = maxNumCorrections;
        this.gramSize = gramSize;
        this.maxScoredPaths = maxScoredPaths;
    }

    public Correction[] findBestCandiates(CandidateSet[] sets, float errorFraction, double cutoffScore) throws IOException {
        if (sets.length == 0) {
            return Correction.EMPTY;
        }
        PriorityQueue<Correction> corrections = new PriorityQueue<>(maxNumCorrections) {
            @Override
            protected boolean lessThan(Correction a, Correction b) {
                return a.compareTo(b) < 0;
            }
        };
        final int numMissspellings;
        if (errorFraction >= 1.0) {
            numMissspellings = (int) errorFraction;
        } else {
            numMissspellings = Math.round(errorFraction * sets.length);
        }
        scoredPaths = 0;
        findCandidates(sets, new Candidate[sets.length], 0, Math.max(1, numMissspellings), corrections, cutoffScore, 0.0);
        Correction[] result = new Correction[corrections.size()];
        for (int i = result.length - 1; i >= 0; i--) {
            result[i] = corrections.pop();
        }
        assert corrections.size() == 0;
        return result;

    }

    public void findCandidates(
        CandidateSet[] candidates,
        Candidate[] path,
        int ord,
        int numMissspellingsLeft,
        PriorityQueue<Correction> corrections,
        double cutoffScore,
        final double pathScore
    ) throws IOException {
        // Every entry into this method scores the term at position `ord` once.
        scoredPaths++;
        ensureWithinBudget();
        CandidateSet current = candidates[ord];
        if (ord == candidates.length - 1) {
            path[ord] = current.originalTerm;
            updateTop(candidates, path, corrections, cutoffScore, pathScore + scorer.score(path, ord, gramSize));
            if (numMissspellingsLeft > 0) {
                for (int i = 0; i < current.candidates.length; i++) {
                    // Each alternative at the last position is a complete combination that is scored without a further
                    // recursive call, so it must be counted here or a single position with many candidates escapes the budget.
                    scoredPaths++;
                    ensureWithinBudget();
                    path[ord] = current.candidates[i];
                    updateTop(candidates, path, corrections, cutoffScore, pathScore + scorer.score(path, ord, gramSize));
                }
            }
        } else {
            if (numMissspellingsLeft > 0) {
                path[ord] = current.originalTerm;
                findCandidates(
                    candidates,
                    path,
                    ord + 1,
                    numMissspellingsLeft,
                    corrections,
                    cutoffScore,
                    pathScore + scorer.score(path, ord, gramSize)
                );
                for (int i = 0; i < current.candidates.length; i++) {
                    path[ord] = current.candidates[i];
                    findCandidates(
                        candidates,
                        path,
                        ord + 1,
                        numMissspellingsLeft - 1,
                        corrections,
                        cutoffScore,
                        pathScore + scorer.score(path, ord, gramSize)
                    );
                }
            } else {
                path[ord] = current.originalTerm;
                findCandidates(candidates, path, ord + 1, 0, corrections, cutoffScore, pathScore + scorer.score(path, ord, gramSize));
            }
        }

    }

    /**
     * Read-only check of the work performed so far against the budget. Callers increment {@link #scoredPaths} at the
     * point where a scoring step actually happens, so that the accounting is visible where the work is done.
     */
    private void ensureWithinBudget() {
        if (scoredPaths > maxScoredPaths) {
            throw new IllegalArgumentException(
                "phrase suggester query is too complex and exceeded maximum combinations [" + maxScoredPaths + "]; reduce token_limit"
            );
        }
    }

    private void updateTop(
        CandidateSet[] candidates,
        Candidate[] path,
        PriorityQueue<Correction> corrections,
        double cutoffScore,
        double score
    ) throws IOException {
        score = Math.exp(score);
        assert Math.abs(score - score(path, candidates)) < 0.00001 : "cur_score=" + score + ", path_score=" + score(path, candidates);
        if (score > cutoffScore) {
            if (corrections.size() < maxNumCorrections) {
                Candidate[] c = new Candidate[candidates.length];
                System.arraycopy(path, 0, c, 0, path.length);
                corrections.add(new Correction(score, c));
            } else if (corrections.top().compareTo(score, path) < 0) {
                Correction top = corrections.top();
                System.arraycopy(path, 0, top.candidates, 0, path.length);
                top.score = score;
                corrections.updateTop();
            }
        }
    }

    public double score(Candidate[] path, CandidateSet[] candidates) throws IOException {
        double score = 0.0d;
        for (int i = 0; i < candidates.length; i++) {
            score += scorer.score(path, i, gramSize);
        }
        return Math.exp(score);
    }
}
