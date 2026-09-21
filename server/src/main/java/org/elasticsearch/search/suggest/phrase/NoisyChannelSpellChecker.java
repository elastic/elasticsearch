/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.CandidateSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

final class NoisyChannelSpellChecker {
    public static final double REAL_WORD_LIKELIHOOD = 0.95d;
    public static final int DEFAULT_TOKEN_LIMIT = 10;
    /**
     * Upper bound for {@code token_limit}, i.e. for the number of analyzed tokens the phrase suggester will attempt to
     * correct.
     * <p>
     * {@link CandidateScorer#findCandidates} recurses exactly once per token, so the number of tokens is also the
     * recursion <em>depth</em>. With the default {@code -Xss1m} thread stack, a phrase of roughly 8,000 tokens
     * overflows the stack; the resulting {@link StackOverflowError} is treated as fatal and halts the node. Without an
     * upper bound a caller could set {@code token_limit} arbitrarily high, disable the early exit in
     * {@link #getCorrections}, and crash the node with a single request.
     * <p>
     * 1000 is 100x the default of {@link #DEFAULT_TOKEN_LIMIT} and roughly 8x below the depth at which the stack
     * overflows. This limit is about depth only; the amount of <em>work</em> done within that depth (the number of
     * candidate combinations scored) is bounded independently by {@link CandidateScorer#MAX_SCORED_PATHS}, and neither
     * limit substitutes for the other: a very long phrase with no alternatives performs few scoring steps yet still
     * recurses deeply, while a short phrase with many alternatives stays shallow yet explores a combinatorial number of
     * paths.
     * <p>
     * The bound is enforced when the request is parsed ({@link PhraseSuggestionBuilder#tokenLimit(int)}), when it is
     * deserialized from the transport layer (an older coordinating node may still forward a larger value during a
     * rolling upgrade), and again in the constructor here so that the scorer is protected regardless of how the value
     * reached it.
     */
    public static final int MAX_TOKEN_LIMIT = 1000;
    private final double realWordLikelihood;
    private final boolean requireUnigram;
    private final int tokenLimit;

    NoisyChannelSpellChecker(double nonErrorLikelihood, boolean requireUnigram, int tokenLimit) {
        this.realWordLikelihood = nonErrorLikelihood;
        this.requireUnigram = requireUnigram;
        this.tokenLimit = Math.min(tokenLimit, MAX_TOKEN_LIMIT);
    }

    Result getCorrections(
        TokenStream stream,
        final CandidateGenerator generator,
        float maxErrors,
        int numCorrections,
        WordScorer wordScorer,
        float confidence,
        int gramSize
    ) throws IOException {

        final List<CandidateSet> candidateSetsList = new ArrayList<>();
        DirectCandidateGenerator.analyze(stream, new DirectCandidateGenerator.TokenConsumer() {
            CandidateSet currentSet = null;
            private TypeAttribute typeAttribute;
            private final BytesRefBuilder termsRef = new BytesRefBuilder();
            private boolean anyUnigram = false;
            private boolean anyTokens = false;

            @Override
            public void reset(TokenStream stream) {
                super.reset(stream);
                typeAttribute = stream.addAttribute(TypeAttribute.class);
            }

            @Override
            public void nextToken() throws IOException {
                anyTokens = true;
                BytesRef term = fillBytesRef(termsRef);
                if (requireUnigram && typeAttribute.type() == ShingleFilter.DEFAULT_TOKEN_TYPE) {
                    return;
                }
                anyUnigram = true;
                if (posIncAttr.getPositionIncrement() == 0 && typeAttribute.type() == SynonymFilter.TYPE_SYNONYM) {
                    assert currentSet != null;
                    TermStats termStats = generator.termStats(term);
                    if (termStats.docFreq() > 0) {
                        currentSet.addOneCandidate(generator.createCandidate(BytesRef.deepCopyOf(term), termStats, realWordLikelihood));
                    }
                } else {
                    if (currentSet != null) {
                        candidateSetsList.add(currentSet);
                    }
                    currentSet = new CandidateSet(Candidate.EMPTY, generator.createCandidate(BytesRef.deepCopyOf(term), true));
                }
            }

            @Override
            public void end() {
                if (currentSet != null) {
                    candidateSetsList.add(currentSet);
                }
                if (requireUnigram && anyUnigram == false && anyTokens) {
                    throw new IllegalArgumentException("At least one unigram is required but all tokens were ngrams");
                }
            }
        });

        if (candidateSetsList.isEmpty() || candidateSetsList.size() >= tokenLimit) {
            return Result.EMPTY;
        }

        for (CandidateSet candidateSet : candidateSetsList) {
            generator.drawCandidates(candidateSet);
        }
        double cutoffScore = Double.MIN_VALUE;
        CandidateScorer scorer = new CandidateScorer(wordScorer, numCorrections, gramSize);
        CandidateSet[] candidateSets = candidateSetsList.toArray(new CandidateSet[candidateSetsList.size()]);
        if (confidence > 0.0) {
            Candidate[] candidates = new Candidate[candidateSets.length];
            for (int i = 0; i < candidates.length; i++) {
                candidates[i] = candidateSets[i].originalTerm;
            }
            double inputPhraseScore = scorer.score(candidates, candidateSets);
            cutoffScore = inputPhraseScore * confidence;
        }
        Correction[] bestCandidates = scorer.findBestCandiates(candidateSets, maxErrors, cutoffScore);

        return new Result(bestCandidates, cutoffScore);
    }

    static class Result {
        public static final Result EMPTY = new Result(Correction.EMPTY, Double.MIN_VALUE);
        public final Correction[] corrections;
        public final double cutoffScore;

        private Result(Correction[] corrections, double cutoffScore) {
            this.corrections = corrections;
            this.cutoffScore = cutoffScore;
        }
    }
}
