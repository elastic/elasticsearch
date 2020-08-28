/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    private final double realWordLikelihood;
    private final boolean requireUnigram;
    private final int tokenLimit;

    NoisyChannelSpellChecker(double nonErrorLikelihood, boolean requireUnigram, int tokenLimit) {
        this.realWordLikelihood = nonErrorLikelihood;
        this.requireUnigram = requireUnigram;
        this.tokenLimit = tokenLimit;
    }

    Result getCorrections(TokenStream stream, final CandidateGenerator generator,
            float maxErrors, int numCorrections, WordScorer wordScorer, float confidence, int gramSize) throws IOException {

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
                    if (termStats.docFreq > 0) {
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
                if (requireUnigram && !anyUnigram && anyTokens) {
                    throw new IllegalStateException("At least one unigram is required but all tokens were ngrams");
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
