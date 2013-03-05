/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.io.FastCharArrayReader;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.CandidateSet;

//TODO public for tests
public final class NoisyChannelSpellChecker {
    public static final double REAL_WORD_LIKELYHOOD = 0.95d;
    private final double realWordLikelihood;
    private final boolean requireUnigram;

    public NoisyChannelSpellChecker() {
        this(REAL_WORD_LIKELYHOOD);
    }

    public NoisyChannelSpellChecker(double nonErrorLikelihood) {
        this(nonErrorLikelihood, true);
    }
    
    public NoisyChannelSpellChecker(double nonErrorLikelihood, boolean requireUnigram) {
        this.realWordLikelihood = nonErrorLikelihood;
        this.requireUnigram = requireUnigram;
    }

    public Correction[] getCorrections(TokenStream stream, final CandidateGenerator generator, final int numCandidates,
            float maxErrors, int numCorrections, IndexReader reader, WordScorer wordScorer, BytesRef separator, float confidence, int gramSize) throws IOException {
        
        final List<CandidateSet> candidateSetsList = new ArrayList<DirectCandidateGenerator.CandidateSet>();
        SuggestUtils.analyze(stream, new SuggestUtils.TokenConsumer() {
            CandidateSet currentSet = null;
            private TypeAttribute typeAttribute;
            private final BytesRef termsRef = new BytesRef();
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
                    long freq = 0;
                    if ((freq = generator.frequency(term)) > 0) {
                        currentSet.addOneCandidate(generator.createCandidate(BytesRef.deepCopyOf(term), freq, realWordLikelihood));
                    }
                } else {
                    if (currentSet != null) {
                        candidateSetsList.add(currentSet);
                    }
                    currentSet = new CandidateSet(Candidate.EMPTY, generator.createCandidate(BytesRef.deepCopyOf(term)));
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
        
        for (CandidateSet candidateSet : candidateSetsList) {
            generator.drawCandidates(candidateSet, numCandidates);
        }
        double cutoffScore = Double.MIN_VALUE;
        CandidateScorer scorer = new CandidateScorer(wordScorer, numCorrections, gramSize);
        CandidateSet[] candidateSets = candidateSetsList.toArray(new CandidateSet[candidateSetsList.size()]);
        if (confidence > 0.0) {
            Candidate[] candidates = new Candidate[candidateSets.length];
            for (int i = 0; i < candidates.length; i++) {
                candidates[i] = candidateSets[i].originalTerm;
            }
            cutoffScore = scorer.score(candidates, candidateSets);
        }
        Correction[] findBestCandiates = scorer.findBestCandiates(candidateSets, maxErrors, cutoffScore * confidence);
        
        return findBestCandiates;
    }

    public Correction[] getCorrections(Analyzer analyzer, BytesRef query, CandidateGenerator generator, int numCandidates,
            float maxErrors, int numCorrections, IndexReader reader, String analysisField, WordScorer scorer, float confidence, int gramSize) throws IOException {
       
        return getCorrections(tokenStream(analyzer, query, new CharsRef(), analysisField), generator, numCandidates, maxErrors, numCorrections, reader, scorer, new BytesRef(" "), confidence, gramSize);

    }

    public TokenStream tokenStream(Analyzer analyzer, BytesRef query, CharsRef spare, String field) throws IOException {
        UnicodeUtil.UTF8toUTF16(query, spare);
        return analyzer.tokenStream(field, new FastCharArrayReader(spare.chars, spare.offset, spare.length));
    }
      

}
