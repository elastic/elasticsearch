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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.suggest.SuggestUtils;

//TODO public for tests
public final class DirectCandidateGenerator extends CandidateGenerator {

    private final DirectSpellChecker spellchecker;
    private final String field;
    private final SuggestMode suggestMode;
    private final IndexReader reader;
    private final int docCount;
    private final double logBase = 5;
    private final int frequencyPlateau;
    private final Analyzer preFilter;
    private final Analyzer postFilter;
    private final double nonErrorLikelihood;
    
    public DirectCandidateGenerator(DirectSpellChecker spellchecker, String field, SuggestMode suggestMode, IndexReader reader, double nonErrorLikelihood) throws IOException {
        this(spellchecker, field, suggestMode, reader,  nonErrorLikelihood, null, null);
    }


    public DirectCandidateGenerator(DirectSpellChecker spellchecker, String field, SuggestMode suggestMode, IndexReader reader, double nonErrorLikelihood, Analyzer preFilter, Analyzer postFilter) throws IOException {
        this.spellchecker = spellchecker;
        this.field = field;
        this.suggestMode = suggestMode;
        this.reader = reader;
        Terms terms = MultiFields.getTerms(reader, field);
        if (terms == null) {
            throw new ElasticSearchIllegalArgumentException("generator field [" + field + "] doesn't exist");
        }
        final int docCount = terms.getDocCount();
        this.docCount =  docCount == -1 ? reader.maxDoc() : docCount;
        this.preFilter = preFilter;
        this.postFilter = postFilter;
        this.nonErrorLikelihood = nonErrorLikelihood;
        float thresholdFrequency = spellchecker.getThresholdFrequency();
        this.frequencyPlateau = thresholdFrequency >= 1.0f ? (int) thresholdFrequency: (int)(docCount * thresholdFrequency);
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.suggest.phrase.CandidateGenerator#isKnownWord(org.apache.lucene.util.BytesRef)
     */
    @Override
    public boolean isKnownWord(BytesRef term) throws IOException {
        return frequency(term) > 0;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.suggest.phrase.CandidateGenerator#frequency(org.apache.lucene.util.BytesRef)
     */
    @Override
    public int frequency(BytesRef term) throws IOException {
        return reader.docFreq(new Term(field, term));
    }
    
    public String getField() {
        return field;
    }
    
    /* (non-Javadoc)
     * @see org.elasticsearch.search.suggest.phrase.CandidateGenerator#drawCandidates(org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.CandidateSet, int)
     */
    @Override
    public CandidateSet drawCandidates(CandidateSet set, int numCandidates) throws IOException {
        CharsRef spare = new CharsRef();
        BytesRef byteSpare = new BytesRef();
        Candidate original = set.originalTerm;
        BytesRef term = preFilter(original.term, spare, byteSpare);
        final int frequency = original.frequency;
        spellchecker.setThresholdFrequency(thresholdFrequency(frequency, docCount));
        SuggestWord[] suggestSimilar = spellchecker.suggestSimilar(new Term(field, term), numCandidates, reader, this.suggestMode);
        List<Candidate> candidates = new ArrayList<Candidate>(suggestSimilar.length);
        for (int i = 0; i < suggestSimilar.length; i++) {
            SuggestWord suggestWord = suggestSimilar[i];
            BytesRef candidate = new BytesRef(suggestWord.string);
            postFilter(new Candidate(candidate, suggestWord.freq, suggestWord.score, score(suggestWord.freq, suggestWord.score, docCount)), spare, byteSpare, candidates);
        }
        set.addCandidates(candidates);
        return set;
    }
    
    protected BytesRef preFilter(final BytesRef term, final CharsRef spare, final BytesRef byteSpare) throws IOException {
        if (preFilter == null) {
            return term;
        }
        final BytesRef result = byteSpare;
        SuggestUtils.analyze(preFilter, term, field, new SuggestUtils.TokenConsumer() {
            
            @Override
            public void nextToken() throws IOException {
                this.fillBytesRef(result);
            }
        }, spare);
        return result;
    }
    
    protected void postFilter(final Candidate candidate, final CharsRef spare, BytesRef byteSpare, final List<Candidate> candidates) throws IOException {
        if (postFilter == null) {
            candidates.add(candidate);
        } else {
            final BytesRef result = byteSpare;
            SuggestUtils.analyze(postFilter, candidate.term, field, new SuggestUtils.TokenConsumer() {
                @Override
                public void nextToken() throws IOException {
                    this.fillBytesRef(result);
                    if (posIncAttr.getPositionIncrement() > 0 && result.bytesEquals(candidate.term))  {
                        candidates.add(new Candidate(BytesRef.deepCopyOf(result), candidate.frequency, candidate.stringDistance, score(candidate.frequency, candidate.stringDistance, docCount)));
                    } else {
                        int freq = frequency(result);
                        candidates.add(new Candidate(BytesRef.deepCopyOf(result), freq, nonErrorLikelihood, score(candidate.frequency, candidate.stringDistance, docCount)));
                    }
                }
            }, spare);
        }
    }
    
    private double score(int frequency, double errorScore, int docCount) {
        return errorScore * (((double)frequency + 1) / ((double)docCount +1));
    }
    
    protected int thresholdFrequency(int termFrequency, int docCount) {
        if (termFrequency > 0) {
            return (int) Math.round(termFrequency * (Math.log10(termFrequency - frequencyPlateau) * (1.0 / Math.log10(logBase))) + 1);
        }
        return 0;
        
    }
    
    public static class CandidateSet {
        public Candidate[] candidates;
        public final Candidate originalTerm;

        public CandidateSet(Candidate[] candidates, Candidate originalTerm) {
            this.candidates = candidates;
            this.originalTerm = originalTerm;
        }
        
        public void addCandidates(List<Candidate> candidates) {
            final Set<Candidate> set = new HashSet<DirectCandidateGenerator.Candidate>(candidates);
            for (int i = 0; i < this.candidates.length; i++) {
                set.add(this.candidates[i]);
            }
            this.candidates = set.toArray(new Candidate[set.size()]);
        }

        public void addOneCandidate(Candidate candidate) {
            Candidate[] candidates = new Candidate[this.candidates.length + 1];
            System.arraycopy(this.candidates, 0, candidates, 0, this.candidates.length);
            candidates[candidates.length-1] = candidate;
            this.candidates = candidates;
        }

    }

    public static class Candidate {
        public static final Candidate[] EMPTY = new Candidate[0];
        public final BytesRef term;
        public final double stringDistance;
        public final int frequency;
        public final double score;

        public Candidate(BytesRef term, int frequency, double stringDistance, double score) {
            this.frequency = frequency;
            this.term = term;
            this.stringDistance = stringDistance;
            this.score = score;
        }

        @Override
        public String toString() {
            return "Candidate [term=" + term.utf8ToString() + ", stringDistance=" + stringDistance + ", frequency=" + frequency + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((term == null) ? 0 : term.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Candidate other = (Candidate) obj;
            if (term == null) {
                if (other.term != null)
                    return false;
            } else if (!term.equals(other.term))
                return false;
            return true;
        }
    }

    @Override
    public Candidate createCandidate(BytesRef term, int frequency, double channelScore) throws IOException {
        return new Candidate(term, frequency, channelScore, score(frequency, channelScore, docCount));
    }

}
