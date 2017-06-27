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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.io.FastCharArrayReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.Math.log10;
import static java.lang.Math.max;
import static java.lang.Math.round;

public final class DirectCandidateGenerator extends CandidateGenerator {

    private final DirectSpellChecker spellchecker;
    private final String field;
    private final SuggestMode suggestMode;
    private final TermsEnum termsEnum;
    private final IndexReader reader;
    private final long dictSize;
    private static final double LOG_BASE = 5;
    private final long frequencyPlateau;
    private final Analyzer preFilter;
    private final Analyzer postFilter;
    private final double nonErrorLikelihood;
    private final boolean useTotalTermFrequency;
    private final CharsRefBuilder spare = new CharsRefBuilder();
    private final BytesRefBuilder byteSpare = new BytesRefBuilder();
    private final int numCandidates;

    public DirectCandidateGenerator(DirectSpellChecker spellchecker, String field, SuggestMode suggestMode, IndexReader reader,
            double nonErrorLikelihood, int numCandidates) throws IOException {
        this(spellchecker, field, suggestMode, reader, nonErrorLikelihood, numCandidates, null, null, MultiFields.getTerms(reader, field));
    }

    public DirectCandidateGenerator(DirectSpellChecker spellchecker, String field, SuggestMode suggestMode, IndexReader reader,
            double nonErrorLikelihood, int numCandidates, Analyzer preFilter, Analyzer postFilter, Terms terms) throws IOException {
        if (terms == null) {
            throw new IllegalArgumentException("generator field [" + field + "] doesn't exist");
        }
        this.spellchecker = spellchecker;
        this.field = field;
        this.numCandidates = numCandidates;
        this.suggestMode = suggestMode;
        this.reader = reader;
        final long dictSize = terms.getSumTotalTermFreq();
        this.useTotalTermFrequency = dictSize != -1;
        this.dictSize =  dictSize == -1 ? reader.maxDoc() : dictSize;
        this.preFilter = preFilter;
        this.postFilter = postFilter;
        this.nonErrorLikelihood = nonErrorLikelihood;
        float thresholdFrequency = spellchecker.getThresholdFrequency();
        this.frequencyPlateau = thresholdFrequency >= 1.0f ? (int) thresholdFrequency: (int)(dictSize * thresholdFrequency);
        termsEnum = terms.iterator();
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
    public long frequency(BytesRef term) throws IOException {
        term = preFilter(term, spare, byteSpare);
        return internalFrequency(term);
    }


    public long internalFrequency(BytesRef term) throws IOException {
        if (termsEnum.seekExact(term)) {
            return useTotalTermFrequency ? termsEnum.totalTermFreq() : termsEnum.docFreq();
        }
        return 0;
    }

    public String getField() {
        return field;
    }

    @Override
    public CandidateSet drawCandidates(CandidateSet set) throws IOException {
        Candidate original = set.originalTerm;
        BytesRef term = preFilter(original.term, spare, byteSpare);
        final long frequency = original.frequency;
        spellchecker.setThresholdFrequency(this.suggestMode == SuggestMode.SUGGEST_ALWAYS ? 0 : thresholdFrequency(frequency, dictSize));
        SuggestWord[] suggestSimilar = spellchecker.suggestSimilar(new Term(field, term), numCandidates, reader, this.suggestMode);
        List<Candidate> candidates = new ArrayList<>(suggestSimilar.length);
        for (int i = 0; i < suggestSimilar.length; i++) {
            SuggestWord suggestWord = suggestSimilar[i];
            BytesRef candidate = new BytesRef(suggestWord.string);
            postFilter(new Candidate(candidate, internalFrequency(candidate), suggestWord.score,
                    score(suggestWord.freq, suggestWord.score, dictSize), false), spare, byteSpare, candidates);
        }
        set.addCandidates(candidates);
        return set;
    }

    protected BytesRef preFilter(final BytesRef term, final CharsRefBuilder spare, final BytesRefBuilder byteSpare) throws IOException {
        if (preFilter == null) {
            return term;
        }
        final BytesRefBuilder result = byteSpare;
        analyze(preFilter, term, field, new TokenConsumer() {

            @Override
            public void nextToken() throws IOException {
                this.fillBytesRef(result);
            }
        }, spare);
        return result.get();
    }

    protected void postFilter(final Candidate candidate, final CharsRefBuilder spare, BytesRefBuilder byteSpare,
            final List<Candidate> candidates) throws IOException {
        if (postFilter == null) {
            candidates.add(candidate);
        } else {
            final BytesRefBuilder result = byteSpare;
            analyze(postFilter, candidate.term, field, new TokenConsumer() {
                @Override
                public void nextToken() throws IOException {
                    this.fillBytesRef(result);

                    if (posIncAttr.getPositionIncrement() > 0 && result.get().bytesEquals(candidate.term))  {
                        BytesRef term = result.toBytesRef();
                        // We should not use frequency(term) here because it will analyze the term again
                        // If preFilter and postFilter are the same analyzer it would fail.
                        long freq = internalFrequency(term);
                        candidates.add(new Candidate(result.toBytesRef(), freq, candidate.stringDistance,
                                score(candidate.frequency, candidate.stringDistance, dictSize), false));
                    } else {
                        candidates.add(new Candidate(result.toBytesRef(), candidate.frequency, nonErrorLikelihood,
                                score(candidate.frequency, candidate.stringDistance, dictSize), false));
                    }
                }
            }, spare);
        }
    }

    private double score(long frequency, double errorScore, long dictionarySize) {
        return errorScore * (((double)frequency + 1) / ((double)dictionarySize +1));
    }

    protected long thresholdFrequency(long termFrequency, long dictionarySize) {
        if (termFrequency > 0) {
            return max(0, round(termFrequency * (log10(termFrequency - frequencyPlateau) * (1.0 / log10(LOG_BASE))) + 1));
        }
        return 0;

    }

    public abstract static class TokenConsumer {
        protected CharTermAttribute charTermAttr;
        protected PositionIncrementAttribute posIncAttr;
        protected OffsetAttribute offsetAttr;

        public void reset(TokenStream stream) {
            charTermAttr = stream.addAttribute(CharTermAttribute.class);
            posIncAttr = stream.addAttribute(PositionIncrementAttribute.class);
            offsetAttr = stream.addAttribute(OffsetAttribute.class);
        }

        protected BytesRef fillBytesRef(BytesRefBuilder spare) {
            spare.copyChars(charTermAttr);
            return spare.get();
        }

        public abstract void nextToken() throws IOException;

        public void end() {}
    }

    public static class CandidateSet {
        public Candidate[] candidates;
        public final Candidate originalTerm;

        public CandidateSet(Candidate[] candidates, Candidate originalTerm) {
            this.candidates = candidates;
            this.originalTerm = originalTerm;
        }

        public void addCandidates(List<Candidate> candidates) {
            // Merge new candidates into existing ones,
            // deduping:
            final Set<Candidate> set = new HashSet<>(candidates);
            Collections.addAll(set, this.candidates);

            this.candidates = set.toArray(new Candidate[set.size()]);
            // Sort strongest to weakest:
            Arrays.sort(this.candidates, Collections.reverseOrder());
        }

        public void addOneCandidate(Candidate candidate) {
            Candidate[] candidates = new Candidate[this.candidates.length + 1];
            System.arraycopy(this.candidates, 0, candidates, 0, this.candidates.length);
            candidates[candidates.length-1] = candidate;
            this.candidates = candidates;
        }

    }

    public static class Candidate implements Comparable<Candidate> {
        public static final Candidate[] EMPTY = new Candidate[0];
        public final BytesRef term;
        public final double stringDistance;
        public final long frequency;
        public final double score;
        public final boolean userInput;

        public Candidate(BytesRef term, long frequency, double stringDistance, double score, boolean userInput) {
            this.frequency = frequency;
            this.term = term;
            this.stringDistance = stringDistance;
            this.score = score;
            this.userInput = userInput;
        }

        @Override
        public String toString() {
            return "Candidate [term=" + term.utf8ToString()
                    + ", stringDistance=" + stringDistance
                    + ", score=" + score
                    + ", frequency=" + frequency
                    + (userInput ? ", userInput" : "") + "]";
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
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            Candidate other = (Candidate) obj;
            if (term == null) {
                if (other.term != null) return false;
            } else {
                if (!term.equals(other.term)) return false;
            }
            return true;
        }

        /** Lower scores sort first; if scores are equal, then later (zzz) terms sort first */
        @Override
        public int compareTo(Candidate other) {
            if (score == other.score) {
                // Later (zzz) terms sort before earlier (aaa) terms:
                return other.term.compareTo(term);
            } else {
                return Double.compare(score, other.score);
            }
        }
    }

    @Override
    public Candidate createCandidate(BytesRef term, long frequency, double channelScore, boolean userInput) throws IOException {
        return new Candidate(term, frequency, channelScore, score(frequency, channelScore, dictSize), userInput);
    }

    public static int analyze(Analyzer analyzer, BytesRef toAnalyze, String field, TokenConsumer consumer, CharsRefBuilder spare)
            throws IOException {
        spare.copyUTF8Bytes(toAnalyze);
        CharsRef charsRef = spare.get();
        try (TokenStream ts = analyzer.tokenStream(
                                  field, new FastCharArrayReader(charsRef.chars, charsRef.offset, charsRef.length))) {
             return analyze(ts, consumer);
        }
    }

    /** NOTE: this method closes the TokenStream, even on exception, which is awkward
     *  because really the caller who called {@link Analyzer#tokenStream} should close it,
     *  but when trying that there are recursion issues when we try to use the same
     *  TokenStream twice in the same recursion... */
    public static int analyze(TokenStream stream, TokenConsumer consumer) throws IOException {
        int numTokens = 0;
        boolean success = false;
        try {
            stream.reset();
            consumer.reset(stream);
            while (stream.incrementToken()) {
                consumer.nextToken();
                numTokens++;
            }
            consumer.end();
            success = true;
        } finally {
            if (success) {
                stream.close();
            } else {
                IOUtils.closeWhileHandlingException(stream);
            }
        }
        return numTokens;
    }

}
