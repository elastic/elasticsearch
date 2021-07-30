/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
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
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.CharArrayReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.Math.log10;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.round;

public final class DirectCandidateGenerator extends CandidateGenerator {

    private final DirectSpellChecker spellchecker;
    private final String field;
    private final SuggestMode suggestMode;
    private final TermsEnum termsEnum;
    private final IndexReader reader;
    private final long sumTotalTermFreq;
    private static final double LOG_BASE = 5;
    private final long frequencyPlateau;
    private final Analyzer preFilter;
    private final Analyzer postFilter;
    private final double nonErrorLikelihood;
    private final CharsRefBuilder spare = new CharsRefBuilder();
    private final BytesRefBuilder byteSpare = new BytesRefBuilder();
    private final int numCandidates;

    public DirectCandidateGenerator(DirectSpellChecker spellchecker, String field, SuggestMode suggestMode, IndexReader reader,
            double nonErrorLikelihood, int numCandidates) throws IOException {
        this(spellchecker, field, suggestMode, reader, nonErrorLikelihood,
                numCandidates, null, null, MultiTerms.getTerms(reader, field));
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
        this.sumTotalTermFreq =  terms.getSumTotalTermFreq() == -1 ? reader.maxDoc() : terms.getSumTotalTermFreq();
        this.preFilter = preFilter;
        this.postFilter = postFilter;
        this.nonErrorLikelihood = nonErrorLikelihood;
        float thresholdFrequency = spellchecker.getThresholdFrequency();
        this.frequencyPlateau = thresholdFrequency >= 1.0f ? (int) thresholdFrequency: (int) (reader.maxDoc() * thresholdFrequency);
        termsEnum = terms.iterator();
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.suggest.phrase.CandidateGenerator#isKnownWord(org.apache.lucene.util.BytesRef)
     */
    @Override
    public boolean isKnownWord(BytesRef term) throws IOException {
        return termStats(term).docFreq > 0;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.suggest.phrase.CandidateGenerator#frequency(org.apache.lucene.util.BytesRef)
     */
    @Override
    public TermStats termStats(BytesRef term) throws IOException {
        term = preFilter(term, spare, byteSpare);
        return internalTermStats(term);
    }


    public TermStats internalTermStats(BytesRef term) throws IOException {
        if (termsEnum.seekExact(term)) {
            return new TermStats(termsEnum.docFreq(),
                /**
                 * We use the {@link TermsEnum#docFreq()} for fields that don't
                 * record the {@link TermsEnum#totalTermFreq()}.
                 */
                termsEnum.totalTermFreq() == -1 ? termsEnum.docFreq() : termsEnum.totalTermFreq());
        }
        return new TermStats(0, 0);
    }

    public String getField() {
        return field;
    }

    @Override
    public CandidateSet drawCandidates(CandidateSet set) throws IOException {
        Candidate original = set.originalTerm;
        BytesRef term = preFilter(original.term, spare, byteSpare);
        float origThreshold = spellchecker.getThresholdFrequency();
        try {
            if (suggestMode != SuggestMode.SUGGEST_ALWAYS) {
                /**
                 * We use the {@link TermStats#docFreq} to compute the frequency threshold
                 * because that's what {@link DirectSpellChecker#suggestSimilar} expects
                 * when filtering terms.
                 */
                int threshold = thresholdTermFrequency(original.termStats.docFreq);
                if (threshold == Integer.MAX_VALUE) {
                    // the threshold is the max possible frequency so we can skip the search
                    return set;
                }
                // don't override the threshold if the provided min_doc_freq is greater
                // than the original term frequency.
                if (spellchecker.getThresholdFrequency() < threshold) {
                    spellchecker.setThresholdFrequency(threshold);
                }
            }

            SuggestWord[] suggestSimilar = spellchecker.suggestSimilar(new Term(field, term), numCandidates, reader, this.suggestMode);
            List<Candidate> candidates = new ArrayList<>(suggestSimilar.length);
            for (int i = 0; i < suggestSimilar.length; i++) {
                SuggestWord suggestWord = suggestSimilar[i];
                BytesRef candidate = new BytesRef(suggestWord.string);
                TermStats termStats = internalTermStats(candidate);
                postFilter(new Candidate(candidate, termStats,
                    suggestWord.score, score(termStats, suggestWord.score, sumTotalTermFreq), false), spare, byteSpare, candidates);
            }
            set.addCandidates(candidates);
            return set;
        } finally {
            // restore the original value back
            spellchecker.setThresholdFrequency(origThreshold);
        }
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
                        TermStats termStats = internalTermStats(term);
                        candidates.add(new Candidate(result.toBytesRef(), termStats, candidate.stringDistance,
                                score(candidate.termStats, candidate.stringDistance, sumTotalTermFreq), false));
                    } else {
                        candidates.add(new Candidate(result.toBytesRef(), candidate.termStats, nonErrorLikelihood,
                                score(candidate.termStats, candidate.stringDistance, sumTotalTermFreq), false));
                    }
                }
            }, spare);
        }
    }

    private double score(TermStats termStats, double errorScore, long dictionarySize) {
        return errorScore * (((double)termStats.totalTermFreq + 1) / ((double)dictionarySize +1));
    }

    // package protected for test
    int thresholdTermFrequency(int docFreq) {
        if (docFreq > 0) {
            return (int) min(
                max(0, round(docFreq * (log10(docFreq - frequencyPlateau) * (1.0 / log10(LOG_BASE))) + 1)), Integer.MAX_VALUE
            );
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
        public final TermStats termStats;
        public final double score;
        public final boolean userInput;

        public Candidate(BytesRef term, TermStats termStats, double stringDistance, double score, boolean userInput) {
            this.termStats = termStats;
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
                    + ", termStats=" + termStats
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
                if (term.equals(other.term) == false) return false;
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
    public Candidate createCandidate(BytesRef term, TermStats termStats, double channelScore, boolean userInput) throws IOException {
        return new Candidate(term, termStats, channelScore, score(termStats, channelScore, sumTotalTermFreq), userInput);
    }

    public static int analyze(Analyzer analyzer, BytesRef toAnalyze, String field, TokenConsumer consumer, CharsRefBuilder spare)
            throws IOException {
        spare.copyUTF8Bytes(toAnalyze);
        CharsRef charsRef = spare.get();
        try (TokenStream ts = analyzer.tokenStream(
                                  field, new CharArrayReader(charsRef.chars, charsRef.offset, charsRef.length))) {
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
