/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lucene.index.FreqTermsEnum;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.CandidateSet;

import java.io.IOException;

//TODO public for tests
public abstract class WordScorer {
    protected final IndexReader reader;
    protected final String field;
    protected final Terms terms;
    protected final long vocabluarySize;
    protected final double realWordLikelihood;
    protected final BytesRefBuilder spare = new BytesRefBuilder();
    protected final BytesRef separator;
    protected final long numTerms;
    private final TermsEnum termsEnum;
    private final boolean useTotalTermFreq;

    public WordScorer(IndexReader reader, String field, double realWordLikelihood, BytesRef separator) throws IOException {
        this(reader, MultiTerms.getTerms(reader, field), field, realWordLikelihood, separator);
    }

    public WordScorer(IndexReader reader, Terms terms, String field, double realWordLikelihood, BytesRef separator) throws IOException {
        this.field = field;
        if (terms == null) {
            throw new IllegalArgumentException("Field: [" + field + "] does not exist");
        }
        this.terms = terms;
        final long vocSize = terms.getSumTotalTermFreq();
        this.vocabluarySize = vocSize == -1 ? reader.maxDoc() : vocSize;
        this.useTotalTermFreq = vocSize != -1;
        // terms.size() might be -1 if it's a MultiTerms instance. In that case,
        // use reader.maxDoc() as an approximation. This also protects from
        // division by zero, by scoreUnigram.
        final long nTerms = terms.size();
        this.numTerms = nTerms == -1 ? reader.maxDoc() : nTerms;
        // non recycling for now
        this.termsEnum = new FreqTermsEnum(
            reader,
            field,
            useTotalTermFreq == false,
            useTotalTermFreq,
            null,
            BigArrays.NON_RECYCLING_INSTANCE
        );
        this.reader = reader;
        this.realWordLikelihood = realWordLikelihood;
        this.separator = separator;
    }

    public long frequency(BytesRef term) throws IOException {
        if (termsEnum.seekExact(term)) {
            return useTotalTermFreq ? termsEnum.totalTermFreq() : termsEnum.docFreq();
        }
        return 0;
    }

    protected double channelScore(Candidate candidate, Candidate original) throws IOException {
        if (candidate.stringDistance == 1.0d) {
            return realWordLikelihood;
        }
        return candidate.stringDistance;
    }

    public double score(Candidate[] path, CandidateSet[] candidateSet, int at, int gramSize) throws IOException {
        if (at == 0 || gramSize == 1) {
            return Math.log10(channelScore(path[at], candidateSet[at].originalTerm) * scoreUnigram(path[at]));
        } else if (at == 1 || gramSize == 2) {
            return Math.log10(channelScore(path[at], candidateSet[at].originalTerm) * scoreBigram(path[at], path[at - 1]));
        } else {
            return Math.log10(channelScore(path[at], candidateSet[at].originalTerm) * scoreTrigram(path[at], path[at - 1], path[at - 2]));
        }
    }

    protected double scoreUnigram(Candidate word) throws IOException {
        return (1.0 + frequency(word.term)) / (vocabluarySize + numTerms);
    }

    protected double scoreBigram(Candidate word, Candidate w_1) throws IOException {
        return scoreUnigram(word);
    }

    protected double scoreTrigram(Candidate word, Candidate w_1, Candidate w_2) throws IOException {
        return scoreBigram(word, w_1);
    }

    public static BytesRef join(BytesRef separator, BytesRefBuilder result, BytesRef... toJoin) {
        result.clear();
        for (int i = 0; i < toJoin.length - 1; i++) {
            result.append(toJoin[i]);
            result.append(separator);
        }
        result.append(toJoin[toJoin.length - 1]);
        return result.get();
    }

    public interface WordScorerFactory {
        WordScorer newScorer(IndexReader reader, Terms terms, String field, double realWordLikelihood, BytesRef separator)
            throws IOException;
    }
}
