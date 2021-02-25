/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;

import java.io.IOException;

//TODO public for tests
public final class LinearInterpolatingScorer extends WordScorer {

    private final double unigramLambda;
    private final double bigramLambda;
    private final double trigramLambda;

    public LinearInterpolatingScorer(IndexReader reader, Terms terms, String field, double realWordLikelihood, BytesRef separator,
            double trigramLambda, double bigramLambda, double unigramLambda) throws IOException {
        super(reader, terms, field, realWordLikelihood, separator);
        double sum = unigramLambda + bigramLambda + trigramLambda;
        this.unigramLambda = unigramLambda / sum;
        this.bigramLambda = bigramLambda / sum;
        this.trigramLambda = trigramLambda / sum;
    }

    double trigramLambda() {
        return this.trigramLambda;
    }

    double bigramLambda() {
        return this.bigramLambda;
    }

    double unigramLambda() {
        return this.unigramLambda;
    }

    @Override
    protected double scoreBigram(Candidate word, Candidate w_1) throws IOException {
        join(separator, spare, w_1.term, word.term);
        final long count = frequency(spare.get());
        if (count < 1) {
            return unigramLambda * scoreUnigram(word);
        }
        return bigramLambda * (count / (0.5d + w_1.termStats.totalTermFreq)) + unigramLambda * scoreUnigram(word);
    }

    @Override
    protected double scoreTrigram(Candidate w, Candidate w_1, Candidate w_2) throws IOException {
        join(separator, spare, w.term, w_1.term, w_2.term);
        final long count = frequency(spare.get());
        if (count < 1) {
            return scoreBigram(w, w_1);
        }
        join(separator, spare, w.term, w_1.term);
        return trigramLambda * (count / (1.d + frequency(spare.get()))) + scoreBigram(w, w_1);
    }

}
