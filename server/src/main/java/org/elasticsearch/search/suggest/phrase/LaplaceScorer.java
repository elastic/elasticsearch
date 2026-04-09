/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;

import java.io.IOException;

final class LaplaceScorer extends WordScorer {
    private final double alpha;

    LaplaceScorer(IndexReader reader, Terms terms, String field, double realWordLikelihood, BytesRef separator, double alpha)
        throws IOException {
        super(reader, terms, field, realWordLikelihood, separator);
        this.alpha = alpha;
    }

    double alpha() {
        return this.alpha;
    }

    @Override
    protected double scoreUnigram(Candidate word) throws IOException {
        return (alpha + frequency(word.term)) / (vocabluarySize + alpha * numTerms);
    }

    @Override
    protected double scoreBigram(Candidate word, Candidate w_1) throws IOException {
        join(separator, spare, w_1.term, word.term);
        return (alpha + frequency(spare.get())) / (w_1.termStats.totalTermFreq() + alpha * numTerms);
    }

    @Override
    protected double scoreTrigram(Candidate word, Candidate w_1, Candidate w_2) throws IOException {
        join(separator, spare, w_2.term, w_1.term, word.term);
        long trigramCount = frequency(spare.get());
        join(separator, spare, w_1.term, word.term);
        return (alpha + trigramCount) / (frequency(spare.get()) + alpha * numTerms);
    }

}
