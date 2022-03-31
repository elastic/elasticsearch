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

class StupidBackoffScorer extends WordScorer {
    private final double discount;

    StupidBackoffScorer(IndexReader reader, Terms terms, String field, double realWordLikelihood, BytesRef separator, double discount)
        throws IOException {
        super(reader, terms, field, realWordLikelihood, separator);
        this.discount = discount;
    }

    double discount() {
        return this.discount;
    }

    @Override
    protected double scoreBigram(Candidate word, Candidate w_1) throws IOException {
        join(separator, spare, w_1.term, word.term);
        final long count = frequency(spare.get());
        if (count < 1) {
            return discount * scoreUnigram(word);
        }
        return count / (w_1.termStats.totalTermFreq + 0.00000000001d);
    }

    @Override
    protected double scoreTrigram(Candidate w, Candidate w_1, Candidate w_2) throws IOException {
        // First see if there are bigrams. If there aren't then skip looking up the trigram. This saves lookups
        // when the bigrams and trigrams are rare and we need both anyway.
        join(separator, spare, w_1.term, w.term);
        long bigramCount = frequency(spare.get());
        if (bigramCount < 1) {
            return discount * scoreUnigram(w);
        }
        join(separator, spare, w_2.term, w_1.term, w.term);
        long trigramCount = frequency(spare.get());
        if (trigramCount < 1) {
            return discount * (bigramCount / (w_1.termStats.totalTermFreq + 0.00000000001d));
        }
        return trigramCount / (bigramCount + 0.00000000001d);
    }

}
