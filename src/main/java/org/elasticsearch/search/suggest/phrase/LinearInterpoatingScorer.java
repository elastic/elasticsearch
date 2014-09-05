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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;

import java.io.IOException;

//TODO public for tests
public final class LinearInterpoatingScorer extends WordScorer {

    private final double unigramLambda;
    private final double bigramLambda;
    private final double trigramLambda;

    public LinearInterpoatingScorer(IndexReader reader, Terms terms, String field,  double realWordLikelyhood, BytesRef separator, double trigramLambda, double bigramLambda, double unigramLambda)
            throws IOException {
        super(reader, terms, field, realWordLikelyhood, separator);
        double sum = unigramLambda + bigramLambda + trigramLambda;
        this.unigramLambda = unigramLambda / sum;
        this.bigramLambda = bigramLambda / sum;
        this.trigramLambda = trigramLambda / sum;
    }
    
    @Override
    protected double scoreBigram(Candidate word, Candidate w_1) throws IOException {
        SuggestUtils.join(separator, spare, w_1.term, word.term);
        final long count = frequency(spare.get());
        if (count < 1) {
            return unigramLambda * scoreUnigram(word);
        }
        return bigramLambda * (count / (0.5d+w_1.frequency)) + unigramLambda * scoreUnigram(word);
    }

    @Override
    protected double scoreTrigram(Candidate w, Candidate w_1, Candidate w_2) throws IOException {
        SuggestUtils.join(separator, spare, w.term, w_1.term, w_2.term);
        final long count = frequency(spare.get());
        if (count < 1) {
            return scoreBigram(w, w_1);
        }
        SuggestUtils.join(separator, spare, w.term, w_1.term);
        return trigramLambda * (count / (1.d+frequency(spare.get()))) + scoreBigram(w, w_1);
    }
}
