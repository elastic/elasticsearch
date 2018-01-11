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
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;

import java.io.IOException;

final class LaplaceScorer extends WordScorer {
    private double alpha;

    LaplaceScorer(IndexReader reader, Terms terms, String field,
            double realWordLikelyhood, BytesRef separator, double alpha) throws IOException {
        super(reader, terms, field, realWordLikelyhood, separator);
        this.alpha = alpha;
    }

    double alpha() {
        return this.alpha;
    }

    @Override
    protected double scoreBigram(Candidate word, Candidate w_1) throws IOException {
        join(separator, spare, w_1.term, word.term);
        return (alpha + frequency(spare.get())) / (alpha +  w_1.frequency + vocabluarySize);
    }

    @Override
    protected double scoreTrigram(Candidate word, Candidate w_1, Candidate w_2) throws IOException {
        join(separator, spare, w_2.term, w_1.term, word.term);
        long trigramCount = frequency(spare.get());
        join(separator, spare, w_1.term, word.term);
        return (alpha + trigramCount) / (alpha  +  frequency(spare.get()) + vocabluarySize);
    }


}
