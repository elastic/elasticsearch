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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;

import java.io.IOException;

public class StupidBackoffScorer extends WordScorer {
    public static final WordScorerFactory FACTORY = new WordScorer.WordScorerFactory() {
        @Override
        public WordScorer newScorer(IndexReader reader, Terms terms, String field, double realWordLikelyhood, BytesRef separator) throws IOException {
            return new StupidBackoffScorer(reader, terms, field, realWordLikelyhood, separator, 0.4f);
        }
    };

    private final double discount;

    public StupidBackoffScorer(IndexReader reader, Terms terms,String field, double realWordLikelyhood, BytesRef separator, double discount)
            throws IOException {
        super(reader, terms, field, realWordLikelyhood, separator);
        this.discount = discount;
    }

    @Override
    protected double scoreBigram(Candidate word, Candidate w_1) throws IOException {
        SuggestUtils.join(separator, spare, w_1.term, word.term);
        final long count = frequency(spare);
        if (count < 1) {
            return discount * scoreUnigram(word);
        }
        return count / (w_1.frequency + 0.00000000001d);
    }

    @Override
    protected double scoreTrigram(Candidate w, Candidate w_1, Candidate w_2) throws IOException {
        SuggestUtils.join(separator, spare, w_2.term, w_1.term, w.term);
        final long trigramCount = frequency(spare);
        if (trigramCount < 1) {
            SuggestUtils.join(separator, spare, w_1.term, w.term);
            final long count = frequency(spare);
            if (count < 1) {
                return discount * scoreUnigram(w);
            }
            return discount * (count / (w_1.frequency + 0.00000000001d));
        }
        SuggestUtils.join(separator, spare, w_1.term, w.term);
        final long bigramCount = frequency(spare);
        return trigramCount / (bigramCount + 0.00000000001d);
    }

}
