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

package org.elasticsearch.search.suggest;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchIllegalArgumentException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 */
public class SuggestionSearchContext {

    private final Map<String, Suggestion> suggestions = new LinkedHashMap<String, Suggestion>(4);

    public void addSuggestion(String name, Suggestion suggestion) {
        suggestions.put(name, suggestion);
    }

    public Map<String, Suggestion> suggestions() {
        return suggestions;
    }

    public static class Suggestion {

        private String suggester;
        private BytesRef text;
        private String field;
        private Analyzer analyzer;
        private SuggestMode suggestMode;
        private Float accuracy;
        private Integer size;
        private Suggest.Suggestion.Sort sort;
        private StringDistance stringDistance;
        private Boolean lowerCaseTerms;
        private Integer maxEdits;
        private Integer factor;
        private Float maxTermFreq;
        private Integer prefixLength;
        private Integer minWordLength;
        private Float minDocFreq;
        private Integer shardSize;

        public String suggester() {
            return suggester;
        }

        public void suggester(String suggester) {
            this.suggester = suggester;
        }

        public BytesRef text() {
            return text;
        }

        public void text(BytesRef text) {
            this.text = text;
        }

        public Analyzer analyzer() {
            return analyzer;
        }

        public void analyzer(Analyzer analyzer) {
            this.analyzer = analyzer;
        }

        public String field() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public SuggestMode suggestMode() {
            return suggestMode;
        }

        public void suggestMode(SuggestMode suggestMode) {
            this.suggestMode = suggestMode;
        }

        public Float accuracy() {
            return accuracy;
        }

        public void accuracy(float accuracy) {
            this.accuracy = accuracy;
        }

        public Integer size() {
            return size;
        }

        public void size(int size) {
            if (size <= 0) {
                throw new ElasticSearchIllegalArgumentException("Size must be positive");
            }

            this.size = size;
        }

        public Suggest.Suggestion.Sort sort() {
            return sort;
        }

        public void sort(Suggest.Suggestion.Sort sort) {
            this.sort = sort;
        }

        public StringDistance stringDistance() {
            return stringDistance;
        }

        public void stringDistance(StringDistance distance) {
            this.stringDistance = distance;
        }

        public Boolean lowerCaseTerms() {
            return lowerCaseTerms;
        }

        public void lowerCaseTerms(boolean lowerCaseTerms) {
            this.lowerCaseTerms = lowerCaseTerms;
        }

        public Integer maxEdits() {
            return maxEdits;
        }

        public void maxEdits(int maxEdits) {
            this.maxEdits = maxEdits;
        }

        public Integer factor() {
            return factor;
        }

        public void factor(int factor) {
            this.factor = factor;
        }

        public Float maxTermFreq() {
            return maxTermFreq;
        }

        public void maxTermFreq(float maxTermFreq) {
            this.maxTermFreq = maxTermFreq;
        }

        public Integer prefixLength() {
            return prefixLength;
        }

        public void prefixLength(int prefixLength) {
            this.prefixLength = prefixLength;
        }

        public Integer minWordLength() {
            return minWordLength;
        }

        public void minQueryLength(int minQueryLength) {
            this.minWordLength = minQueryLength;
        }

        public Float minDocFreq() {
            return minDocFreq;
        }

        public void minDocFreq(float minDocFreq) {
            this.minDocFreq = minDocFreq;
        }

        public Integer shardSize() {
            return shardSize;
        }

        public void shardSize(Integer shardSize) {
            this.shardSize = shardSize;
        }
    }

}
