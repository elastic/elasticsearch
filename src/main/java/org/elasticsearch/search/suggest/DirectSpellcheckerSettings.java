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
package org.elasticsearch.search.suggest;

import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.util.automaton.LevenshteinAutomata;

public class DirectSpellcheckerSettings  {

    private SuggestMode suggestMode = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
    private float accuracy = 0.5f;
    private Suggest.Suggestion.Sort sort = Suggest.Suggestion.Sort.SCORE;
    private StringDistance stringDistance = DirectSpellChecker.INTERNAL_LEVENSHTEIN;
    private int maxEdits = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;
    private int maxInspections = 5;
    private float maxTermFreq = 0.01f;
    private int prefixLength = 1;
    private int minWordLength = 4;
    private float minDocFreq = 0f;

    public SuggestMode suggestMode() {
        return suggestMode;
    }

    public void suggestMode(SuggestMode suggestMode) {
        this.suggestMode = suggestMode;
    }

    public float accuracy() {
        return accuracy;
    }

    public void accuracy(float accuracy) {
        this.accuracy = accuracy;
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

    public int maxEdits() {
        return maxEdits;
    }

    public void maxEdits(int maxEdits) {
        this.maxEdits = maxEdits;
    }

    public int maxInspections() {
        return maxInspections;
    }

    public void maxInspections(int maxInspections) {
        this.maxInspections = maxInspections;
    }

    public float maxTermFreq() {
        return maxTermFreq;
    }

    public void maxTermFreq(float maxTermFreq) {
        this.maxTermFreq = maxTermFreq;
    }

    public int prefixLength() {
        return prefixLength;
    }

    public void prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
    }

    public int minWordLength() {
        return minWordLength;
    }

    public void minQueryLength(int minQueryLength) {
        this.minWordLength = minQueryLength;
    }

    public float minDocFreq() {
        return minDocFreq;
    }

    public void minDocFreq(float minDocFreq) {
        this.minDocFreq = minDocFreq;
    }

}