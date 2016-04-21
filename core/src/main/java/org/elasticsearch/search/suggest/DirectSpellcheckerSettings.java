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

    // NB: If this changes, make sure to change the default in TermBuilderSuggester
    public static SuggestMode DEFAULT_SUGGEST_MODE = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;
    public static float DEFAULT_ACCURACY = 0.5f;
    public static SortBy DEFAULT_SORT = SortBy.SCORE;
    // NB: If this changes, make sure to change the default in TermBuilderSuggester
    public static StringDistance DEFAULT_STRING_DISTANCE = DirectSpellChecker.INTERNAL_LEVENSHTEIN;
    public static int DEFAULT_MAX_EDITS = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;
    public static int DEFAULT_MAX_INSPECTIONS = 5;
    public static float DEFAULT_MAX_TERM_FREQ = 0.01f;
    public static int DEFAULT_PREFIX_LENGTH = 1;
    public static int DEFAULT_MIN_WORD_LENGTH = 4;
    public static float DEFAULT_MIN_DOC_FREQ = 0f;

    private SuggestMode suggestMode = DEFAULT_SUGGEST_MODE;
    private float accuracy = DEFAULT_ACCURACY;
    private SortBy sort = DEFAULT_SORT;
    private StringDistance stringDistance = DEFAULT_STRING_DISTANCE;
    private int maxEdits = DEFAULT_MAX_EDITS;
    private int maxInspections = DEFAULT_MAX_INSPECTIONS;
    private float maxTermFreq = DEFAULT_MAX_TERM_FREQ;
    private int prefixLength = DEFAULT_PREFIX_LENGTH;
    private int minWordLength = DEFAULT_MIN_WORD_LENGTH;
    private float minDocFreq = DEFAULT_MIN_DOC_FREQ;

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

    public SortBy sort() {
        return sort;
    }

    public void sort(SortBy sort) {
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

    public void minWordLength(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    public float minDocFreq() {
        return minDocFreq;
    }

    public void minDocFreq(float minDocFreq) {
        this.minDocFreq = minDocFreq;
    }

    @Override
    public String toString() {
        return "[" +
                   "suggestMode=" + suggestMode +
                   ",sort=" + sort +
                   ",stringDistance=" + stringDistance +
                   ",accuracy=" + accuracy +
                   ",maxEdits=" + maxEdits +
                   ",maxInspections=" + maxInspections +
                   ",maxTermFreq=" + maxTermFreq +
                   ",prefixLength=" + prefixLength +
                   ",minWordLength=" + minWordLength +
                   ",minDocFreq=" + minDocFreq +
               "]";
    }

}
