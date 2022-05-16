/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest;

import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.SuggestWordFrequencyComparator;
import org.apache.lucene.search.spell.SuggestWordQueue;
import org.apache.lucene.util.automaton.LevenshteinAutomata;

import java.util.Comparator;

public class DirectSpellcheckerSettings {

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

    private static final Comparator<SuggestWord> LUCENE_FREQUENCY = new SuggestWordFrequencyComparator();
    private static final Comparator<SuggestWord> SCORE_COMPARATOR = SuggestWordQueue.DEFAULT_COMPARATOR;

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

    public DirectSpellChecker createDirectSpellChecker() {

        DirectSpellChecker directSpellChecker = new DirectSpellChecker();
        directSpellChecker.setAccuracy(accuracy());
        Comparator<SuggestWord> comparator = switch (sort()) {
            case SCORE -> SCORE_COMPARATOR;
            case FREQUENCY -> LUCENE_FREQUENCY;
        };
        directSpellChecker.setComparator(comparator);
        directSpellChecker.setDistance(stringDistance());
        directSpellChecker.setMaxEdits(maxEdits());
        directSpellChecker.setMaxInspections(maxInspections());
        directSpellChecker.setMaxQueryFrequency(maxTermFreq());
        directSpellChecker.setMinPrefix(prefixLength());
        directSpellChecker.setMinQueryLength(minWordLength());
        directSpellChecker.setThresholdFrequency(minDocFreq());
        directSpellChecker.setLowerCaseTerms(false);
        return directSpellChecker;
    }

    @Override
    public String toString() {
        return "["
            + "suggestMode="
            + suggestMode
            + ",sort="
            + sort
            + ",stringDistance="
            + stringDistance
            + ",accuracy="
            + accuracy
            + ",maxEdits="
            + maxEdits
            + ",maxInspections="
            + maxInspections
            + ",maxTermFreq="
            + maxTermFreq
            + ",prefixLength="
            + prefixLength
            + ",minWordLength="
            + minWordLength
            + ",minDocFreq="
            + minDocFreq
            + "]";
    }

}
