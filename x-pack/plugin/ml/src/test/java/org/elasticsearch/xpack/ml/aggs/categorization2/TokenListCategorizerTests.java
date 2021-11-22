/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization2;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.Matchers.equalTo;

public class TokenListCategorizerTests extends CategorizationTestCase {

    public void testMinMatchingWeights() {
        assertThat(TokenListCategorizer.minMatchingWeight(0, 0.7f), equalTo(0));
        assertThat(TokenListCategorizer.minMatchingWeight(1, 0.7f), equalTo(1));
        assertThat(TokenListCategorizer.minMatchingWeight(2, 0.7f), equalTo(2));
        assertThat(TokenListCategorizer.minMatchingWeight(3, 0.7f), equalTo(3));
        assertThat(TokenListCategorizer.minMatchingWeight(4, 0.7f), equalTo(3));
        assertThat(TokenListCategorizer.minMatchingWeight(5, 0.7f), equalTo(4));
        assertThat(TokenListCategorizer.minMatchingWeight(6, 0.7f), equalTo(5));
        assertThat(TokenListCategorizer.minMatchingWeight(7, 0.7f), equalTo(5));
        assertThat(TokenListCategorizer.minMatchingWeight(8, 0.7f), equalTo(6));
        assertThat(TokenListCategorizer.minMatchingWeight(9, 0.7f), equalTo(7));
        assertThat(TokenListCategorizer.minMatchingWeight(10, 0.7f), equalTo(8));
    }

    public void testMaxMatchingWeights() {
        assertThat(TokenListCategorizer.maxMatchingWeight(0, 0.7f), equalTo(0));
        assertThat(TokenListCategorizer.maxMatchingWeight(1, 0.7f), equalTo(1));
        assertThat(TokenListCategorizer.maxMatchingWeight(2, 0.7f), equalTo(2));
        assertThat(TokenListCategorizer.maxMatchingWeight(3, 0.7f), equalTo(4));
        assertThat(TokenListCategorizer.maxMatchingWeight(4, 0.7f), equalTo(5));
        assertThat(TokenListCategorizer.maxMatchingWeight(5, 0.7f), equalTo(7));
        assertThat(TokenListCategorizer.maxMatchingWeight(6, 0.7f), equalTo(8));
        assertThat(TokenListCategorizer.maxMatchingWeight(7, 0.7f), equalTo(9));
        assertThat(TokenListCategorizer.maxMatchingWeight(8, 0.7f), equalTo(11));
        assertThat(TokenListCategorizer.maxMatchingWeight(9, 0.7f), equalTo(12));
        assertThat(TokenListCategorizer.maxMatchingWeight(10, 0.7f), equalTo(14));
    }

    public void testWeightCalculator() throws IOException {

        CategorizationPartOfSpeechDictionary partOfSpeechDictionary;
        try (
            InputStream is = CategorizationPartOfSpeechDictionary.class.getResourceAsStream(CategorizeTextAggregator.DICTIONARY_FILE_PATH)
        ) {
            partOfSpeechDictionary = new CategorizationPartOfSpeechDictionary(is);
        }

        TokenListCategorizer.WeightCalculator weightCalculator = new TokenListCategorizer.WeightCalculator(partOfSpeechDictionary);

        assertThat(weightCalculator.calculateWeight("Info"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("my_host"), equalTo(1)); // not dictionary word
        assertThat(weightCalculator.calculateWeight("web"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("service"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("starting"), equalTo(12)); // dictionary word, verb, 3rd in a row
        assertThat(weightCalculator.calculateWeight("user123"), equalTo(1)); // not dictionary word
        assertThat(weightCalculator.calculateWeight("a"), equalTo(1)); // too short for dictionary weighting
        assertThat(weightCalculator.calculateWeight("cool"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("web"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("service"), equalTo(9)); // dictionary word, not verb, 3rd in a row
        assertThat(weightCalculator.calculateWeight("called"), equalTo(9)); // dictionary word, not verb, 4th in a row
        assertThat(weightCalculator.calculateWeight("my_service"), equalTo(1)); // not dictionary word
        assertThat(weightCalculator.calculateWeight("is"), equalTo(3)); // dictionary word, not verb, not 3rd in a row
        assertThat(weightCalculator.calculateWeight("starting"), equalTo(6)); // dictionary word, verb, not 3rd in a row
    }
}
