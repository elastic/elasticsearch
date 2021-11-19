/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization2;

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
}
