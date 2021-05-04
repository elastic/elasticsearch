/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pipelines.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.contains;

public class BertTokenizerTests extends ESTestCase {

    public void testTokenize() {
        BertTokenizer tokenizer = BertTokenizer.builder(
            WordPieceTokenizerTests.createVocabMap("elastic", "##search", "fun"))
            .build();

        BertTokenizer.TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch fun");
        assertThat(tokenization.getTokens(), contains("elastic", "##search", "fun"));
        assertArrayEquals(new int[] {0, 1, 2}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1}, tokenization.getTokenMap());
    }

    public void testNeverSplitTokens() {
        final String specialToken = "SP001";

        BertTokenizer tokenizer = BertTokenizer.builder(
            WordPieceTokenizerTests.createVocabMap("elastic", "##search", "fun", specialToken, BertTokenizer.UNKNOWN_TOKEN))
            .setNeverSplit(Collections.singleton(specialToken))
            .build();

        BertTokenizer.TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch " + specialToken + " fun");
        assertThat(tokenization.getTokens(), contains("elastic", "##search", specialToken, "fun"));
        assertArrayEquals(new int[] {0, 1, 3, 2}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1, 2}, tokenization.getTokenMap());
    }

    public void testDoLowerCase() {
        {
            BertTokenizer tokenizer = BertTokenizer.builder(
                WordPieceTokenizerTests.createVocabMap("elastic", "##search", "fun", BertTokenizer.UNKNOWN_TOKEN))
                .setDoLowerCase(false)
                .build();

            BertTokenizer.TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch fun");
            assertThat(tokenization.getTokens(), contains(BertTokenizer.UNKNOWN_TOKEN, "fun"));
            assertArrayEquals(new int[] {3, 2}, tokenization.getTokenIds());
            assertArrayEquals(new int[] {0, 1}, tokenization.getTokenMap());

            tokenization = tokenizer.tokenize("elasticsearch fun");
            assertThat(tokenization.getTokens(), contains("elastic", "##search", "fun"));
        }

        {
            BertTokenizer tokenizer = BertTokenizer.builder(
                WordPieceTokenizerTests.createVocabMap("elastic", "##search", "fun"))
                .setDoLowerCase(true)
                .build();

            BertTokenizer.TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch fun");
            assertThat(tokenization.getTokens(), contains("elastic", "##search", "fun"));
        }
    }
}
