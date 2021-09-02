/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.contains;

public class BertTokenizerTests extends ESTestCase {

    public void testTokenize() {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun"),
            new BertTokenization(null, false, null)
        ).build();

        TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch fun");
        assertThat(tokenization.getTokens(), contains("Elastic", "##search", "fun"));
        assertArrayEquals(new int[] {0, 1, 2}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1}, tokenization.getTokenMap());
    }

    public void testTokenizeAppendSpecialTokens() {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList( "elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN),
            Tokenization.createDefault()
        ).build();

        TokenizationResult tokenization = tokenizer.tokenize("elasticsearch fun");
        assertThat(tokenization.getTokens(), contains("[CLS]", "elastic", "##search", "fun", "[SEP]"));
        assertArrayEquals(new int[] {3, 0, 1, 2, 4}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {-1, 0, 0, 1, -1}, tokenization.getTokenMap());
    }

    public void testNeverSplitTokens() {
        final String specialToken = "SP001";

        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", specialToken, BertTokenizer.UNKNOWN_TOKEN),
            Tokenization.createDefault()
        ).setNeverSplit(Collections.singleton(specialToken))
         .setWithSpecialTokens(false)
         .build();

        TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch " + specialToken + " fun");
        assertThat(tokenization.getTokens(), contains("Elastic", "##search", specialToken, "fun"));
        assertArrayEquals(new int[] {0, 1, 3, 2}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1, 2}, tokenization.getTokenMap());
    }

    public void testDoLowerCase() {
        {
            BertTokenizer tokenizer = BertTokenizer.builder(
                Arrays.asList("elastic", "##search", "fun", BertTokenizer.UNKNOWN_TOKEN),
                Tokenization.createDefault()
            ).setDoLowerCase(false)
             .setWithSpecialTokens(false)
             .build();

            TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch fun");
            assertThat(tokenization.getTokens(), contains(BertTokenizer.UNKNOWN_TOKEN, "fun"));
            assertArrayEquals(new int[] {3, 2}, tokenization.getTokenIds());
            assertArrayEquals(new int[] {0, 1}, tokenization.getTokenMap());

            tokenization = tokenizer.tokenize("elasticsearch fun");
            assertThat(tokenization.getTokens(), contains("elastic", "##search", "fun"));
        }

        {
            BertTokenizer tokenizer = BertTokenizer.builder(Arrays.asList("elastic", "##search", "fun"), Tokenization.createDefault())
                .setDoLowerCase(true)
                .setWithSpecialTokens(false)
                .build();

            TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch fun");
            assertThat(tokenization.getTokens(), contains("elastic", "##search", "fun"));
        }
    }

    public void testPunctuation() {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", ".", ",", BertTokenizer.MASK_TOKEN, BertTokenizer.UNKNOWN_TOKEN),
            Tokenization.createDefault()
        ).setWithSpecialTokens(false).build();

        TokenizationResult tokenization = tokenizer.tokenize("Elasticsearch, fun.");
        assertThat(tokenization.getTokens(), contains("Elastic", "##search", ",", "fun", "."));
        assertArrayEquals(new int[] {0, 1, 4, 2, 3}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1, 2, 3}, tokenization.getTokenMap());

        tokenization = tokenizer.tokenize("Elasticsearch, fun [MASK].");
        assertThat(tokenization.getTokens(), contains("Elastic", "##search", ",", "fun", "[MASK]", "."));
        assertArrayEquals(new int[] {0, 1, 4, 2, 5, 3}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1, 2, 3, 4}, tokenization.getTokenMap());
    }
}
