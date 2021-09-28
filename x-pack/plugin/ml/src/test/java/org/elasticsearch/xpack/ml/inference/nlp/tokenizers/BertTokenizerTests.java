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
import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.hasSize;

public class BertTokenizerTests extends ESTestCase {

    public void testTokenize() {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun"),
            new BertTokenization(null, false, null)
        ).build();

        TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun");
        assertThat(tokenization.getTokens(), arrayContaining("Elastic", "##search", "fun"));
        assertArrayEquals(new int[] {0, 1, 2}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1}, tokenization.getTokenMap());
    }

    public void testTokenizeAppendSpecialTokens() {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList( "elastic", "##search", "fun", BertTokenizer.CLASS_TOKEN, BertTokenizer.SEPARATOR_TOKEN),
            Tokenization.createDefault()
        ).build();

        TokenizationResult.Tokenization tokenization = tokenizer.tokenize("elasticsearch fun");
        assertThat(tokenization.getTokens(), arrayContaining("[CLS]", "elastic", "##search", "fun", "[SEP]"));
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

        TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch " + specialToken + " fun");
        assertThat(tokenization.getTokens(), arrayContaining("Elastic", "##search", specialToken, "fun"));
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

            TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun");
            assertThat(tokenization.getTokens(), arrayContaining(BertTokenizer.UNKNOWN_TOKEN, "fun"));
            assertArrayEquals(new int[] {3, 2}, tokenization.getTokenIds());
            assertArrayEquals(new int[] {0, 1}, tokenization.getTokenMap());

            tokenization = tokenizer.tokenize("elasticsearch fun");
            assertThat(tokenization.getTokens(), arrayContaining("elastic", "##search", "fun"));
        }

        {
            BertTokenizer tokenizer = BertTokenizer.builder(Arrays.asList("elastic", "##search", "fun"), Tokenization.createDefault())
                .setDoLowerCase(true)
                .setWithSpecialTokens(false)
                .build();

            TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun");
            assertThat(tokenization.getTokens(), arrayContaining("elastic", "##search", "fun"));
        }
    }

    public void testPunctuation() {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun", ".", ",", BertTokenizer.MASK_TOKEN, BertTokenizer.UNKNOWN_TOKEN),
            Tokenization.createDefault()
        ).setWithSpecialTokens(false).build();

        TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch, fun.");
        assertThat(tokenization.getTokens(), arrayContaining("Elastic", "##search", ",", "fun", "."));
        assertArrayEquals(new int[] {0, 1, 4, 2, 3}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1, 2, 3}, tokenization.getTokenMap());

        tokenization = tokenizer.tokenize("Elasticsearch, fun [MASK].");
        assertThat(tokenization.getTokens(), arrayContaining("Elastic", "##search", ",", "fun", "[MASK]", "."));
        assertArrayEquals(new int[] {0, 1, 4, 2, 5, 3}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1, 2, 3, 4}, tokenization.getTokenMap());
    }

    public void testBatchInput() {
        BertTokenizer tokenizer = BertTokenizer.builder(
            Arrays.asList("Elastic", "##search", "fun",
                "Pancake", "day",
                "my", "little", "red", "car",
                "God", "##zilla"
                ),
            new BertTokenization(null, false, null)
        ).build();

        TokenizationResult tr = tokenizer.buildTokenizationResult(
            List.of(
                tokenizer.tokenize("Elasticsearch"),
                tokenizer.tokenize("my little red car"),
                tokenizer.tokenize("Godzilla day"),
                tokenizer.tokenize("Godzilla Pancake red car day")
            )
        );
        assertThat(tr.getTokenizations(), hasSize(4));

        TokenizationResult.Tokenization tokenization = tr.getTokenizations().get(0);
        assertThat(tokenization.getTokens(), arrayContaining("Elastic", "##search"));
        assertArrayEquals(new int[] {0, 1}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0}, tokenization.getTokenMap());

        tokenization = tr.getTokenizations().get(1);
        assertThat(tokenization.getTokens(), arrayContaining("my", "little", "red", "car"));
        assertArrayEquals(new int[] {5, 6, 7, 8}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 1, 2, 3}, tokenization.getTokenMap());

        tokenization = tr.getTokenizations().get(2);
        assertThat(tokenization.getTokens(), arrayContaining("God", "##zilla", "day"));
        assertArrayEquals(new int[] {9, 10, 4}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1}, tokenization.getTokenMap());

        tokenization = tr.getTokenizations().get(3);
        assertThat(tokenization.getTokens(), arrayContaining("God", "##zilla", "Pancake", "red", "car", "day"));
        assertArrayEquals(new int[] {9, 10, 3, 7, 8, 4}, tokenization.getTokenIds());
        assertArrayEquals(new int[] {0, 0, 1, 2, 3, 4}, tokenization.getTokenMap());
    }

    public void testMultiSeqTokenization() {
        List<String> vocab = List.of(
            "Elastic",
            "##search",
            "is",
            "fun",
            "my",
            "little",
            "red",
            "car",
            "God",
            "##zilla",
            BertTokenizer.CLASS_TOKEN,
            BertTokenizer.SEPARATOR_TOKEN
        );
        BertTokenizer tokenizer = BertTokenizer.builder(vocab, Tokenization.createDefault())
            .setDoLowerCase(false)
            .setWithSpecialTokens(true)
            .build();
        TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch is fun", "Godzilla my little red car");
        assertThat(
            tokenization.getTokens(),
            arrayContaining(
                BertTokenizer.CLASS_TOKEN,
                "Elastic",
                "##search",
                "is",
                "fun",
                BertTokenizer.SEPARATOR_TOKEN,
                "God",
                "##zilla",
                "my",
                "little",
                "red",
                "car",
                BertTokenizer.SEPARATOR_TOKEN
            )
        );
        assertArrayEquals(new int[] { 10, 0, 1, 2, 3, 11, 8, 9, 4, 5, 6, 7, 11 }, tokenization.getTokenIds());
    }

    public void testMultiSeqRequiresSpecialTokens() {
        BertTokenizer tokenizer = BertTokenizer.builder(List.of("foo"), Tokenization.createDefault())
            .setDoLowerCase(false)
            .setWithSpecialTokens(false)
            .build();
        expectThrows(Exception.class, () -> tokenizer.tokenize("foo", "foo"));
    }
}
