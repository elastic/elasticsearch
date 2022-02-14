/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class BertTokenizerTests extends ESTestCase {

    public static final List<String> TEST_CASED_VOCAB = List.of(
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
        ".",
        ",",
        BertTokenizer.CLASS_TOKEN,
        BertTokenizer.SEPARATOR_TOKEN,
        BertTokenizer.MASK_TOKEN,
        BertTokenizer.UNKNOWN_TOKEN,
        "day",
        "Pancake",
        "with",
        BertTokenizer.PAD_TOKEN
    );

    private List<String> tokenStrings(List<WordPieceTokenFilter.WordPieceToken> tokens) {
        return tokens.stream().map(WordPieceTokenFilter.WordPieceToken::toString).collect(Collectors.toList());
    }

    public void testTokenize() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, null, Tokenization.Truncate.NONE)
            ).build()
        ) {
            TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE);
            assertThat(tokenStrings(tokenization.getTokens()), contains("Elastic", "##search", "fun"));
            assertArrayEquals(new int[] { 0, 1, 3 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.getTokenMap());
        }
    }

    public void testTokenizeLargeInputNoTruncation() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, 5, Tokenization.Truncate.NONE)
            ).build();
            BertTokenizer specialCharTokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, true, 5, Tokenization.Truncate.NONE)
            ).build()
        ) {

            ElasticsearchStatusException ex = expectThrows(
                ElasticsearchStatusException.class,
                () -> tokenizer.tokenize("Elasticsearch fun with Pancake and Godzilla", Tokenization.Truncate.NONE)
            );
            assertThat(ex.getMessage(), equalTo("Input too large. The tokenized input length [8] exceeds the maximum sequence length [5]"));

            // Shouldn't throw
            tokenizer.tokenize("Elasticsearch fun with Pancake", Tokenization.Truncate.NONE);

            // Should throw as special chars add two tokens
            expectThrows(
                ElasticsearchStatusException.class,
                () -> specialCharTokenizer.tokenize("Elasticsearch fun with Pancake", Tokenization.Truncate.NONE)
            );
        }

    }

    public void testTokenizeLargeInputTruncation() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, 5, Tokenization.Truncate.FIRST)
            ).build()
        ) {

            TokenizationResult.Tokenization tokenization = tokenizer.tokenize(
                "Elasticsearch fun with Pancake and Godzilla",
                Tokenization.Truncate.FIRST
            );
            assertArrayEquals(new int[] { 0, 1, 3, 18, 17 }, tokenization.getTokenIds());
        }

        try (
            BertTokenizer tokenizerWithSpecialTokens = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, true, 5, Tokenization.Truncate.FIRST)
            ).build()
        ) {
            var tokenization = tokenizerWithSpecialTokens.tokenize(
                "Elasticsearch fun with Pancake and Godzilla",
                Tokenization.Truncate.FIRST
            );
            assertArrayEquals(new int[] { 12, 0, 1, 3, 13 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { -1, 0, 0, 1, -1 }, tokenization.getTokenMap());
        }
    }

    public void testTokenizeAppendSpecialTokens() {
        try (BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault()).build()) {
            TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE);
            assertArrayEquals(new int[] { 12, 0, 1, 3, 13 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { -1, 0, 0, 1, -1 }, tokenization.getTokenMap());
        }
    }

    public void testNeverSplitTokens() {
        final String specialToken = "SP001";

        try (
            BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault())
                .setNeverSplit(Collections.singleton(specialToken))
                .setWithSpecialTokens(false)
                .build()
        ) {

            TokenizationResult.Tokenization tokenization = tokenizer.tokenize(
                "Elasticsearch " + specialToken + " fun",
                Tokenization.Truncate.NONE
            );
            assertThat(tokenStrings(tokenization.getTokens()), contains("Elastic", "##search", specialToken, "fun"));
            assertArrayEquals(new int[] { 0, 1, 15, 3 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 0, 1, 2 }, tokenization.getTokenMap());
        }
    }

    public void testDoLowerCase() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                Arrays.asList("elastic", "##search", "fun", BertTokenizer.UNKNOWN_TOKEN, BertTokenizer.PAD_TOKEN),
                Tokenization.createDefault()
            ).setDoLowerCase(false).setWithSpecialTokens(false).build()
        ) {

            TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE);
            assertArrayEquals(new int[] { 3, 2 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 1 }, tokenization.getTokenMap());

            tokenization = tokenizer.tokenize("elasticsearch fun", Tokenization.Truncate.NONE);
            assertArrayEquals(new int[] { 0, 1, 2 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.getTokenMap());
        }

        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                Arrays.asList("elastic", "##search", "fun", BertTokenizer.UNKNOWN_TOKEN, BertTokenizer.PAD_TOKEN),
                Tokenization.createDefault()
            ).setDoLowerCase(true).setWithSpecialTokens(false).build()
        ) {

            TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE);
            assertArrayEquals(new int[] { 0, 1, 2 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.getTokenMap());
        }
    }

    public void testPunctuation() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault())
                .setWithSpecialTokens(false)
                .build()
        ) {
            TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch, fun.", Tokenization.Truncate.NONE);
            assertThat(tokenStrings(tokenization.getTokens()), contains("Elastic", "##search", ",", "fun", "."));
            assertArrayEquals(new int[] { 0, 1, 11, 3, 10 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 0, 1, 2, 3 }, tokenization.getTokenMap());

            tokenization = tokenizer.tokenize("Elasticsearch, fun [MASK].", Tokenization.Truncate.NONE);
            assertArrayEquals(new int[] { 0, 1, 11, 3, 14, 10 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 0, 1, 2, 3, 4 }, tokenization.getTokenMap());
        }
    }

    public void testPunctuationWithMask() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                List.of(
                    "[CLS]",
                    "This",
                    "is",
                    "[MASK]",
                    "-",
                    "~",
                    "ta",
                    "##stic",
                    "!",
                    "[SEP]",
                    "sub",
                    ",",
                    ".",
                    BertTokenizer.UNKNOWN_TOKEN,
                    BertTokenizer.PAD_TOKEN
                ),
                Tokenization.createDefault()
            ).setWithSpecialTokens(true).setNeverSplit(Set.of("[MASK]")).build()
        ) {

            TokenizationResult.Tokenization tokenization = tokenizer.tokenize("This is [MASK]-tastic!", Tokenization.Truncate.NONE);
            assertThat(tokenStrings(tokenization.getTokens()), contains("This", "is", "[MASK]", "-", "ta", "##stic", "!"));
            assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 6, 7, 8, 9 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { -1, 0, 1, 2, 3, 4, 4, 5, -1 }, tokenization.getTokenMap());

            tokenization = tokenizer.tokenize("This is sub~[MASK]!", Tokenization.Truncate.NONE);
            assertThat(tokenStrings(tokenization.getTokens()), contains("This", "is", "sub", "~", "[MASK]", "!"));
            assertArrayEquals(new int[] { 0, 1, 2, 10, 5, 3, 8, 9 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { -1, 0, 1, 2, 3, 4, 5, -1 }, tokenization.getTokenMap());

            tokenization = tokenizer.tokenize("This is sub,[MASK].tastic!", Tokenization.Truncate.NONE);
            assertThat(tokenStrings(tokenization.getTokens()), contains("This", "is", "sub", ",", "[MASK]", ".", "ta", "##stic", "!"));
            assertArrayEquals(new int[] { 0, 1, 2, 10, 11, 3, 12, 6, 7, 8, 9 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { -1, 0, 1, 2, 3, 4, 5, 6, 6, 7, -1 }, tokenization.getTokenMap());
        }
    }

    public void testBatchInput() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, null, Tokenization.Truncate.NONE)
            ).build()
        ) {

            TokenizationResult tr = tokenizer.buildTokenizationResult(
                List.of(
                    tokenizer.tokenize("Elasticsearch", Tokenization.Truncate.NONE),
                    tokenizer.tokenize("my little red car", Tokenization.Truncate.NONE),
                    tokenizer.tokenize("Godzilla day", Tokenization.Truncate.NONE),
                    tokenizer.tokenize("Godzilla Pancake red car day", Tokenization.Truncate.NONE)
                )
            );
            assertThat(tr.getTokenizations(), hasSize(4));

            TokenizationResult.Tokenization tokenization = tr.getTokenizations().get(0);
            assertArrayEquals(new int[] { 0, 1 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 0 }, tokenization.getTokenMap());

            tokenization = tr.getTokenizations().get(1);
            assertArrayEquals(new int[] { 4, 5, 6, 7 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 1, 2, 3 }, tokenization.getTokenMap());

            tokenization = tr.getTokenizations().get(2);
            assertArrayEquals(new int[] { 8, 9, 16 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.getTokenMap());

            tokenization = tr.getTokenizations().get(3);
            assertArrayEquals(new int[] { 8, 9, 17, 6, 7, 16 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 0, 1, 2, 3, 4 }, tokenization.getTokenMap());
        }
    }

    public void testMultiSeqTokenization() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault())
                .setDoLowerCase(false)
                .setWithSpecialTokens(true)
                .build()
        ) {
            TokenizationResult.Tokenization tokenization = tokenizer.tokenize(
                "Elasticsearch is fun",
                "Godzilla my little red car",
                Tokenization.Truncate.NONE
            );

            var tokenStream = Arrays.stream(tokenization.getTokenIds()).mapToObj(TEST_CASED_VOCAB::get).collect(Collectors.toList());
            assertThat(
                tokenStream,
                contains(
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
            assertArrayEquals(new int[] { 12, 0, 1, 2, 3, 13, 8, 9, 4, 5, 6, 7, 13 }, tokenization.getTokenIds());
        }
    }

    public void testTokenizeLargeInputMultiSequenceTruncation() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, true, 10, Tokenization.Truncate.FIRST)
            ).build()
        ) {

            TokenizationResult.Tokenization tokenization = tokenizer.tokenize(
                "Elasticsearch is fun",
                "Godzilla my little red car",
                Tokenization.Truncate.FIRST
            );

            var tokenStream = Arrays.stream(tokenization.getTokenIds()).mapToObj(TEST_CASED_VOCAB::get).collect(Collectors.toList());
            assertThat(
                tokenStream,
                contains(
                    BertTokenizer.CLASS_TOKEN,
                    "Elastic",
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

            expectThrows(
                ElasticsearchStatusException.class,
                () -> BertTokenizer.builder(TEST_CASED_VOCAB, new BertTokenization(null, true, 8, Tokenization.Truncate.NONE))
                    .build()
                    .tokenize("Elasticsearch is fun", "Godzilla my little red car", Tokenization.Truncate.NONE)
            );
        }

        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, true, 10, Tokenization.Truncate.SECOND)
            ).build()
        ) {

            TokenizationResult.Tokenization tokenization = tokenizer.tokenize(
                "Elasticsearch is fun",
                "Godzilla my little red car",
                Tokenization.Truncate.SECOND
            );
            var tokenStream = Arrays.stream(tokenization.getTokenIds()).mapToObj(TEST_CASED_VOCAB::get).collect(Collectors.toList());
            assertThat(
                tokenStream,
                contains(
                    BertTokenizer.CLASS_TOKEN,
                    "Elastic",
                    "##search",
                    "is",
                    "fun",
                    BertTokenizer.SEPARATOR_TOKEN,
                    "God",
                    "##zilla",
                    "my",
                    BertTokenizer.SEPARATOR_TOKEN
                )
            );
        }

    }

    public void testMultiSeqRequiresSpecialTokens() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                List.of(
                    "foo",
                    BertTokenizer.UNKNOWN_TOKEN,
                    BertTokenizer.PAD_TOKEN,
                    BertTokenizer.CLASS_TOKEN,
                    BertTokenizer.SEPARATOR_TOKEN
                ),
                Tokenization.createDefault()
            ).setDoLowerCase(false).setWithSpecialTokens(false).build()
        ) {
            expectThrows(Exception.class, () -> tokenizer.tokenize("foo", "foo", Tokenization.Truncate.NONE));
        }
    }
}
