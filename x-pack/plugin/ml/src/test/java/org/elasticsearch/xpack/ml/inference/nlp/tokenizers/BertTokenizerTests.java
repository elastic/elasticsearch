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

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class BertTokenizerTests extends ESTestCase {

    private static final List<String> TEST_CASED_VOCAB = List.of(
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
        "with"
    );

    public void testTokenize() {
        BertTokenizer tokenizer = BertTokenizer.builder(
            TEST_CASED_VOCAB,
            new BertTokenization(null, false, null, Tokenization.Truncate.NONE)
        ).build();

        TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE);
        assertThat(tokenization.getTokens(), arrayContaining("Elastic", "##search", "fun"));
        assertArrayEquals(new int[] { 0, 1, 3 }, tokenization.getTokenIds());
        assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.getTokenMap());
    }

    public void testTokenizeLargeInputNoTruncation() {
        BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, new BertTokenization(null, false, 5, Tokenization.Truncate.NONE))
            .build();

        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> tokenizer.tokenize("Elasticsearch fun with Pancake and Godzilla", Tokenization.Truncate.NONE)
        );
        assertThat(ex.getMessage(), equalTo("Input too large. The tokenized input length [8] exceeds the maximum sequence length [5]"));

        BertTokenizer specialCharTokenizer = BertTokenizer.builder(
            TEST_CASED_VOCAB,
            new BertTokenization(null, true, 5, Tokenization.Truncate.NONE)
        ).build();

        // Shouldn't throw
        tokenizer.tokenize("Elasticsearch fun with Pancake", Tokenization.Truncate.NONE);

        // Should throw as special chars add two tokens
        expectThrows(
            ElasticsearchStatusException.class,
            () -> specialCharTokenizer.tokenize("Elasticsearch fun with Pancake", Tokenization.Truncate.NONE)
        );
    }

    public void testTokenizeLargeInputTruncation() {
        BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, new BertTokenization(null, false, 5, Tokenization.Truncate.FIRST))
            .build();

        TokenizationResult.Tokenization tokenization = tokenizer.tokenize(
            "Elasticsearch fun with Pancake and Godzilla",
            Tokenization.Truncate.FIRST
        );
        assertThat(tokenization.getTokens(), arrayContaining("Elastic", "##search", "fun", "with", "Pancake"));

        tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, new BertTokenization(null, true, 5, Tokenization.Truncate.FIRST)).build();
        tokenization = tokenizer.tokenize("Elasticsearch fun with Pancake and Godzilla", Tokenization.Truncate.FIRST);
        assertThat(tokenization.getTokens(), arrayContaining("[CLS]", "Elastic", "##search", "fun", "[SEP]"));
    }

    public void testTokenizeAppendSpecialTokens() {
        BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault()).build();

        TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE);
        assertThat(tokenization.getTokens(), arrayContaining("[CLS]", "Elastic", "##search", "fun", "[SEP]"));
        assertArrayEquals(new int[] { 12, 0, 1, 3, 13 }, tokenization.getTokenIds());
        assertArrayEquals(new int[] { -1, 0, 0, 1, -1 }, tokenization.getTokenMap());
    }

    public void testNeverSplitTokens() {
        final String specialToken = "SP001";

        BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault())
            .setNeverSplit(Collections.singleton(specialToken))
            .setWithSpecialTokens(false)
            .build();

        TokenizationResult.Tokenization tokenization = tokenizer.tokenize(
            "Elasticsearch " + specialToken + " fun",
            Tokenization.Truncate.NONE
        );
        assertThat(tokenization.getTokens(), arrayContaining("Elastic", "##search", specialToken, "fun"));
        assertArrayEquals(new int[] { 0, 1, 15, 3 }, tokenization.getTokenIds());
        assertArrayEquals(new int[] { 0, 0, 1, 2 }, tokenization.getTokenMap());
    }

    public void testDoLowerCase() {
        {
            BertTokenizer tokenizer = BertTokenizer.builder(
                Arrays.asList("elastic", "##search", "fun", BertTokenizer.UNKNOWN_TOKEN),
                Tokenization.createDefault()
            ).setDoLowerCase(false).setWithSpecialTokens(false).build();

            TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE);
            assertThat(tokenization.getTokens(), arrayContaining(BertTokenizer.UNKNOWN_TOKEN, "fun"));
            assertArrayEquals(new int[] { 3, 2 }, tokenization.getTokenIds());
            assertArrayEquals(new int[] { 0, 1 }, tokenization.getTokenMap());

            tokenization = tokenizer.tokenize("elasticsearch fun", Tokenization.Truncate.NONE);
            assertThat(tokenization.getTokens(), arrayContaining("elastic", "##search", "fun"));
        }

        {
            BertTokenizer tokenizer = BertTokenizer.builder(Arrays.asList("elastic", "##search", "fun"), Tokenization.createDefault())
                .setDoLowerCase(true)
                .setWithSpecialTokens(false)
                .build();

            TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE);
            assertThat(tokenization.getTokens(), arrayContaining("elastic", "##search", "fun"));
        }
    }

    public void testPunctuation() {
        BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault()).setWithSpecialTokens(false).build();

        TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch, fun.", Tokenization.Truncate.NONE);
        assertThat(tokenization.getTokens(), arrayContaining("Elastic", "##search", ",", "fun", "."));
        assertArrayEquals(new int[] { 0, 1, 11, 3, 10 }, tokenization.getTokenIds());
        assertArrayEquals(new int[] { 0, 0, 1, 2, 3 }, tokenization.getTokenMap());

        tokenization = tokenizer.tokenize("Elasticsearch, fun [MASK].", Tokenization.Truncate.NONE);
        assertThat(tokenization.getTokens(), arrayContaining("Elastic", "##search", ",", "fun", "[MASK]", "."));
        assertArrayEquals(new int[] { 0, 1, 11, 3, 14, 10 }, tokenization.getTokenIds());
        assertArrayEquals(new int[] { 0, 0, 1, 2, 3, 4 }, tokenization.getTokenMap());
    }

    public void testBatchInput() {
        BertTokenizer tokenizer = BertTokenizer.builder(
            TEST_CASED_VOCAB,
            new BertTokenization(null, false, null, Tokenization.Truncate.NONE)
        ).build();

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
        assertThat(tokenization.getTokens(), arrayContaining("Elastic", "##search"));
        assertArrayEquals(new int[] { 0, 1 }, tokenization.getTokenIds());
        assertArrayEquals(new int[] { 0, 0 }, tokenization.getTokenMap());

        tokenization = tr.getTokenizations().get(1);
        assertThat(tokenization.getTokens(), arrayContaining("my", "little", "red", "car"));
        assertArrayEquals(new int[] { 4, 5, 6, 7 }, tokenization.getTokenIds());
        assertArrayEquals(new int[] { 0, 1, 2, 3 }, tokenization.getTokenMap());

        tokenization = tr.getTokenizations().get(2);
        assertThat(tokenization.getTokens(), arrayContaining("God", "##zilla", "day"));
        assertArrayEquals(new int[] { 8, 9, 16 }, tokenization.getTokenIds());
        assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.getTokenMap());

        tokenization = tr.getTokenizations().get(3);
        assertThat(tokenization.getTokens(), arrayContaining("God", "##zilla", "Pancake", "red", "car", "day"));
        assertArrayEquals(new int[] { 8, 9, 17, 6, 7, 16 }, tokenization.getTokenIds());
        assertArrayEquals(new int[] { 0, 0, 1, 2, 3, 4 }, tokenization.getTokenMap());
    }

    public void testMultiSeqTokenization() {
        BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault())
            .setDoLowerCase(false)
            .setWithSpecialTokens(true)
            .build();
        TokenizationResult.Tokenization tokenization = tokenizer.tokenize(
            "Elasticsearch is fun",
            "Godzilla my little red car",
            Tokenization.Truncate.NONE
        );
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
        assertArrayEquals(new int[] { 12, 0, 1, 2, 3, 13, 8, 9, 4, 5, 6, 7, 13 }, tokenization.getTokenIds());
    }

    public void testTokenizeLargeInputMultiSequenceTruncation() {
        BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, new BertTokenization(null, true, 10, Tokenization.Truncate.FIRST))
            .build();

        TokenizationResult.Tokenization tokenization = tokenizer.tokenize(
            "Elasticsearch is fun",
            "Godzilla my little red car",
            Tokenization.Truncate.FIRST
        );
        assertThat(
            tokenization.getTokens(),
            arrayContaining(
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

        tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, new BertTokenization(null, true, 10, Tokenization.Truncate.SECOND)).build();

        tokenization = tokenizer.tokenize("Elasticsearch is fun", "Godzilla my little red car", Tokenization.Truncate.SECOND);
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
                BertTokenizer.SEPARATOR_TOKEN
            )
        );

    }

    public void testMultiSeqRequiresSpecialTokens() {
        BertTokenizer tokenizer = BertTokenizer.builder(List.of("foo"), Tokenization.createDefault())
            .setDoLowerCase(false)
            .setWithSpecialTokens(false)
            .build();
        expectThrows(Exception.class, () -> tokenizer.tokenize("foo", "foo", Tokenization.Truncate.NONE));
    }
}
