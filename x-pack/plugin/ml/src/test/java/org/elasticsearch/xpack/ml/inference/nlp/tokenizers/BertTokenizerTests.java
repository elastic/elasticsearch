/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

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
        BertTokenizer.PAD_TOKEN,
        "?"
    );

    private List<String> tokenStrings(List<? extends DelimitedToken> tokens) {
        return tokens.stream().map(DelimitedToken::toString).collect(Collectors.toList());
    }

    public void testTokenize() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, null, Tokenization.Truncate.NONE, -1)
            ).build()
        ) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenStrings(tokenization.tokens().get(0)), contains("Elastic", "##search", "fun"));
            assertArrayEquals(new int[] { 0, 1, 3 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.tokenMap());
        }
    }

    public void testTokenizeWithTokensThatAreRemovedByStripAccents() {

        List<String> vocab = List.of(
            "Arabic",
            ":",
            "و, ##ق",
            "3",
            "ا, ##ل, ##ا, ##ز, ##د, ##ه, ##ا, ##م",
            ".",
            "4",
            "There",
            "are",
            "2",
            "main",
            "types",
            "of",
            "non",
            "mel",
            "##ano",
            "##ma",
            "skin",
            "cancer",
            "basel",
            "cell",
            "car",
            "##cino",
            "(",
            ")",
            "BCC",
            "SCC",
            "and",
            "squamous",
            BertTokenizer.CLASS_TOKEN,
            BertTokenizer.SEPARATOR_TOKEN,
            BertTokenizer.MASK_TOKEN,
            BertTokenizer.UNKNOWN_TOKEN,
            BertTokenizer.PAD_TOKEN
        );

        String inputWithAccentsToStrip1 = "  Arabic: وَقْ 3 ُ الِازْدِهَام.4  ";
        String inputWithAccentsToStrip2 =
            "There are 2 main types of non melanoma skin cancer ̶̶ basal cell carcinoma (BCC) and squamous cell carcinoma (SCC).";
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(vocab, new BertTokenization(true, true, null, Tokenization.Truncate.NONE, -1))
                .build()
        ) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize(inputWithAccentsToStrip1, Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenization.tokenIds(), equalTo(new int[] { 29, 0, 1, 32, 3, 32, 5, 6, 30 }));

            tokenization = tokenizer.tokenize(inputWithAccentsToStrip2, Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(
                tokenization.tokenIds(),
                equalTo(
                    new int[] {
                        29,
                        7,
                        8,
                        9,
                        10,
                        11,
                        12,
                        13,
                        14,
                        15,
                        16,
                        17,
                        18,
                        32,
                        20,
                        21,
                        22,
                        16,
                        23,
                        25,
                        24,
                        27,
                        28,
                        20,
                        21,
                        22,
                        16,
                        23,
                        26,
                        24,
                        5,
                        30 }
                )
            );
        }
    }

    public void testTokenizeFailureCaseAccentFilter() {
        List<String> testingVocab = List.of(
            "[CLS]",
            "br",
            "##ᄎ",
            "##ᅡ",
            "##ᆼ",
            "##n",
            "'",
            "s",
            "[SEP]",
            BertTokenizer.MASK_TOKEN,
            BertTokenizer.UNKNOWN_TOKEN,
            BertTokenizer.PAD_TOKEN
        );
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                testingVocab,
                new BertTokenization(true, true, 512, Tokenization.Truncate.FIRST, -1)
            ).build()
        ) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize("Br창n's", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenization.tokenIds(), equalTo(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }));

            tokenization = tokenizer.tokenize("Br창n", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenization.tokenIds(), equalTo(new int[] { 0, 1, 2, 3, 4, 5, 8 }));
        }
    }

    public void testTokenizeLargeInputNoTruncation() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, 5, Tokenization.Truncate.NONE, -1)
            ).build();
            BertTokenizer specialCharTokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, true, 5, Tokenization.Truncate.NONE, -1)
            ).build()
        ) {

            ElasticsearchStatusException ex = expectThrows(
                ElasticsearchStatusException.class,
                () -> tokenizer.tokenize("Elasticsearch fun with Pancake and Godzilla", Tokenization.Truncate.NONE, -1, 0)
            );
            assertThat(ex.getMessage(), equalTo("Input too large. The tokenized input length [8] exceeds the maximum sequence length [5]"));

            // Shouldn't throw
            tokenizer.tokenize("Elasticsearch fun with Pancake", Tokenization.Truncate.NONE, -1, 0);

            // Should throw as special chars add two tokens
            expectThrows(
                ElasticsearchStatusException.class,
                () -> specialCharTokenizer.tokenize("Elasticsearch fun with Pancake", Tokenization.Truncate.NONE, -1, 0)
            );
        }
    }

    public void testTokenizeLargeInputNoTruncationWithWindowing() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, true, 5, Tokenization.Truncate.NONE, 0)
            ).build()
        ) {
            List<TokenizationResult.Tokens> tokens = tokenizer.tokenize(
                "Pancake day fun with Elasticsearch and Godzilla",
                Tokenization.Truncate.NONE,
                0,
                0
            );
            assertThat(tokens.size(), equalTo(3));
            assertArrayEquals(new int[] { 12, 17, 16, 3, 13 }, tokens.get(0).tokenIds());
            assertArrayEquals(new int[] { -1, 0, 1, 2, -1 }, tokens.get(0).tokenMap());

            assertArrayEquals(new int[] { 12, 18, 0, 1, 13 }, tokens.get(1).tokenIds());
            assertArrayEquals(new int[] { -1, 3, 4, 4, -1 }, tokens.get(1).tokenMap());

            assertArrayEquals(new int[] { 12, 15, 8, 9, 13 }, tokens.get(2).tokenIds());
            assertArrayEquals(new int[] { -1, 5, 6, 6, -1 }, tokens.get(2).tokenMap());

            tokens = tokenizer.tokenize("Godzilla Elasticsearch day with Pancake and my little red car.", Tokenization.Truncate.NONE, 0, 0);
            assertThat(tokens.size(), equalTo(5));
            assertArrayEquals(new int[] { 12, 8, 9, 13 }, tokens.get(0).tokenIds());
            assertArrayEquals(new int[] { -1, 0, 0, -1 }, tokens.get(0).tokenMap());

            assertArrayEquals(new int[] { 12, 0, 1, 16, 13 }, tokens.get(1).tokenIds());
            assertArrayEquals(new int[] { -1, 1, 1, 2, -1 }, tokens.get(1).tokenMap());

            assertArrayEquals(new int[] { 12, 18, 17, 15, 13 }, tokens.get(2).tokenIds());
            assertArrayEquals(new int[] { -1, 3, 4, 5, -1 }, tokens.get(2).tokenMap());

            assertArrayEquals(new int[] { 12, 4, 5, 6, 13 }, tokens.get(3).tokenIds());
            assertArrayEquals(new int[] { -1, 6, 7, 8, -1 }, tokens.get(3).tokenMap());

            assertArrayEquals(new int[] { 12, 7, 10, 13 }, tokens.get(4).tokenIds());
            assertArrayEquals(new int[] { -1, 9, 10, -1 }, tokens.get(4).tokenMap());

            // Adverse scenario to make sure that we simply attempt to split on words and proceed to split on words if the configuration
            // is such that we cannot
            // In this case, with a span of one and two split words next to eachother, the first subsequence has
            // "God ##zilla" and the next starts "##zilla Elastic ##search"
            tokens = tokenizer.tokenize("Godzilla Elasticsearch day with Pancake and my little red car.", Tokenization.Truncate.NONE, 1, 0);
            assertThat(tokens.size(), equalTo(7));
            assertArrayEquals(new int[] { 12, 8, 9, 13 }, tokens.get(0).tokenIds());
            assertArrayEquals(new int[] { -1, 0, 0, -1 }, tokens.get(0).tokenMap());
            assertArrayEquals(new int[] { 12, 9, 0, 1, 13 }, tokens.get(1).tokenIds());
            assertArrayEquals(new int[] { -1, 0, 1, 1, -1 }, tokens.get(1).tokenMap());
        }
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, 5, Tokenization.Truncate.NONE, 0)
            ).build()
        ) {
            List<TokenizationResult.Tokens> tokens = tokenizer.tokenize(
                "Godzilla Elasticsearch day with Pancake and my little red car.",
                Tokenization.Truncate.NONE,
                1,
                0
            );
            assertThat(tokens.size(), equalTo(3));
            assertArrayEquals(new int[] { 8, 9, 0, 1, 16 }, tokens.get(0).tokenIds());
            assertArrayEquals(new int[] { 0, 0, 1, 1, 2 }, tokens.get(0).tokenMap());

            assertArrayEquals(new int[] { 16, 18, 17, 15, 4 }, tokens.get(1).tokenIds());
            assertArrayEquals(new int[] { 2, 3, 4, 5, 6 }, tokens.get(1).tokenMap());

            assertArrayEquals(new int[] { 4, 5, 6, 7, 10 }, tokens.get(2).tokenIds());
            assertArrayEquals(new int[] { 6, 7, 8, 9, 10 }, tokens.get(2).tokenMap());
        }
    }

    public void testTokenizeLargeInputTruncation() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, 5, Tokenization.Truncate.FIRST, -1)
            ).build()
        ) {

            TokenizationResult.Tokens tokenization = tokenizer.tokenize(
                "Elasticsearch fun with Pancake and Godzilla",
                Tokenization.Truncate.FIRST,
                -1,
                0
            ).get(0);
            assertArrayEquals(new int[] { 0, 1, 3, 18, 17 }, tokenization.tokenIds());
        }

        try (
            BertTokenizer tokenizerWithSpecialTokens = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, true, 5, Tokenization.Truncate.FIRST, -1)
            ).build()
        ) {
            var tokenization = tokenizerWithSpecialTokens.tokenize(
                "Elasticsearch fun with Pancake and Godzilla",
                Tokenization.Truncate.FIRST,
                -1,
                0
            ).get(0);
            assertArrayEquals(new int[] { 12, 0, 1, 3, 13 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { -1, 0, 0, 1, -1 }, tokenization.tokenMap());
        }
    }

    public void testTokenizeAppendSpecialTokens() {
        try (BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault()).build()) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertArrayEquals(new int[] { 12, 0, 1, 3, 13 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { -1, 0, 0, 1, -1 }, tokenization.tokenMap());
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

            TokenizationResult.Tokens tokenization = tokenizer.tokenize(
                "Elasticsearch " + specialToken + " fun",
                Tokenization.Truncate.NONE,
                -1,
                0
            ).get(0);
            assertThat(tokenStrings(tokenization.tokens().get(0)), contains("Elastic", "##search", specialToken, "fun"));
            assertArrayEquals(new int[] { 0, 1, 15, 3 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 0, 1, 2 }, tokenization.tokenMap());
        }
    }

    public void testDoLowerCase() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                Arrays.asList("elastic", "##search", "fun", BertTokenizer.UNKNOWN_TOKEN, BertTokenizer.PAD_TOKEN),
                Tokenization.createDefault()
            ).setDoLowerCase(false).setWithSpecialTokens(false).build()
        ) {

            TokenizationResult.Tokens tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertArrayEquals(new int[] { 3, 2 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 1 }, tokenization.tokenMap());

            tokenization = tokenizer.tokenize("elasticsearch fun", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertArrayEquals(new int[] { 0, 1, 2 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.tokenMap());
        }

        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                Arrays.asList("elastic", "##search", "fun", BertTokenizer.UNKNOWN_TOKEN, BertTokenizer.PAD_TOKEN),
                Tokenization.createDefault()
            ).setDoLowerCase(true).setWithSpecialTokens(false).build()
        ) {

            TokenizationResult.Tokens tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertArrayEquals(new int[] { 0, 1, 2 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.tokenMap());
        }
    }

    public void testPunctuation() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault())
                .setWithSpecialTokens(false)
                .build()
        ) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize("Elasticsearch, fun.", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenStrings(tokenization.tokens().get(0)), contains("Elastic", "##search", ",", "fun", "."));
            assertArrayEquals(new int[] { 0, 1, 11, 3, 10 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 0, 1, 2, 3 }, tokenization.tokenMap());

            tokenization = tokenizer.tokenize("Elasticsearch, fun [MASK].", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertArrayEquals(new int[] { 0, 1, 11, 3, 14, 10 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 0, 1, 2, 3, 4 }, tokenization.tokenMap());
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

            TokenizationResult.Tokens tokenization = tokenizer.tokenize("This is [MASK]-tastic!", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenStrings(tokenization.tokens().get(0)), contains("This", "is", "[MASK]", "-", "ta", "##stic", "!"));
            assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 6, 7, 8, 9 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { -1, 0, 1, 2, 3, 4, 4, 5, -1 }, tokenization.tokenMap());

            tokenization = tokenizer.tokenize("This is sub~[MASK]!", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenStrings(tokenization.tokens().get(0)), contains("This", "is", "sub", "~", "[MASK]", "!"));
            assertArrayEquals(new int[] { 0, 1, 2, 10, 5, 3, 8, 9 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { -1, 0, 1, 2, 3, 4, 5, -1 }, tokenization.tokenMap());

            tokenization = tokenizer.tokenize("This is sub,[MASK].tastic!", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenStrings(tokenization.tokens().get(0)), contains("This", "is", "sub", ",", "[MASK]", ".", "ta", "##stic", "!"));
            assertArrayEquals(new int[] { 0, 1, 2, 10, 11, 3, 12, 6, 7, 8, 9 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { -1, 0, 1, 2, 3, 4, 5, 6, 6, 7, -1 }, tokenization.tokenMap());
        }
    }

    public void testBatchInput() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, null, Tokenization.Truncate.NONE, -1)
            ).build()
        ) {

            TokenizationResult tr = tokenizer.buildTokenizationResult(
                List.of(
                    tokenizer.tokenize("Elasticsearch", Tokenization.Truncate.NONE, -1, 0).get(0),
                    tokenizer.tokenize("my little red car", Tokenization.Truncate.NONE, -1, 1).get(0),
                    tokenizer.tokenize("Godzilla day", Tokenization.Truncate.NONE, -1, 2).get(0),
                    tokenizer.tokenize("Godzilla Pancake red car day", Tokenization.Truncate.NONE, -1, 3).get(0)
                )
            );
            assertThat(tr.getTokens(), hasSize(4));

            TokenizationResult.Tokens tokenization = tr.getTokenization(0);
            assertArrayEquals(new int[] { 0, 1 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 0 }, tokenization.tokenMap());

            tokenization = tr.getTokenization(1);
            assertArrayEquals(new int[] { 4, 5, 6, 7 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 1, 2, 3 }, tokenization.tokenMap());

            tokenization = tr.getTokenization(2);
            assertArrayEquals(new int[] { 8, 9, 16 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.tokenMap());

            tokenization = tr.getTokenization(3);
            assertArrayEquals(new int[] { 8, 9, 17, 6, 7, 16 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 0, 1, 2, 3, 4 }, tokenization.tokenMap());
        }
    }

    public void testMultiSeqTokenization() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault())
                .setDoLowerCase(false)
                .setWithSpecialTokens(true)
                .build()
        ) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize(
                "Elasticsearch is fun",
                "Godzilla my little red car",
                Tokenization.Truncate.NONE,
                0
            );

            var tokenStream = Arrays.stream(tokenization.tokenIds()).mapToObj(TEST_CASED_VOCAB::get).collect(Collectors.toList());
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
            assertArrayEquals(new int[] { 12, 0, 1, 2, 3, 13, 8, 9, 4, 5, 6, 7, 13 }, tokenization.tokenIds());
        }
    }

    public void testMultiSeqTokenizationWithSpan() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault())
                .setDoLowerCase(false)
                .setWithSpecialTokens(true)
                .build()
        ) {
            List<TokenizationResult.Tokens> tokenization = tokenizer.tokenize(
                "Elasticsearch is fun",
                "Godzilla my little red car",
                Tokenization.Truncate.NONE,
                1,
                0
            );
            assertThat(tokenization, hasSize(1));

            var tokenStream = Arrays.stream(tokenization.get(0).tokenIds()).mapToObj(TEST_CASED_VOCAB::get).collect(Collectors.toList());
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
            assertArrayEquals(new int[] { 12, 0, 1, 2, 3, 13, 8, 9, 4, 5, 6, 7, 13 }, tokenization.get(0).tokenIds());
        }
    }

    public void testMultiSeqTokenizationWithSpanOnLongInput() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault())
                .setDoLowerCase(false)
                .setWithSpecialTokens(true)
                .setMaxSequenceLength(8)
                .build()
        ) {
            List<TokenizationResult.Tokens> tokenizationList = tokenizer.tokenize(
                "Elasticsearch is fun",
                "Godzilla my little red car",
                Tokenization.Truncate.NONE,
                0,
                0
            );
            assertThat(tokenizationList, hasSize(6));
            String[] seventhToken = new String[] { "God", "##zilla", "my", "little", "red", "car" };
            for (int i = 0; i < seventhToken.length; i++) {
                assertThat(
                    Arrays.stream(tokenizationList.get(i).tokenIds()).mapToObj(TEST_CASED_VOCAB::get).collect(Collectors.toList()),
                    contains(
                        BertTokenizer.CLASS_TOKEN,
                        "Elastic",
                        "##search",
                        "is",
                        "fun",
                        BertTokenizer.SEPARATOR_TOKEN,
                        seventhToken[i],
                        BertTokenizer.SEPARATOR_TOKEN
                    )
                );
            }
        }
    }

    public void testMultiSeqTokenizationWithSpanFirstInputTooLong() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault())
                .setDoLowerCase(false)
                .setWithSpecialTokens(true)
                .setMaxSequenceLength(3)
                .build()
        ) {
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> tokenizer.tokenize("Elasticsearch is fun", "Godzilla my little red car", Tokenization.Truncate.NONE, 2, 0)
            );
            assertThat(
                iae.getMessage(),
                containsString(
                    "Unable to do sequence pair tokenization: the first sequence [7 tokens] "
                        + "is longer than the max sequence length [3 tokens]"
                )
            );
        }
    }

    public void testMultiSeqTokenizationWithSpanPlusFirstInputTooLong() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(TEST_CASED_VOCAB, Tokenization.createDefault())
                .setDoLowerCase(false)
                .setWithSpecialTokens(true)
                .setMaxSequenceLength(8)
                .build()
        ) {
            IllegalArgumentException iae = expectThrows(
                IllegalArgumentException.class,
                () -> tokenizer.tokenize("Elasticsearch is fun", "Godzilla my little red car", Tokenization.Truncate.NONE, 5, 0)
            );
            assertThat(
                iae.getMessage(),
                containsString(
                    "Unable to do sequence pair tokenization: the combined first sequence and span length [4 + 5 = 9 tokens] "
                        + "is longer than the max sequence length [8 tokens]. Reduce the size of the [span] window."
                )
            );
        }
    }

    public void testTokenizeLargeInputMultiSequenceTruncation() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, true, 10, Tokenization.Truncate.FIRST, -1)
            ).build()
        ) {

            TokenizationResult.Tokens tokenization = tokenizer.tokenize(
                "Elasticsearch is fun",
                "Godzilla my little red car",
                Tokenization.Truncate.FIRST,
                0
            );

            var tokenStream = Arrays.stream(tokenization.tokenIds()).mapToObj(TEST_CASED_VOCAB::get).collect(Collectors.toList());
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
                () -> BertTokenizer.builder(TEST_CASED_VOCAB, new BertTokenization(null, true, 8, Tokenization.Truncate.NONE, -1))
                    .build()
                    .tokenize("Elasticsearch is fun", "Godzilla my little red car", Tokenization.Truncate.NONE, 0)
            );
        }

        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, true, 10, Tokenization.Truncate.SECOND, -1)
            ).build()
        ) {

            TokenizationResult.Tokens tokenization = tokenizer.tokenize(
                "Elasticsearch is fun",
                "Godzilla my little red car",
                Tokenization.Truncate.SECOND,
                0
            );
            var tokenStream = Arrays.stream(tokenization.tokenIds()).mapToObj(TEST_CASED_VOCAB::get).collect(Collectors.toList());
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
            expectThrows(Exception.class, () -> tokenizer.tokenize("foo", "foo", Tokenization.Truncate.NONE, 0));
        }
    }

    public void testUnknownWordWithKnownSubWords() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, null, Tokenization.Truncate.NONE, -1)
            ).build()
        ) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize("Elasticsearchfoo fun", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenStrings(tokenization.tokens().get(0)), contains("[UNK]", "fun"));
            assertEquals(BertTokenizer.UNKNOWN_TOKEN, TEST_CASED_VOCAB.get(tokenization.tokenIds()[0]));
            assertEquals("fun", TEST_CASED_VOCAB.get(tokenization.tokenIds()[1]));
            assertArrayEquals(new int[] { 0, 1 }, tokenization.tokenMap());
        }
    }

    public void testCreateAnalyzer() {
        try (
            BertTokenizer tokenizer = BertTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, null, Tokenization.Truncate.NONE, -1)
            ).build()
        ) {
            WordPieceAnalyzer analyzer = tokenizer.createWordPieceAnalyzer(
                TEST_CASED_VOCAB,
                Collections.emptyList(),
                false,
                false,
                false,
                BertTokenizer.UNKNOWN_TOKEN
            );
            assertThat(analyzer, instanceOf(WordPieceAnalyzer.class));
            Tokenizer preTokenizer = analyzer.createTokenizer();
            assertThat(preTokenizer, instanceOf(WhitespaceTokenizer.class));
        }
    }
}
