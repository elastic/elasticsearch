/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.XLMRobertaTokenization;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;

public class XLMRobertaTokenizerTests extends ESTestCase {

    private static final List<String> TEST_CASE_VOCAB = List.of(
        "<s>",
        "<pad>",
        "</s>",
        "<unk>",
        "▁Ela",
        "stic",
        "search",
        "▁is",
        "▁fun",
        "▁God",
        "z",
        "illa",
        "▁my",
        "▁little",
        "▁red",
        "▁car",
        "<mask>",
        "."
    );
    private static final List<Double> TEST_CASE_SCORES = List.of(
        0.0,
        0.0,
        0.0,
        0.0,
        -12.535264015197754,
        -12.300995826721191,
        -13.255199432373047,
        -7.402246475219727,
        -11.201482772827148,
        -10.576351165771484,
        -7.898513317108154,
        -10.230172157287598,
        -9.18289566040039,
        -11.451579093933105,
        -10.858806610107422,
        -10.214239120483398,
        0.0,
        -3.0
    );

    private List<String> tokenStrings(List<? extends DelimitedToken> tokens) {
        return tokens.stream().map(DelimitedToken::toString).collect(Collectors.toList());
    }

    public void testTokenize() throws IOException {
        try (
            XLMRobertaTokenizer tokenizer = XLMRobertaTokenizer.builder(
                TEST_CASE_VOCAB,
                TEST_CASE_SCORES,
                new XLMRobertaTokenization(false, null, Tokenization.Truncate.NONE, -1)
            ).build()
        ) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenStrings(tokenization.tokens().get(0)), contains("▁Ela", "stic", "search", "▁fun"));
            assertArrayEquals(new int[] { 4, 5, 6, 8 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 1, 2, 3 }, tokenization.tokenMap());
        }
    }

    public void testTokenizeWithNeverSplit() throws IOException {
        try (
            XLMRobertaTokenizer tokenizer = XLMRobertaTokenizer.builder(
                TEST_CASE_VOCAB,
                TEST_CASE_SCORES,
                new XLMRobertaTokenization(false, null, Tokenization.Truncate.NONE, -1)
            ).build()
        ) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize("Elasticsearch .<mask>.", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenStrings(tokenization.tokens().get(0)), contains("▁Ela", "stic", "search", "▁", ".", "<mask>", "▁", "."));
        }
    }

    public void testMultiSeqTokenization() throws IOException {
        try (
            XLMRobertaTokenizer tokenizer = XLMRobertaTokenizer.builder(
                TEST_CASE_VOCAB,
                TEST_CASE_SCORES,
                new XLMRobertaTokenization(false, null, Tokenization.Truncate.NONE, -1)
            ).setWithSpecialTokens(true).build()
        ) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize(
                "Elasticsearch is fun",
                "Godzilla my little red car",
                Tokenization.Truncate.NONE,
                0
            );

            var tokenStream = Arrays.stream(tokenization.tokenIds()).mapToObj(TEST_CASE_VOCAB::get).collect(Collectors.toList());
            assertThat(
                tokenStream,
                contains(
                    XLMRobertaTokenizer.CLASS_TOKEN,
                    "▁Ela",
                    "stic",
                    "search",
                    "▁is",
                    "▁fun",
                    XLMRobertaTokenizer.SEPARATOR_TOKEN,
                    XLMRobertaTokenizer.SEPARATOR_TOKEN,
                    "▁God",
                    "z",
                    "illa",
                    "▁my",
                    "▁little",
                    "▁red",
                    "▁car",
                    XLMRobertaTokenizer.SEPARATOR_TOKEN
                )
            );
        }
    }

}
