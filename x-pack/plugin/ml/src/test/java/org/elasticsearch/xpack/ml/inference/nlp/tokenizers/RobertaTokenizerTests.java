/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RobertaTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BpeTokenizerTests.TEST_CASED_VOCAB;
import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BpeTokenizerTests.TEST_CASE_MERGE;
import static org.hamcrest.Matchers.contains;

public class RobertaTokenizerTests extends ESTestCase {

    private List<String> tokenStrings(List<? extends DelimitedToken> tokens) {
        return tokens.stream().map(DelimitedToken::toString).collect(Collectors.toList());
    }

    public void testTokenize() {
        try (
            RobertaTokenizer tokenizer = RobertaTokenizer.builder(
                TEST_CASED_VOCAB,
                TEST_CASE_MERGE,
                new RobertaTokenization(true, false, null, Tokenization.Truncate.NONE, -1)
            ).build()
        ) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE, -1, 0).get(0);
            assertThat(tokenStrings(tokenization.tokens().get(0)), contains("Elast", "icsearch", "Ġfun"));
            assertArrayEquals(new int[] { 0, 297, 299, 275, 2 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { -1, 0, 0, 1, -1 }, tokenization.tokenMap());
        }
    }

    public void testMultiSeqTokenization() {
        try (
            RobertaTokenizer tokenizer = RobertaTokenizer.builder(
                TEST_CASED_VOCAB,
                TEST_CASE_MERGE,
                new RobertaTokenization(null, false, null, Tokenization.Truncate.NONE, -1)
            ).setWithSpecialTokens(true).build()
        ) {
            TokenizationResult.Tokens tokenization = tokenizer.tokenize(
                "Elasticsearch is fun",
                " Godzilla my little red car",
                Tokenization.Truncate.NONE,
                0
            );

            var tokenStream = Arrays.stream(tokenization.tokenIds()).mapToObj(TEST_CASED_VOCAB::get).collect(Collectors.toList());
            assertThat(
                tokenStream,
                contains(
                    RobertaTokenizer.CLASS_TOKEN,
                    "Elast",
                    "icsearch",
                    "Ġ",
                    "i",
                    "s",
                    "Ġfun",
                    RobertaTokenizer.SEPARATOR_TOKEN,
                    RobertaTokenizer.SEPARATOR_TOKEN,
                    "ĠGodzilla",
                    "Ġ",
                    "m",
                    "y",
                    "Ġlittle",
                    "Ġred",
                    "Ġcar",
                    RobertaTokenizer.SEPARATOR_TOKEN
                )
            );
        }
    }

}
