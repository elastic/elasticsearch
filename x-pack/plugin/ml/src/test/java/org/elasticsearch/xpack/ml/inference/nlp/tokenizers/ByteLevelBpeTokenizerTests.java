/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ByteLevelBpeTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RobertaTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BpeTokenizerTests.TEST_CASED_VOCAB;
import static org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BpeTokenizerTests.TEST_CASE_MERGE;
import static org.hamcrest.Matchers.contains;

public class ByteLevelBpeTokenizerTests extends ESTestCase {

    private List<String> tokenStrings(List<? extends DelimitedToken> tokens) {
        return tokens.stream().map(DelimitedToken::toString).collect(Collectors.toList());
    }

    public void testTokenizeMatchesRobertaStyleDefaults() {
        var tokenization = new ByteLevelBpeTokenization(
            false,
            false,
            null,
            Tokenization.Truncate.NONE,
            -1,
            false,
            RobertaTokenizer.UNKNOWN_TOKEN,
            RobertaTokenizer.PAD_TOKEN,
            RobertaTokenizer.CLASS_TOKEN,
            RobertaTokenizer.SEPARATOR_TOKEN,
            RobertaTokenizer.MASK_TOKEN
        );
        try (
            ByteLevelBpeTokenizer tokenizer = ByteLevelBpeTokenizer.builder(TEST_CASED_VOCAB, TEST_CASE_MERGE, tokenization).build();
            RobertaTokenizer reference = RobertaTokenizer.builder(
                TEST_CASED_VOCAB,
                TEST_CASE_MERGE,
                new RobertaTokenization(false, false, null, Tokenization.Truncate.NONE, -1)
            ).build()
        ) {
            TokenizationResult.Tokens a = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE, -1, 0, null).get(0);
            TokenizationResult.Tokens b = reference.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE, -1, 0, null).get(0);
            assertThat(tokenStrings(a.tokens().get(0)), contains("Elast", "icsearch", "Ġfun"));
            assertArrayEquals(b.tokenIds(), a.tokenIds());
            assertArrayEquals(b.tokenMap(), a.tokenMap());
        }
    }

    public void testMultiSeqTokenization() {
        var tokenization = new ByteLevelBpeTokenization(
            null,
            null,
            null,
            Tokenization.Truncate.NONE,
            -1,
            false,
            null,
            null,
            null,
            null,
            null
        );
        try (
            ByteLevelBpeTokenizer tokenizer = ByteLevelBpeTokenizer.builder(TEST_CASED_VOCAB, TEST_CASE_MERGE, tokenization)
                .setWithSpecialTokens(true)
                .build()
        ) {
            TokenizationResult.Tokens tokenizationResult = tokenizer.tokenize(
                "Elasticsearch is fun",
                " Godzilla my little red car",
                Tokenization.Truncate.NONE,
                0
            );

            var tokenStream = Arrays.stream(tokenizationResult.tokenIds()).mapToObj(TEST_CASED_VOCAB::get).collect(Collectors.toList());
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

    /** User-configured {@code bos_token} and {@code eos_token} may be identical when both exist in the vocabulary. */
    public void testIdenticalBosAndEosSpecialTokensBuild() {
        var tokenization = new ByteLevelBpeTokenization(
            false,
            false,
            null,
            Tokenization.Truncate.NONE,
            -1,
            false,
            "<unk>",
            "<pad>",
            "</s>",
            "</s>",
            "<mask>"
        );
        try (
            ByteLevelBpeTokenizer tokenizer = ByteLevelBpeTokenizer.builder(TEST_CASED_VOCAB, TEST_CASE_MERGE, tokenization)
                .setWithSpecialTokens(true)
                .build()
        ) {
            assertEquals(tokenizer.clsTokenId(), tokenizer.sepTokenId());
        }
    }
}
