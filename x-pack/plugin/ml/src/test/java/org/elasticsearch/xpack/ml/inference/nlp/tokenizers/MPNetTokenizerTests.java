/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.MPNetTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;

public class MPNetTokenizerTests extends ESTestCase {

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
        MPNetTokenizer.CLASS_TOKEN,
        MPNetTokenizer.SEPARATOR_TOKEN,
        MPNetTokenizer.MASK_TOKEN,
        MPNetTokenizer.UNKNOWN_TOKEN,
        "day",
        "Pancake",
        "with",
        MPNetTokenizer.PAD_TOKEN
    );

    private List<String> tokenStrings(List<DelimitedToken> tokens) {
        return tokens.stream().map(DelimitedToken::getToken).collect(Collectors.toList());
    }

    public void testTokenize() {
        BertTokenizer tokenizer = MPNetTokenizer.mpBuilder(
            TEST_CASED_VOCAB,
            new MPNetTokenization(null, false, null, Tokenization.Truncate.NONE)
        ).build();

        TokenizationResult.Tokenization tokenization = tokenizer.tokenize("Elasticsearch fun", Tokenization.Truncate.NONE);
        assertThat(tokenStrings(tokenization.getTokens()), contains("Elasticsearch", "fun"));
        assertArrayEquals(new int[] { 0, 1, 3 }, tokenization.getTokenIds());
        assertArrayEquals(new int[] { 0, 0, 1 }, tokenization.getTokenMap());
    }

}
