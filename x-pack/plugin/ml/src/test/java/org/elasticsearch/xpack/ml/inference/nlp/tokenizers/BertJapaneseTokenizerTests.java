/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class BertJapaneseTokenizerTests extends ESTestCase {

    public static final List<String> TEST_CASED_VOCAB = List.of(
        "日本",
        "語",
        "使",
        "テスト",
        "、",
        "。",
        "Bert",
        "##Japanese",
        "##Tokenizer",
        "Elastic",
        "##search",
        BertTokenizer.CLASS_TOKEN,
        BertTokenizer.SEPARATOR_TOKEN,
        BertTokenizer.MASK_TOKEN,
        BertTokenizer.UNKNOWN_TOKEN,
        BertTokenizer.PAD_TOKEN
    );

    private List<String> tokenStrings(List<? extends DelimitedToken> tokens) {
        return tokens.stream().map(DelimitedToken::toString).collect(Collectors.toList());
    }

    public void testTokenize() {
        try (
            BertTokenizer tokenizer = BertJapaneseTokenizer.builder(
                TEST_CASED_VOCAB,
                new BertTokenization(null, false, null, Tokenization.Truncate.NONE, -1)
            ).build()
        ) {

            String msg = "日本語で、ElasticsearchのBertJapaneseTokenizerを使うテスト。";
            TokenizationResult.Tokens tokenization = tokenizer.tokenize(msg, Tokenization.Truncate.NONE, -1, 0).get(0);

            assertThat(
                tokenStrings(tokenization.tokens().get(0)),
                contains(
                    "日本",
                    "語",
                    BertTokenizer.UNKNOWN_TOKEN,
                    "、",
                    "Elastic",
                    "##search",
                    BertTokenizer.UNKNOWN_TOKEN,
                    "Bert",
                    "##Japanese",
                    "##Tokenizer",
                    BertTokenizer.UNKNOWN_TOKEN,
                    BertTokenizer.UNKNOWN_TOKEN,
                    "テスト",
                    "。"
                )
            );
            assertArrayEquals(new int[] { 0, 1, 14, 4, 9, 10, 14, 6, 7, 8, 14, 14, 3, 5 }, tokenization.tokenIds());
            assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 4, 5, 6, 6, 6, 7, 8, 9, 10 }, tokenization.tokenMap());
        }
    }

    public void testCreateAnalyzer() {
        try (
            BertTokenizer tokenizer = BertJapaneseTokenizer.builder(
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
            assertThat(analyzer, instanceOf(JapaneseWordPieceAnalyzer.class));
            Tokenizer preTokenizer = analyzer.createTokenizer();
            assertThat(preTokenizer, instanceOf(JapaneseTokenizer.class));
        }
    }
}
