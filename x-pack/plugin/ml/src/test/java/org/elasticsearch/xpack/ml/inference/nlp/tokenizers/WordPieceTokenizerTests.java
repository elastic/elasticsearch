/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class WordPieceTokenizerTests extends ESTestCase {

    public static final String UNKNOWN_TOKEN = "[UNK]";

    public void testTokenize() {
        String[] vocab = { UNKNOWN_TOKEN, "[CLS]", "[SEP]", "want", "##want", "##ed", "wa", "un", "runn", "##ing" };
        Map<String, Integer> vocabMap = createVocabMap(vocab);

        WordPieceTokenizer tokenizer = new WordPieceTokenizer(vocabMap, UNKNOWN_TOKEN, 100);

        var tokenIds = tokenizer.tokenize(new DelimitedToken(0, 0, ""));
        assertThat(tokenIds, empty());

        tokenIds = tokenizer.tokenize(makeToken("unwanted"));
        List<String> tokenStrings = tokenIds.stream().map(index -> vocab[index]).collect(Collectors.toList());
        assertThat(tokenStrings, contains("un", "##want", "##ed"));

        tokenIds = tokenizer.tokenize(makeToken("running"));
        tokenStrings = tokenIds.stream().map(index -> vocab[index]).collect(Collectors.toList());
        assertThat(tokenStrings, contains("runn", "##ing"));

        tokenIds = tokenizer.tokenize(makeToken("unwantedX"));
        tokenStrings = tokenIds.stream().map(index -> vocab[index]).collect(Collectors.toList());
        assertThat(tokenStrings, contains(UNKNOWN_TOKEN));
    }

    private DelimitedToken makeToken(String str) {
        return new DelimitedToken(0, str.length(), str);
    }

    public void testMaxCharLength() {
        String[] vocab = { "Some", "words", "will", "become", "UNK" };
        Map<String, Integer> vocabMap = createVocabMap(vocab);

        WordPieceTokenizer tokenizer = new WordPieceTokenizer(vocabMap, "UNK", 4);
        var tokenIds = tokenizer.tokenize(new DelimitedToken(0, 0, "become"));
        List<String> tokenStrings = tokenIds.stream().map(index -> vocab[index]).collect(Collectors.toList());
        assertThat(tokenStrings, contains("UNK"));
    }

    static Map<String, Integer> createVocabMap(String... words) {
        Map<String, Integer> vocabMap = new HashMap<>();
        for (int i = 0; i < words.length; i++) {
            vocabMap.put(words[i], i);
        }
        return vocabMap;
    }
}
