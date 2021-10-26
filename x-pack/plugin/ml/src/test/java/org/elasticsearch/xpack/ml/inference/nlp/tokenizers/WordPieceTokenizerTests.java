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
        Map<String, Integer> vocabMap =
            createVocabMap(UNKNOWN_TOKEN, "[CLS]", "[SEP]", "want", "##want", "##ed", "wa", "un", "runn", "##ing");
        WordPieceTokenizer tokenizer = new WordPieceTokenizer(vocabMap, UNKNOWN_TOKEN, 100);

        List<WordPieceTokenizer.TokenAndId> tokenAndIds = tokenizer.tokenize("");
        assertThat(tokenAndIds, empty());

        tokenAndIds = tokenizer.tokenize("unwanted running");
        List<String> tokens = tokenAndIds.stream().map(WordPieceTokenizer.TokenAndId::getToken).collect(Collectors.toList());
        assertThat(tokens, contains("un", "##want", "##ed", "runn", "##ing"));

        tokenAndIds = tokenizer.tokenize("unwantedX running");
        tokens = tokenAndIds.stream().map(WordPieceTokenizer.TokenAndId::getToken).collect(Collectors.toList());
        assertThat(tokens, contains(UNKNOWN_TOKEN, "runn", "##ing"));
    }

    public void testMaxCharLength() {
        Map<String, Integer> vocabMap = createVocabMap("Some", "words", "will", "become", "UNK");

        WordPieceTokenizer tokenizer = new WordPieceTokenizer(vocabMap, "UNK", 4);
        List<WordPieceTokenizer.TokenAndId> tokenAndIds = tokenizer.tokenize("Some words will become UNK");
        List<String> tokens = tokenAndIds.stream().map(WordPieceTokenizer.TokenAndId::getToken).collect(Collectors.toList());
        assertThat(tokens, contains("Some", "UNK", "will", "UNK", "UNK"));
    }

    static Map<String, Integer> createVocabMap(String ... words) {
        Map<String, Integer> vocabMap = new HashMap<>();
        for (int i=0; i<words.length; i++) {
            vocabMap.put(words[i], i);
        }
        return vocabMap;
    }
}
