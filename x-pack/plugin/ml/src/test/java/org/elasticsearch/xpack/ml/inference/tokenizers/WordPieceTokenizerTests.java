/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.tokenizers;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class WordPieceTokenizerTests extends ESTestCase {

    public void testTokenize() {
        Map<String, Integer> vocabMap =
            createVocabMap("[UNK]", "[CLS]", "[SEP]", "want", "##want", "##ed", "wa", "un", "runn", "##ing");
        WordPieceTokenizer tokenizer = new WordPieceTokenizer(vocabMap);

        List<String> tokens = tokenizer.tokenize("");
        assertThat(tokens, empty());

        tokens = tokenizer.tokenize("unwanted running");
        assertThat(tokens, contains("un", "##want", "##ed", "runn", "##ing"));

        tokens = tokenizer.tokenize("unwantedX running");
        assertThat(tokens, contains("[UNK]", "runn", "##ing"));
    }

    public void testMaxCharLength() {
        Map<String, Integer> vocabMap = createVocabMap("Some", "words", "will", "become", "UNK");

        WordPieceTokenizer tokenizer = new WordPieceTokenizer(vocabMap, "UNK", 4);
        List<String> tokens = tokenizer.tokenize("Some words will become UNK");
        assertThat(tokens, contains("Some", "UNK", "will", "UNK", "UNK"));
    }

    private Map<String, Integer> createVocabMap(String ... words) {
        Map<String, Integer> vocabMap = new HashMap<>();
        for (int i=0; i<words.length; i++) {
            vocabMap.put(words[i], i);
        }

        return vocabMap;
    }
}
