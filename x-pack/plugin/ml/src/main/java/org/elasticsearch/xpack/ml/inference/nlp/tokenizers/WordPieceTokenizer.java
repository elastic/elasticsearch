/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * SubWord tokenization via the Word Piece algorithm using the
 * provided vocabulary.
 *
 * The input is split by white space and should be pre-processed
 * by {@link BasicTokenizer}
 */
public class WordPieceTokenizer {

    private static final String CONTINUATION = "##";

    private final Map<String, Integer> vocab;
    private final String unknownToken;
    private final int maxInputCharsPerWord;

    /**
     * @param vocab The token vocabulary
     * @param unknownToken If not found in the vocabulary
     * @param maxInputCharsPerWord Inputs tokens longer than this are 'unknown'
     */
    public WordPieceTokenizer(Map<String, Integer> vocab, String unknownToken, int maxInputCharsPerWord) {
        this.vocab = vocab;
        this.unknownToken = unknownToken;
        this.maxInputCharsPerWord = maxInputCharsPerWord;
    }

    /**
     * Wordpiece tokenize the input text.
     *
     * @param token Word to tokenize
     * @return List of token IDs
     */
    public List<Integer> tokenize(DelimitedToken token) {

        if (token.getToken().length() > maxInputCharsPerWord) {
            assert vocab.containsKey(unknownToken);
            return Collections.singletonList(vocab.get(unknownToken));
        }

        List<Integer> output = new ArrayList<>();
        boolean isBad = false;
        int start = 0;
        int length = token.getToken().length();
        while (start < length) {
            int end = length;

            String currentValidSubStr = null;

            while (start < end) {
                String subStr;
                if (start > 0) {
                    subStr = CONTINUATION + token.getToken().substring(start, end);
                } else {
                    subStr = token.getToken().substring(start, end);
                }

                if (vocab.containsKey(subStr)) {
                    currentValidSubStr = subStr;
                    break;
                }

                end--;
            }

            if (currentValidSubStr == null) {
                isBad = true;
                break;
            }

            output.add(vocab.get(currentValidSubStr));

            start = end;
        }

        if (isBad) {
            return Collections.singletonList(vocab.get(unknownToken));
        } else {
            return output;
        }
    }
}
