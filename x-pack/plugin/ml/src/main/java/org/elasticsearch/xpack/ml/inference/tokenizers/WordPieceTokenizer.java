/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.tokenizers;

import java.util.ArrayList;
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

    public static final String DEFAULT_UNKNOWN_TOKEN = "[UNK]";
    public static final int DEFAULT_MAX_INPUT_CHARS_PER_WORD = 100;

    private static final String CONTINUATION = "##";

    private final Map<String, Integer> vocab;
    private final String unknownToken;
    private final int maxInputCharsPerWord;

    public WordPieceTokenizer(Map<String, Integer> vocab) {
        this(vocab, DEFAULT_UNKNOWN_TOKEN, DEFAULT_MAX_INPUT_CHARS_PER_WORD);
    }

    public WordPieceTokenizer(Map<String, Integer> vocab, String unknownToken, int maxInputCharsPerWord) {
        this.vocab = vocab;
        this.unknownToken = unknownToken;
        this.maxInputCharsPerWord = maxInputCharsPerWord;
    }

    public List<String> tokenize(String text) {
        String[] tokens = BasicTokenizer.whiteSpaceTokenize(text);

        List<String> output = new ArrayList<>(tokens.length);
        for (String token : tokens) {
            if (token.length() > maxInputCharsPerWord) {
                output.add(unknownToken);
                continue;
            }

            boolean isBad = false;
            int start = 0;
            List<String> subTokens = new ArrayList<>();
            int length = token.length();
            while (start < length) {
                int end = length;

                String currentValidSubStr = null;

                while (start < end) {
                    String subStr;
                    if (start > 0) {
                        subStr = CONTINUATION + token.substring(start, end);
                    } else {
                        subStr = token.substring(start, end);
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
                subTokens.add(currentValidSubStr);

                start = end;
            }

            if (isBad) {
                output.add(unknownToken);
            } else  {
                output.addAll(subTokens);
            }
        }

        return output;
    }
}
