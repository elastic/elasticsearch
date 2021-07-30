/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

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

    private static final String CONTINUATION = "##";

    private final Map<String, Integer> vocab;
    private final String unknownToken;
    private final int maxInputCharsPerWord;

    public static class TokenAndId {
        private final String token;
        private final int id;

        TokenAndId(String token, int id) {
            this.token = token;
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public String getToken() {
            return token;
        }
    }

    /**
     *
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
     * @param text A single token or whitespace separated tokens.
     *             Input should have been normalized by the {@link BasicTokenizer}.
     * @return List of tokens
     */
    public List<TokenAndId> tokenize(String text) {
        String[] tokens = BasicTokenizer.whiteSpaceTokenize(text);

        List<TokenAndId> output = new ArrayList<>();
        for (String token : tokens) {
            if (token.length() > maxInputCharsPerWord) {
                assert vocab.containsKey(unknownToken);
                output.add(new TokenAndId(unknownToken, vocab.get(unknownToken)));
                continue;
            }

            boolean isBad = false;
            int start = 0;
            List<TokenAndId> subTokens = new ArrayList<>();
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

                subTokens.add(new TokenAndId(currentValidSubStr, vocab.get(currentValidSubStr)));

                start = end;
            }

            if (isBad) {
                output.add(new TokenAndId(unknownToken, vocab.get(unknownToken)));
            } else  {
                output.addAll(subTokens);
            }
        }

        return output;
    }
}
