/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import java.util.ArrayList;
import java.util.List;

public class TokenizationResult {

    private final List<String> vocab;
    private final List<Tokenization> tokenizations = new ArrayList<>();
    private int maxLength;

    public TokenizationResult(List<String> vocab) {
        this.vocab = vocab;
        this.maxLength = -1;
    }

    public String getFromVocab(int tokenId) {
        return vocab.get(tokenId);
    }

    public List<Tokenization> getTokenizations() {
        return tokenizations;
    }

    public void addTokenization(String input, List<String> tokens, int[] tokenIds, int[] tokenMap) {
        maxLength = Math.max(maxLength, tokenIds.length);
        tokenizations.add(new Tokenization(input, tokens, tokenIds, tokenMap));
    }

    public int getLongestSequenceLength() {
        return maxLength;
    }

    public static class Tokenization {

        String input;
        private final List<String> tokens;
        private final int[] tokenIds;
        private final int[] tokenMap;

        public Tokenization(String input, List<String> tokens, int[] tokenIds, int[] tokenMap) {
            assert tokens.size() == tokenIds.length;
            assert tokenIds.length == tokenMap.length;
            this.input = input;
            this.tokens = tokens;
            this.tokenIds = tokenIds;
            this.tokenMap = tokenMap;
        }

        /**
         * The token strings from the tokenization process
         *
         * @return A list of tokens
         */
        public List<String> getTokens() {
            return tokens;
        }

        /**
         * The integer values of the tokens in {@link #getTokens()}
         *
         * @return A list of token Ids
         */
        public int[] getTokenIds() {
            return tokenIds;
        }

        /**
         * Maps the token position to the position in the source text.
         * Source words may be divided into more than one token so more
         * than one token can map back to the source token
         *
         * @return Map of source token to
         */
        public int[] getTokenMap() {
            return tokenMap;
        }

        public String getInput() {
            return input;
        }
    }
}
