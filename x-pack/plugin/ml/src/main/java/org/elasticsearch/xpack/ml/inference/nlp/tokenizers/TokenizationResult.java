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

    public boolean anyTruncated() {
        return tokenizations.stream().anyMatch(Tokenization::isTruncated);
    }

    public String getFromVocab(int tokenId) {
        return vocab.get(tokenId);
    }

    public List<Tokenization> getTokenizations() {
        return tokenizations;
    }

    public void addTokenization(
        String input,
        boolean isTruncated,
        List<WordPieceTokenFilter.WordPieceToken> tokens,
        int[] tokenIds,
        int[] tokenMap
    ) {
        maxLength = Math.max(maxLength, tokenIds.length);
        tokenizations.add(new Tokenization(input, tokens, isTruncated, tokenIds, tokenMap));
    }

    public void addTokenization(Tokenization tokenization) {
        maxLength = Math.max(maxLength, tokenization.tokenIds.length);
        tokenizations.add(tokenization);
    }

    public int getLongestSequenceLength() {
        return maxLength;
    }

    public static class Tokenization {

        private final String input;
        private final List<WordPieceTokenFilter.WordPieceToken> tokens;
        private final int[] tokenIds;
        private final int[] tokenMap;
        private final boolean truncated;

        public Tokenization(
            String input,
            List<WordPieceTokenFilter.WordPieceToken> tokens,
            boolean truncated,
            int[] tokenIds,
            int[] tokenMap
        ) {
            assert tokenIds.length == tokenMap.length;
            this.input = input;
            this.tokens = tokens;
            this.tokenIds = tokenIds;
            this.tokenMap = tokenMap;
            this.truncated = truncated;
        }

        /**
         * The integer values of the tokens}
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

        public List<WordPieceTokenFilter.WordPieceToken> getTokens() {
            return tokens;
        }

        public boolean isTruncated() {
            return truncated;
        }
    }
}
