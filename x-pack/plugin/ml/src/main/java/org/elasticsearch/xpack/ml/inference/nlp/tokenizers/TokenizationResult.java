/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import java.util.List;

public class TokenizationResult {

    String input;
    final List<String> vocab;
    private final List<String> tokens;
    private final int [] tokenIds;
    private final int [] tokenMap;

    public TokenizationResult(String input, List<String> vocab, List<String> tokens, int[] tokenIds, int[] tokenMap) {
        assert tokens.size() == tokenIds.length;
        assert tokenIds.length == tokenMap.length;
        this.input = input;
        this.vocab = vocab;
        this.tokens = tokens;
        this.tokenIds = tokenIds;
        this.tokenMap = tokenMap;
    }

    public String getFromVocab(int tokenId) {
        return vocab.get(tokenId);
    }

    /**
     * The token strings from the tokenization process
     * @return A list of tokens
     */
    public List<String> getTokens() {
        return tokens;
    }

    /**
     * The integer values of the tokens in {@link #getTokens()}
     * @return A list of token Ids
     */
    public int[] getTokenIds() {
        return tokenIds;
    }

    /**
     * Maps the token position to the position in the source text.
     * Source words may be divided into more than one token so more
     * than one token can map back to the source token
     * @return Map of source token to
     */
    public int[] getTokenMap() {
        return tokenMap;
    }

    public String getInput() {
        return input;
    }
}
