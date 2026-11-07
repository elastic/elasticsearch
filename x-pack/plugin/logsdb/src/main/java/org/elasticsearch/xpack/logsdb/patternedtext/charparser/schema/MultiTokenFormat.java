/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

import java.util.List;

/**
 * A multi-token format can only contain valid token names prefixed with '$', separated by token delimiters and optionally
 * {@link Schema#getTokenBoundaryCharacters() "token boundary characters"}. The {@link #delimiterParts} are literal strings that
 * represent whole parts of the format between tokens, meaning that they may include only {@link Schema#getTokenDelimiters() token
 * delimiters} and {@link Schema#getTokenBoundaryCharacters() token boundary characters}. This list is always one element shorter than the
 * {@link #tokens} list.
 */
public class MultiTokenFormat {
    private final String rawFormat;
    private final List<String> delimiterParts;
    private final List<TokenType> tokens;

    public MultiTokenFormat(String rawFormat, List<String> delimiterParts, List<TokenType> tokens) {
        this.rawFormat = rawFormat;
        this.delimiterParts = delimiterParts;
        this.tokens = tokens;
    }

    public String getRawFormat() {
        return rawFormat;
    }

    public List<String> getDelimiterParts() {
        return delimiterParts;
    }

    public List<TokenType> getTokens() {
        return tokens;
    }

    public int getNumberOfSubTokens() {
        int count = 0;
        for (TokenType token : tokens) {
            count += token.getNumberOfSubTokens();
        }
        return count;
    }

    @Override
    public String toString() {
        return rawFormat;
    }
}
