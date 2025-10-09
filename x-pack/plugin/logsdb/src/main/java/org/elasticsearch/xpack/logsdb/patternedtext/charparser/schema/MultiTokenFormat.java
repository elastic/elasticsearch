/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A multi-token format can only contain valid token names prefixed with '$', separated by token delimiters and optionally
 * {@link Schema#getTrimmedCharacters() "trimmed characters"}. The {@link #formatParts} can be a mix of {@link TokenType} instances and
 * literal strings that represent whole parts of the format between tokens, meaning that they may include only
 * {@link Schema#getTokenDelimiters() token delimiters} and {@link Schema#getTrimmedCharacters() trimmed characters}.
 * For convenience, the {@link #tokens} list contains only the {@link TokenType} instances extracted from the format parts, in the
 * order they appear in the format.
 */
public class MultiTokenFormat {
    private final String rawFormat;
    private final List<Object> formatParts;
    private final List<TokenType> tokens;

    public MultiTokenFormat(String rawFormat, List<Object> formatParts) {
        this.rawFormat = rawFormat;
        this.formatParts = formatParts;
        this.tokens = formatParts.stream().filter(TokenType.class::isInstance).map(TokenType.class::cast).collect(Collectors.toList());
    }

    public String getRawFormat() {
        return rawFormat;
    }

    public List<Object> getFormatParts() {
        return formatParts;
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
