/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

public class MultiTokenFormat {
    String rawFormat;
    TokenType[] tokens;

    public MultiTokenFormat(String rawFormat, TokenType[] tokens) {
        this.rawFormat = rawFormat;
        this.tokens = tokens;
    }

    public String getRawFormat() {
        return rawFormat;
    }

    public TokenType[] getTokens() {
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
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tokens.length; i++) {
            sb.append('$').append(tokens[i].name());
            if (i < tokens.length - 1) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }
}
