/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

public final class TokenFormat {
    private final String rawFormat;
    private final char[] subTokenDelimiters;
    private final SubTokenType[] subTokenTypes;

    public TokenFormat(String rawFormat, char[] subTokenDelimiters, SubTokenType[] subTokens) {
        this.rawFormat = rawFormat;
        this.subTokenDelimiters = subTokenDelimiters;
        this.subTokenTypes = subTokens;
    }

    public String getRawFormat() {
        return rawFormat;
    }

    public char[] getSubTokenDelimiters() {
        return subTokenDelimiters;
    }

    public SubTokenType[] getSubTokenTypes() {
        return subTokenTypes;
    }

    public int getNumberOfSubTokens() {
        return subTokenTypes.length;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < subTokenTypes.length; i++) {
            sb.append('$').append(subTokenTypes[i].name());
            if (i < subTokenDelimiters.length) {
                sb.append(subTokenDelimiters[i]);
            }
        }
        return sb.toString();
    }
}
