/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DelimitedToken {

    /**
     * Merges the list of tokens.
     *
     * Assumes that the tokens are in order.
     *
     * @param tokens
     * @return The merged token
     */
    public static DelimitedToken mergeTokens(List<DelimitedToken> tokens) {
        if (tokens.size() == 1) {
            return tokens.get(0);
        }

        String merged = tokens.stream().map(DelimitedToken::getToken).collect(Collectors.joining());
        return new DelimitedToken(tokens.get(0).getStartPos(), tokens.get(tokens.size() - 1).getEndPos(), merged);
    }

    private final int startPos;
    private final int endPos;
    private final String token;

    DelimitedToken(int startPos, int endPos, String token) {
        this.startPos = startPos;
        this.endPos = endPos;
        this.token = token;
    }

    public int getStartPos() {
        return startPos;
    }

    public int getEndPos() {
        return endPos;
    }

    public String getToken() {
        return token;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelimitedToken that = (DelimitedToken) o;
        return startPos == that.startPos && endPos == that.endPos && Objects.equals(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startPos, endPos, token);
    }

    @Override
    public String toString() {
        return "{" + "startPos=" + startPos + ", endPos=" + endPos + ", token=" + token + '}';
    }
}
