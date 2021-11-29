/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import java.util.Objects;

public class DelimitedToken {
    public int startPos;
    public int endPos;
    public String token;

    DelimitedToken(int startPos, int endPos, String token) {
        this.startPos = startPos;
        this.endPos = endPos;
        this.token = token;
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
