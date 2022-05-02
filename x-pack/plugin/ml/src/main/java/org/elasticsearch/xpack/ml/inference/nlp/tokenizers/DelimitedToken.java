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

    static DelimitedToken mergeTokens(List<DelimitedToken> tokens) {
        if (tokens.size() == 1) {
            return tokens.get(0);
        }
        int startOffSet = tokens.get(0).startOffset;
        int endOffset = tokens.get(tokens.size() - 1).endOffset;
        return new DelimitedToken(
            tokens.stream().map(DelimitedToken::charSequence).map(CharSequence::toString).collect(Collectors.joining()),
            startOffSet,
            endOffset
        );
    }

    private final CharSequence charSequence;
    private final int startOffset;
    private final int endOffset;

    public DelimitedToken(CharSequence charSequence, int startOffset, int endOffset) {
        this.charSequence = charSequence;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public CharSequence charSequence() {
        return charSequence;
    }

    public int startOffset() {
        return startOffset;
    }

    public int endOffset() {
        return endOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelimitedToken that = (DelimitedToken) o;
        return startOffset == that.startOffset && endOffset == that.endOffset && Objects.equals(charSequence, that.charSequence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charSequence, startOffset, endOffset);
    }

    @Override
    public String toString() {
        return this.charSequence.toString();
    }

    public static class Encoded extends DelimitedToken {
        private final int encoding;

        public Encoded(CharSequence charSequence, int encoding, int startOffset, int endOffset) {
            super(charSequence, startOffset, endOffset);
            this.encoding = encoding;
        }

        public int getEncoding() {
            return encoding;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            Encoded encoded = (Encoded) o;
            return encoding == encoded.encoding;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), encoding);
        }
    }
}
