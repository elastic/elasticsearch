/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

import java.util.Objects;

public class AnalyzeToken {
    private char[] term;
    private int termLen;
    private int startOffset;
    private int endOffset;
    private int position;
    private int positionLength;
    private String type;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AnalyzeToken that = (AnalyzeToken) o;
        return startOffset == that.startOffset
            && endOffset == that.endOffset
            && position == that.position
            && positionLength == that.positionLength
            && termLen == that.termLen
            && Objects.equals(term, that.term)
            && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, termLen, startOffset, endOffset, position, positionLength, type);
    }

    public AnalyzeToken() {

    }

    public AnalyzeToken(char[] term, int termLen, int position, int startOffset, int endOffset, int positionLength, String type) {
        this.term = term;
        this.termLen = termLen;
        this.position = position;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.positionLength = positionLength;
        this.type = type;
    }

    public char[] term() {
        return this.term;
    }

    public int termLen() {
        return this.termLen;
    }

    public int startOffset() {
        return this.startOffset;
    }

    public int endOffset() {
        return this.endOffset;
    }

    public int position() {
        return this.position;
    }

    public int positionLength() {
        return this.positionLength;
    }

    public String type() {
        return this.type;
    }

    public AnalyzeToken term(char[] term) {
        this.term = term;
        return this;
    }

    public AnalyzeToken type(String type) {
        this.type = type;
        return this;
    }

    public AnalyzeToken startOffset(int startOffset) {
        this.startOffset = startOffset;
        return this;
    }

    public AnalyzeToken endOffset(int endOffset) {
        this.endOffset = endOffset;
        return this;
    }

    public AnalyzeToken position(int position) {
        this.position = position;
        return this;
    }

    public AnalyzeToken positionLength(int positionLength) {
        this.positionLength = positionLength;
        return this;
    }

    public AnalyzeToken termLen(int termLen) {
        this.termLen = termLen;
        return this;
    }
}
