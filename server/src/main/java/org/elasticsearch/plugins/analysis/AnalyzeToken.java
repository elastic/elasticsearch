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
    private final String term;
    private final int startOffset;
    private final int endOffset;
    private final int position;
    private final int positionLength;
    private final String type;

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
            && Objects.equals(term, that.term)
            && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, startOffset, endOffset, position, positionLength, type);
    }

    public AnalyzeToken(
        String term,
        int position,
        int startOffset,
        int endOffset,
        int positionLength,
        String type
    ) {
        this.term = term;
        this.position = position;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.positionLength = positionLength;
        this.type = type;
    }

    public String getTerm() {
        return this.term;
    }

    public int getStartOffset() {
        return this.startOffset;
    }

    public int getEndOffset() {
        return this.endOffset;
    }

    public int getPosition() {
        return this.position;
    }

    public int getPositionLength() {
        return this.positionLength;
    }

    public String getType() {
        return this.type;
    }
}
