/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.search.uhighlight;

/**
 * Represents a scored highlighted snippet.
 * It's our own arbitrary object that we get back from the unified highlighter when highlighting a document.
 * Every snippet contains its formatted text and its score.
 * The score is needed in case we want to sort snippets by score, they get sorted by position in the text by default.
 */
public class Snippet {

    private final String text;
    private final float score;
    private final boolean isHighlighted;

    public Snippet(String text, float score, boolean isHighlighted) {
        this.text = text;
        this.score = score;
        this.isHighlighted = isHighlighted;
    }

    public String getText() {
        return text;
    }

    public float getScore() {
        return score;
    }

    public boolean isHighlighted() {
        return isHighlighted;
    }
}
