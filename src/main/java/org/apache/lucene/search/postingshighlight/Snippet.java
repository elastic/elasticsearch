/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.lucene.search.postingshighlight;

/**
 * Represents a scored highlighted snippet.
 * It's our own arbitrary object that we get back from the postings highlighter when highlighting a document.
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
