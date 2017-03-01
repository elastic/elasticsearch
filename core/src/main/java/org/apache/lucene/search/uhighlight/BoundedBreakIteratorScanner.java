/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lucene.search.uhighlight;

import java.text.BreakIterator;
import java.text.CharacterIterator;
import java.util.Locale;

/**
 * A custom break iterator that scans text to find break-delimited passages bounded by a provided {@code maxLen}.
 * This scanner uses two level of {@link BreakIterator} to bound passage size "close" to {@maxLen}.
 * This is useful to "bound" {@link BreakIterator}s like `sentence` that can create big outliers on
 * semi-structured text.
 *
 * @warning This break iterator is designed to work with the {@link UnifiedHighlighter} only.
 **/
public class BoundedBreakIteratorScanner extends BreakIterator {
    private final BreakIterator mainBreak;
    private final BreakIterator innerBreak;
    private final int maxLen;

    private int lastPrecedingOffset = -1;
    private int windowStart = -1;
    private int windowEnd = -1;
    private int innerStart = -1;
    private int innerEnd = -1;

    private BoundedBreakIteratorScanner(BreakIterator mainBreak, BreakIterator innerBreak, int maxLen) {
        this.mainBreak = mainBreak;
        this.innerBreak = innerBreak;
        this.maxLen = maxLen;
    }

    @Override
    public CharacterIterator getText() {
        return mainBreak.getText();
    }

    @Override
    public void setText(CharacterIterator newText) {
        reset();
        mainBreak.setText(newText);
        innerBreak.setText(newText);
    }

    @Override
    public void setText(String newText) {
        reset();
        mainBreak.setText(newText);
        innerBreak.setText(newText);
    }

    private void reset() {
        lastPrecedingOffset = -1;
        windowStart = -1;
        windowEnd = -1;
        innerStart = -1;
        innerEnd = -1;
    }

    @Override
    public int preceding(int offset) {
        assert(offset > lastPrecedingOffset);
        if (offset > windowStart && offset < windowEnd) {
            innerStart = innerEnd;
            innerEnd = windowEnd;
        } else {
            windowStart = innerStart = mainBreak.preceding(offset);
            windowEnd = innerEnd = mainBreak.following(offset-1);
        }

        if (innerEnd - innerStart > maxLen) {
            // the current split is too big,
            // so starting from the current term we try to find boundaries on the left first
            if (offset - maxLen > innerStart) {
                innerStart = Math.max(innerStart, innerBreak.preceding(offset - maxLen));
            }
            // and then we try to expand the passage to the right with the remaining size
            int remaining = Math.max(0, maxLen - (offset - innerStart));
            if (offset + remaining < windowEnd) {
                innerEnd = Math.min(windowEnd, innerBreak.following(offset + remaining));
            }
        }
        lastPrecedingOffset = offset - 1;
        return innerStart;
    }

    @Override
    public int following(int offset) {
        assert(offset == lastPrecedingOffset && innerEnd != -1);
        return innerEnd;
    }

    /**
     * Returns a {@link BreakIterator#getSentenceInstance(Locale)} bounded to {@code maxLen}.
     * Secondary boundaries are found using a {@link BreakIterator#getWordInstance(Locale)}.
     */
    public static BreakIterator getSentence(Locale locale, int maxLen) {
        final BreakIterator sBreak = BreakIterator.getSentenceInstance(locale);
        final BreakIterator wBreak = BreakIterator.getWordInstance(locale);
        return new BoundedBreakIteratorScanner(sBreak, wBreak, maxLen);
    }

    @Override
    public int first() {
        return 0;
    }

    @Override
    public int current() {
        return 0;
    }

    @Override
    public int next() {
        throw new IllegalStateException("next() should not be called in this context");
    }

    @Override
    public int last() {
        throw new IllegalStateException("last should not be called in this context");
    }

    @Override
    public int next(int n) {
        throw new IllegalStateException("next(n) should not be call");
    }

    @Override
    public int previous() {
        throw new IllegalStateException("previous should not be call");
    }
}
