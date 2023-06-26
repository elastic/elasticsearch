/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lucene.search.uhighlight;

import org.apache.lucene.search.uhighlight.FieldHighlighter;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;

import java.text.BreakIterator;
import java.text.CharacterIterator;
import java.util.Locale;

/**
 * A custom break iterator that is used to find break-delimited passages bounded by
 * a provided maximum length in the {@link UnifiedHighlighter} context.
 * This class uses a {@link BreakIterator} to find the last break after the provided offset
 * that would create a passage smaller than <code>maxLen</code>.
 * If the {@link BreakIterator} cannot find a passage smaller than the maximum length,
 * a secondary break iterator is used to re-split the passage at the first boundary after
 * maximum length.
 *
 * This is useful to split passages created by {@link BreakIterator}s like `sentence` that
 * can create big outliers on semi-structured text.
 *
 *
 * WARNING: This break iterator is designed to work with the {@link UnifiedHighlighter}.
 *
 * TODO: We should be able to create passages incrementally, starting from the offset of the first match and expanding or not
 * depending on the offsets of subsequent matches. This is currently impossible because {@link FieldHighlighter} uses
 * only the first matching offset to derive the start and end of each passage.
 **/
public class BoundedBreakIteratorScanner extends BreakIterator {
    private final BreakIterator mainBreak;
    private final BreakIterator innerBreak;
    private final int maxLen;

    private int lastPrecedingOffset = -1;
    private int windowStart = -1;
    private int windowEnd = -1;
    private int innerStart = -1;
    private int innerEnd = 0;

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
        innerEnd = 0;
    }

    /**
     * Must be called with increasing offset. See {@link FieldHighlighter} for usage.
     */
    @Override
    public int preceding(int offset) {
        if (offset < lastPrecedingOffset) {
            throw new IllegalArgumentException("offset < lastPrecedingOffset: " + "usage doesn't look like UnifiedHighlighter");
        }
        if (offset > windowStart && offset < windowEnd) {
            innerStart = innerEnd;
            innerEnd = windowEnd;
        } else {
            innerStart = Math.max(mainBreak.preceding(offset), 0);

            final long targetEndOffset = (long) offset + Math.max(0, maxLen - (offset - innerStart));
            final int textEndIndex = getText().getEndIndex();

            if (targetEndOffset + 1 > textEndIndex) {
                innerEnd = textEndIndex;
            } else {
                innerEnd = mainBreak.preceding((int) targetEndOffset + 1);
            }

            assert innerEnd != DONE && innerEnd >= innerStart;

            // in case no break was found up to maxLen, find one afterwards.
            if (innerStart == innerEnd) {
                innerEnd = mainBreak.following((int) targetEndOffset);
                assert innerEnd - innerStart > maxLen;
            } else {
                assert innerEnd - innerStart <= maxLen;
            }

            windowStart = innerStart;
            windowEnd = innerEnd;
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

    /**
     * Can be invoked only after a call to preceding(offset+1).
     * See {@link FieldHighlighter} for usage.
     */
    @Override
    public int following(int offset) {
        if (offset != lastPrecedingOffset || innerEnd == -1) {
            throw new IllegalArgumentException("offset != lastPrecedingOffset: " + "usage doesn't look like UnifiedHighlighter");
        }
        return innerEnd;
    }

    /**
     * Returns a {@link BreakIterator#getSentenceInstance(Locale)} bounded to maxLen.
     * Secondary boundaries are found using a {@link BreakIterator#getWordInstance(Locale)}.
     */
    public static BreakIterator getSentence(Locale locale, int maxLen) {
        final BreakIterator sBreak = BreakIterator.getSentenceInstance(locale);
        final BreakIterator wBreak = BreakIterator.getWordInstance(locale);
        return new BoundedBreakIteratorScanner(sBreak, wBreak, maxLen);
    }

    @Override
    public int current() {
        // Returns the last offset of the current split
        return this.innerEnd;
    }

    @Override
    public int first() {
        throw new IllegalStateException("first() should not be called in this context");
    }

    @Override
    public int next() {
        throw new IllegalStateException("next() should not be called in this context");
    }

    @Override
    public int last() {
        throw new IllegalStateException("last() should not be called in this context");
    }

    @Override
    public int next(int n) {
        throw new IllegalStateException("next(n) should not be called in this context");
    }

    @Override
    public int previous() {
        throw new IllegalStateException("previous() should not be called in this context");
    }
}
