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

package org.elasticsearch.search.highlight;

import org.apache.lucene.search.vectorhighlight.BoundaryScanner;

import java.text.BreakIterator;

/**
 * Tries to split snippets at sentence boundaries.  Not thread safe and shouldn't be kept between highlights.
 */
public class SentenceBoundaryScanner implements BoundaryScanner {
    private final int maxScan;
    private StringBuilder lastBuffer;
    private int lastBufferLength;
    private BreakIterator sentenceBreakIterator;
    private BreakIterator wordBreakIterator;

    public SentenceBoundaryScanner(int maxScan) {
        this.maxScan = maxScan;
    }

    @Override
    public int findStartOffset(StringBuilder buffer, int start) {
        // avoid illegal start offset
        if(start > buffer.length() || start < 1) {
            return start;
        }

        ensureSentenceBreakIterator(buffer);
        int proposedStart = sentenceBreakIterator.preceding(start);
        if (proposedStart == BreakIterator.DONE) {
            // Start is on the first break so we just start there.
            return start;
        }
        if (start - proposedStart <= maxScan) {
            // Proposed start is within maxScan so start there.
            return proposedStart;
        }

        // Proposed start is outside of max scan so find a word break.
        ensureWordBreakIterator(buffer);
        proposedStart = wordBreakIterator.preceding(start);
        if (start - proposedStart <= maxScan) {
            // Proposed start is within maxScan so start there.
            return proposedStart;
        }
        assert BreakIterator.DONE < 0: "If DONE is > 0, first wow, second that would break this.";
        // Proposed start is either not within maxScan OR proposed start is first break.
        return proposedStart;
    }

    @Override
    public int findEndOffset(StringBuilder buffer, int end) {
        // avoid illegal start offset
        if(end > buffer.length() || end < 0) {
            return end;
        }

        ensureSentenceBreakIterator(buffer);
        int proposedEnd = sentenceBreakIterator.following(end);
        if (proposedEnd == BreakIterator.DONE) {
            // End is on the last break so we just end there.
            return end;
        }
        if (proposedEnd - end <= maxScan) {
            // Proposed end is within maxScan so end there.
            return proposedEnd;
        }

        // Proposed end is outside of max scan so find a word break.
        ensureWordBreakIterator(buffer);
        proposedEnd = wordBreakIterator.following(end);
        if (proposedEnd - end <= maxScan) {
            // Proposed end is within maxScan so end there.
            return proposedEnd;
        }
        assert BreakIterator.DONE < 0: "If DONE is > 0, first wow, second that would break this.";
        // Proposed end is either not within maxScan OR proposed end is on last break.
        return proposedEnd;
    }

    private void ensureSentenceBreakIterator(StringBuilder buffer) {
        if (lastBuffer == buffer && lastBufferLength == buffer.length()) {
            return;
        }
        lastBuffer = buffer;
        lastBufferLength = buffer.length();
        sentenceBreakIterator = BreakIterator.getSentenceInstance();
        sentenceBreakIterator.setText(buffer.toString()); // TODO? implement a proper CharacterIterator?
        wordBreakIterator = null;
    }

    private void ensureWordBreakIterator(StringBuilder buffer) {
        if (wordBreakIterator != null) {
            return;
        }
        lastBuffer = buffer;
        wordBreakIterator = BreakIterator.getWordInstance();
        wordBreakIterator.setText(buffer.toString()); // TODO? implement a proper CharacterIterator?
    }
}
