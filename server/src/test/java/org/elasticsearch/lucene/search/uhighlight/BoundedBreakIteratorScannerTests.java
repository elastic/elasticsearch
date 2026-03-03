/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.uhighlight;

import org.apache.lucene.search.uhighlight.SplittingBreakIterator;
import org.elasticsearch.test.ESTestCase;

import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BoundedBreakIteratorScannerTests extends ESTestCase {
    private static final String[] WORD_BOUNDARIES = new String[] { " ", "  ", "\t", "#", "\n" };
    private static final String[] SENTENCE_BOUNDARIES = new String[] { "! ", "? ", ". ", ".\n", ".\n\n" };

    private void testRandomAsciiTextCase(BreakIterator bi, int maxLen) {
        // Generate a random set of unique terms with ascii character
        int maxSize = randomIntBetween(5, 100);
        String[] vocabulary = new String[maxSize];
        for (int i = 0; i < maxSize; i++) {
            if (rarely()) {
                vocabulary[i] = randomAlphaOfLengthBetween(50, 200);
            } else {
                vocabulary[i] = randomAlphaOfLengthBetween(1, 30);
            }
        }

        // Generate a random text made of random terms separated with word-boundaries
        // and sentence-boundaries.
        StringBuilder text = new StringBuilder();
        List<Integer> offsetList = new ArrayList<>();
        List<Integer> sizeList = new ArrayList<>();
        // the number of sentences to generate
        int numSentences = randomIntBetween(10, 100);
        int maxTermLen = 0;
        for (int i = 0; i < numSentences; i++) {
            // the number of terms in the sentence
            int numTerms = randomIntBetween(5, 10);
            for (int j = 0; j < numTerms; j++) {
                int termId = randomIntBetween(0, vocabulary.length - 1);
                String term = vocabulary[termId].toLowerCase(Locale.ROOT);
                if (j == 0) {
                    // capitalize the first letter of the first term in the sentence
                    term = term.substring(0, 1).toUpperCase(Locale.ROOT) + term.substring(1);
                } else {
                    String sep = randomFrom(WORD_BOUNDARIES);
                    text.append(sep);
                }
                maxTermLen = Math.max(term.length(), maxTermLen);
                offsetList.add(text.length());
                sizeList.add(term.length());
                text.append(term);
            }
            String boundary = randomFrom(SENTENCE_BOUNDARIES);
            text.append(boundary);
        }

        int[] sizes = sizeList.stream().mapToInt(i -> i).toArray();
        int[] offsets = offsetList.stream().mapToInt(i -> i).toArray();

        bi.setText(text.toString());
        int currentPos = randomIntBetween(0, 20);
        int lastEnd = -1;
        int maxPassageLen = maxLen + (maxTermLen * 2);
        while (currentPos < offsets.length) {
            // find the passage that contains the current term
            int nextOffset = offsets[currentPos];
            int start = bi.preceding(nextOffset + 1);
            int end = bi.following(nextOffset);

            // check that the passage is valid
            assertThat(start, greaterThanOrEqualTo(lastEnd));
            assertThat(end, greaterThan(start));
            assertThat(start, lessThanOrEqualTo(nextOffset));
            assertThat(end, greaterThanOrEqualTo(nextOffset));
            int passageLen = end - start;
            assertThat(passageLen, lessThanOrEqualTo(maxPassageLen));

            // checks that the start and end of the passage are on word boundaries.
            int startPos = Arrays.binarySearch(offsets, start);
            int endPos = Arrays.binarySearch(offsets, end);
            if (startPos < 0) {
                int lastWordEnd = offsets[Math.abs(startPos) - 2] + sizes[Math.abs(startPos) - 2];
                assertThat(start, greaterThanOrEqualTo(lastWordEnd));
            }
            if (endPos < 0) {
                if (Math.abs(endPos) - 2 < offsets.length) {
                    int lastWordEnd = offsets[Math.abs(endPos) - 2] + sizes[Math.abs(endPos) - 2];
                    assertThat(end, greaterThanOrEqualTo(lastWordEnd));
                }
                // advance the position to the end of the current passage
                currentPos = (Math.abs(endPos) - 1);
            } else {
                // advance the position to the end of the current passage
                currentPos = endPos;
            }
            // randomly advance to the next term to highlight
            currentPos += randomIntBetween(0, 20);
            lastEnd = end;
        }
    }

    public void testBoundedSentence() {
        for (int i = 0; i < 20; i++) {
            int maxLen = randomIntBetween(10, 500);
            testRandomAsciiTextCase(BoundedBreakIteratorScanner.getSentence(Locale.ROOT, maxLen), maxLen);
        }
    }

    public void testTextThatEndsBeforeMaxLen() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 1000);

        final String text = "This is the first test sentence. Here is the second one.";

        int offset = text.indexOf("first");
        bi.setText(text);
        assertEquals(0, bi.preceding(offset));
        assertEquals(text.length(), bi.following(offset - 1));

        offset = text.indexOf("second");
        bi.setText(text);
        assertEquals(33, bi.preceding(offset));
        assertEquals(text.length(), bi.following(offset - 1));
    }

    public void testFragmentSizeThatIsTooBig() {
        final int fragmentSize = Integer.MAX_VALUE;
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, fragmentSize);

        final String text = "Any sentence";
        final int offset = 0; // find at beggining of text

        bi.setText(text);
        assertEquals(0, bi.preceding(offset));
        assertEquals(text.length(), bi.following(offset - 1));
    }

    // ---- Tests for first() ----

    public void testFirstReturnsZeroAfterSetText() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        final String text = "This is the first sentence. Here is the second one.";
        bi.setText(text);
        assertEquals(0, bi.first());
    }

    public void testFirstResetsStateAndPrecedingFollowingStillWorks() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        final String text = "This is the first sentence. Here is the second one.";
        bi.setText(text);

        // Perform a normal preceding/following call first
        int offset = text.indexOf("second");
        int start1 = bi.preceding(offset);
        int end1 = bi.following(offset - 1);

        // Call first() to reset state
        assertEquals(0, bi.first());

        // Re-setText and call preceding/following again, results should be consistent
        bi.setText(text);
        int start2 = bi.preceding(offset);
        int end2 = bi.following(offset - 1);

        assertEquals(start1, start2);
        assertEquals(end1, end2);
    }

    // ---- Tests for next() ----

    public void testNextReturnsSentenceBoundary() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 1000);
        final String text = "First sentence. Second sentence. Third sentence.";
        bi.setText(text);

        // first() positions to the beginning
        assertEquals(0, bi.first());
        // next() should return the next sentence boundary
        int boundary = bi.next();
        assertThat(boundary, greaterThan(0));
        assertThat(boundary, lessThanOrEqualTo(text.length()));
    }

    public void testNextThenPrecedingFollowingStillWorks() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        final String text = "First sentence. Second sentence. Third sentence.";

        // Get baseline results
        bi.setText(text);
        int offset = text.indexOf("Second");
        int start1 = bi.preceding(offset);
        int end1 = bi.following(offset - 1);

        // After calling next(), re-setText, preceding/following should not be affected
        bi.setText(text);
        bi.first();
        bi.next();

        bi.setText(text);
        int start2 = bi.preceding(offset);
        int end2 = bi.following(offset - 1);

        assertEquals(start1, start2);
        assertEquals(end1, end2);
    }

    // ---- Tests for last() ----

    public void testLastReturnsTextEndPosition() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 1000);
        final String text = "First sentence. Second sentence.";
        bi.setText(text);

        int lastPos = bi.last();
        assertEquals(text.length(), lastPos);
    }

    public void testLastThenPrecedingFollowingStillWorks() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        final String text = "First sentence. Second sentence.";

        // Get baseline results
        bi.setText(text);
        int offset = text.indexOf("Second");
        int start1 = bi.preceding(offset);
        int end1 = bi.following(offset - 1);

        // After calling last(), re-setText, preceding/following should not be affected
        bi.setText(text);
        bi.last();

        bi.setText(text);
        int start2 = bi.preceding(offset);
        int end2 = bi.following(offset - 1);

        assertEquals(start1, start2);
        assertEquals(end1, end2);
    }

    // ---- Tests for next(n) ----

    public void testNextNReturnsBoundary() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 1000);
        final String text = "First sentence. Second sentence. Third sentence.";
        bi.setText(text);

        // Position to the start, then next(2) should skip 2 boundaries
        bi.first();
        int boundary = bi.next(2);
        assertThat(boundary, greaterThan(0));
        assertThat(boundary, lessThanOrEqualTo(text.length()));
    }

    public void testNextNThenPrecedingFollowingStillWorks() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        final String text = "First sentence. Second sentence. Third sentence.";

        // Get baseline results
        bi.setText(text);
        int offset = text.indexOf("Third");
        int start1 = bi.preceding(offset);
        int end1 = bi.following(offset - 1);

        // After calling next(n), re-setText, preceding/following should not be affected
        bi.setText(text);
        bi.first();
        bi.next(2);

        bi.setText(text);
        int start2 = bi.preceding(offset);
        int end2 = bi.following(offset - 1);

        assertEquals(start1, start2);
        assertEquals(end1, end2);
    }

    // ---- Tests for previous() ----

    public void testPreviousReturnsBoundary() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 1000);
        final String text = "First sentence. Second sentence. Third sentence.";
        bi.setText(text);

        // Position to the end first, then previous() should return the preceding boundary
        bi.last();
        int boundary = bi.previous();
        assertThat(boundary, greaterThanOrEqualTo(0));
        assertThat(boundary, lessThanOrEqualTo(text.length()));
    }

    public void testPreviousThenPrecedingFollowingStillWorks() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        final String text = "First sentence. Second sentence.";

        // Get baseline results
        bi.setText(text);
        int offset = text.indexOf("Second");
        int start1 = bi.preceding(offset);
        int end1 = bi.following(offset - 1);

        // After calling previous(), re-setText, preceding/following should not be affected
        bi.setText(text);
        bi.last();
        bi.previous();

        bi.setText(text);
        int start2 = bi.preceding(offset);
        int end2 = bi.following(offset - 1);

        assertEquals(start1, start2);
        assertEquals(end1, end2);
    }

    // ---- Tests for SplittingBreakIterator wrapping scenario ----

    public void testSplittingBreakIteratorWrappingDoesNotThrow() {
        // Simulate the scenario where UnifiedHighlighter wraps BoundedBreakIteratorScanner with SplittingBreakIterator
        BreakIterator bounded = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        // \u0000 is the multi-value field separator (MULTIVAL_SEP_CHAR)
        SplittingBreakIterator splitting = new SplittingBreakIterator(bounded, '\u0000');

        // Construct multi-value field text containing \0 separator
        final String text = "First value sentence.\u0000Second value sentence. Another one here.";
        splitting.setText(text);

        // following() should work normally without throwing IllegalStateException
        // When offset crosses the separator segment boundary, SplittingBreakIterator internally calls delegate.first()
        int boundary = splitting.following(0);
        assertThat(boundary, greaterThanOrEqualTo(0));
        assertThat(boundary, lessThanOrEqualTo(text.length()));

        // Call following in the second segment, triggering cross-segment boundary logic
        int secondValueStart = text.indexOf('\u0000') + 1;
        boundary = splitting.following(secondValueStart);
        assertThat(boundary, greaterThanOrEqualTo(secondValueStart));
        assertThat(boundary, lessThanOrEqualTo(text.length()));
    }

    public void testSplittingBreakIteratorPrecedingFollowingAcrossSegments() {
        BreakIterator bounded = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        SplittingBreakIterator splitting = new SplittingBreakIterator(bounded, '\u0000');

        final String text = "Hello world.\u0000Foo bar. Baz qux.";
        splitting.setText(text);

        // Perform preceding/following in the first segment
        int offset = text.indexOf("world");
        int start = splitting.preceding(offset + 1);
        int end = splitting.following(offset);
        assertThat(start, greaterThanOrEqualTo(0));
        assertThat(end, greaterThan(start));

        // Perform preceding/following in the second segment
        int secondOffset = text.indexOf("Baz");
        start = splitting.preceding(secondOffset + 1);
        end = splitting.following(secondOffset);
        assertThat(start, greaterThan(0));
        assertThat(end, greaterThan(start));
    }
}
