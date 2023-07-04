/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.search.uhighlight;

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
}
