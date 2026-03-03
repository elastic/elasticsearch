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

    // ---- first() 方法测试 ----

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

        // 先执行一次正常的 preceding/following 调用
        int offset = text.indexOf("second");
        int start1 = bi.preceding(offset);
        int end1 = bi.following(offset - 1);

        // 调用 first() 重置状态
        assertEquals(0, bi.first());

        // 重新 setText 并再次 preceding/following，结果应一致
        bi.setText(text);
        int start2 = bi.preceding(offset);
        int end2 = bi.following(offset - 1);

        assertEquals(start1, start2);
        assertEquals(end1, end2);
    }

    // ---- next() 方法测试 ----

    public void testNextReturnsSentenceBoundary() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 1000);
        final String text = "First sentence. Second sentence. Third sentence.";
        bi.setText(text);

        // first() 定位到起始位置
        assertEquals(0, bi.first());
        // next() 应该返回下一个句子的 boundary
        int boundary = bi.next();
        assertThat(boundary, greaterThan(0));
        assertThat(boundary, lessThanOrEqualTo(text.length()));
    }

    public void testNextThenPrecedingFollowingStillWorks() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        final String text = "First sentence. Second sentence. Third sentence.";

        // 获取基线结果
        bi.setText(text);
        int offset = text.indexOf("Second");
        int start1 = bi.preceding(offset);
        int end1 = bi.following(offset - 1);

        // 调用 next() 后重新 setText，preceding/following 应不受影响
        bi.setText(text);
        bi.first();
        bi.next();

        bi.setText(text);
        int start2 = bi.preceding(offset);
        int end2 = bi.following(offset - 1);

        assertEquals(start1, start2);
        assertEquals(end1, end2);
    }

    // ---- last() 方法测试 ----

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

        // 获取基线结果
        bi.setText(text);
        int offset = text.indexOf("Second");
        int start1 = bi.preceding(offset);
        int end1 = bi.following(offset - 1);

        // 调用 last() 后重新 setText，preceding/following 应不受影响
        bi.setText(text);
        bi.last();

        bi.setText(text);
        int start2 = bi.preceding(offset);
        int end2 = bi.following(offset - 1);

        assertEquals(start1, start2);
        assertEquals(end1, end2);
    }

    // ---- next(n) 方法测试 ----

    public void testNextNReturnsBoundary() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 1000);
        final String text = "First sentence. Second sentence. Third sentence.";
        bi.setText(text);

        // 定位到起始，然后 next(2) 应跳过 2 个 boundary
        bi.first();
        int boundary = bi.next(2);
        assertThat(boundary, greaterThan(0));
        assertThat(boundary, lessThanOrEqualTo(text.length()));
    }

    public void testNextNThenPrecedingFollowingStillWorks() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        final String text = "First sentence. Second sentence. Third sentence.";

        // 获取基线结果
        bi.setText(text);
        int offset = text.indexOf("Third");
        int start1 = bi.preceding(offset);
        int end1 = bi.following(offset - 1);

        // 调用 next(n) 后重新 setText，preceding/following 应不受影响
        bi.setText(text);
        bi.first();
        bi.next(2);

        bi.setText(text);
        int start2 = bi.preceding(offset);
        int end2 = bi.following(offset - 1);

        assertEquals(start1, start2);
        assertEquals(end1, end2);
    }

    // ---- previous() 方法测试 ----

    public void testPreviousReturnsBoundary() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 1000);
        final String text = "First sentence. Second sentence. Third sentence.";
        bi.setText(text);

        // 先定位到末尾，然后 previous() 应返回前一个 boundary
        bi.last();
        int boundary = bi.previous();
        assertThat(boundary, greaterThanOrEqualTo(0));
        assertThat(boundary, lessThanOrEqualTo(text.length()));
    }

    public void testPreviousThenPrecedingFollowingStillWorks() {
        BreakIterator bi = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        final String text = "First sentence. Second sentence.";

        // 获取基线结果
        bi.setText(text);
        int offset = text.indexOf("Second");
        int start1 = bi.preceding(offset);
        int end1 = bi.following(offset - 1);

        // 调用 previous() 后重新 setText，preceding/following 应不受影响
        bi.setText(text);
        bi.last();
        bi.previous();

        bi.setText(text);
        int start2 = bi.preceding(offset);
        int end2 = bi.following(offset - 1);

        assertEquals(start1, start2);
        assertEquals(end1, end2);
    }

    // ---- SplittingBreakIterator 包装场景测试 ----

    public void testSplittingBreakIteratorWrappingDoesNotThrow() {
        // 模拟 UnifiedHighlighter 用 SplittingBreakIterator 包装 BoundedBreakIteratorScanner 的场景
        BreakIterator bounded = BoundedBreakIteratorScanner.getSentence(Locale.ROOT, 100);
        // \u0000 是多值字段分隔符（MULTIVAL_SEP_CHAR）
        SplittingBreakIterator splitting = new SplittingBreakIterator(bounded, '\u0000');

        // 构造包含 \0 分隔符的多值字段文本
        final String text = "First value sentence.\u0000Second value sentence. Another one here.";
        splitting.setText(text);

        // following() 应该能正常工作，不抛出 IllegalStateException
        // 当 offset 跨越分隔符片段边界时，SplittingBreakIterator 内部会调用 delegate.first()
        int boundary = splitting.following(0);
        assertThat(boundary, greaterThanOrEqualTo(0));
        assertThat(boundary, lessThanOrEqualTo(text.length()));

        // 在第二个片段中调用 following，触发跨片段边界的逻辑
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

        // 在第一个片段中进行 preceding/following
        int offset = text.indexOf("world");
        int start = splitting.preceding(offset + 1);
        int end = splitting.following(offset);
        assertThat(start, greaterThanOrEqualTo(0));
        assertThat(end, greaterThan(start));

        // 在第二个片段中进行 preceding/following
        int secondOffset = text.indexOf("Baz");
        start = splitting.preceding(secondOffset + 1);
        end = splitting.following(secondOffset);
        assertThat(start, greaterThan(0));
        assertThat(end, greaterThan(start));
    }
}
