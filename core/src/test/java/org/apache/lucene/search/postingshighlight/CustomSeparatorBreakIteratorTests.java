/*
Licensed to Elasticsearch under one or more contributor
license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright
ownership. Elasticsearch licenses this file to you under
the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License.
You may obtain a copy of the License at
 *
   http://www.apache.org/licenses/LICENSE-2.0
 *
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
 */

package org.apache.lucene.search.postingshighlight;

import org.elasticsearch.search.highlight.HighlightUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.text.BreakIterator;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Locale;

import static org.hamcrest.CoreMatchers.equalTo;

public class CustomSeparatorBreakIteratorTests extends ESTestCase {

    @Test
    public void testBreakOnCustomSeparator() throws Exception {
        Character separator = randomSeparator();
        BreakIterator bi = new CustomSeparatorBreakIterator(separator);
        String source = "this" + separator + "is" + separator + "the" + separator + "first" + separator + "sentence";
        bi.setText(source);
        assertThat(bi.current(), equalTo(0));
        assertThat(bi.first(), equalTo(0));
        assertThat(source.substring(bi.current(), bi.next()), equalTo("this" + separator));
        assertThat(source.substring(bi.current(), bi.next()), equalTo("is" + separator));
        assertThat(source.substring(bi.current(), bi.next()), equalTo("the" + separator));
        assertThat(source.substring(bi.current(), bi.next()), equalTo("first" + separator));
        assertThat(source.substring(bi.current(), bi.next()), equalTo("sentence"));
        assertThat(bi.next(), equalTo(BreakIterator.DONE));

        assertThat(bi.last(), equalTo(source.length()));
        int current = bi.current();
        assertThat(source.substring(bi.previous(), current), equalTo("sentence"));
        current = bi.current();
        assertThat(source.substring(bi.previous(), current), equalTo("first" + separator));
        current = bi.current();
        assertThat(source.substring(bi.previous(), current), equalTo("the" + separator));
        current = bi.current();
        assertThat(source.substring(bi.previous(), current), equalTo("is" + separator));
        current = bi.current();
        assertThat(source.substring(bi.previous(), current), equalTo("this" + separator));
        assertThat(bi.previous(), equalTo(BreakIterator.DONE));
        assertThat(bi.current(), equalTo(0));

        assertThat(source.substring(0, bi.following(9)), equalTo("this" + separator + "is" + separator + "the" + separator));

        assertThat(source.substring(0, bi.preceding(9)), equalTo("this" + separator + "is" + separator));

        assertThat(bi.first(), equalTo(0));
        assertThat(source.substring(0, bi.next(3)), equalTo("this" + separator + "is" + separator + "the" + separator));
    }

    @Test
    public void testSingleSentences() throws Exception {
        BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
        BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
        assertSameBreaks("a", expected, actual);
        assertSameBreaks("ab", expected, actual);
        assertSameBreaks("abc", expected, actual);
        assertSameBreaks("", expected, actual);
    }

    @Test
    public void testSliceEnd() throws Exception {
        BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
        BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
        assertSameBreaks("a000", 0, 1, expected, actual);
        assertSameBreaks("ab000", 0, 1, expected, actual);
        assertSameBreaks("abc000", 0, 1, expected, actual);
        assertSameBreaks("000", 0, 0, expected, actual);
    }

    @Test
    public void testSliceStart() throws Exception {
        BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
        BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
        assertSameBreaks("000a", 3, 1, expected, actual);
        assertSameBreaks("000ab", 3, 2, expected, actual);
        assertSameBreaks("000abc", 3, 3, expected, actual);
        assertSameBreaks("000", 3, 0, expected, actual);
    }

    @Test
    public void testSliceMiddle() throws Exception {
        BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
        BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
        assertSameBreaks("000a000", 3, 1, expected, actual);
        assertSameBreaks("000ab000", 3, 2, expected, actual);
        assertSameBreaks("000abc000", 3, 3, expected, actual);
        assertSameBreaks("000000", 3, 0, expected, actual);
    }

    /** the current position must be ignored, initial position is always first() */
    @Test
    public void testFirstPosition() throws Exception {
        BreakIterator expected = BreakIterator.getSentenceInstance(Locale.ROOT);
        BreakIterator actual = new CustomSeparatorBreakIterator(randomSeparator());
        assertSameBreaks("000ab000", 3, 2, 4, expected, actual);
    }

    private static char randomSeparator() {
        return randomFrom(' ', HighlightUtils.NULL_SEPARATOR, HighlightUtils.PARAGRAPH_SEPARATOR);
    }

    private static void assertSameBreaks(String text, BreakIterator expected, BreakIterator actual) {
        assertSameBreaks(new StringCharacterIterator(text),
                new StringCharacterIterator(text),
                expected,
                actual);
    }

    private static void assertSameBreaks(String text, int offset, int length, BreakIterator expected, BreakIterator actual) {
        assertSameBreaks(text, offset, length, offset, expected, actual);
    }

    private static void assertSameBreaks(String text, int offset, int length, int current, BreakIterator expected, BreakIterator actual) {
        assertSameBreaks(new StringCharacterIterator(text, offset, offset + length, current),
                new StringCharacterIterator(text, offset, offset + length, current),
                expected,
                actual);
    }

    /** Asserts that two breakiterators break the text the same way */
    private static void assertSameBreaks(CharacterIterator one, CharacterIterator two, BreakIterator expected, BreakIterator actual) {
        expected.setText(one);
        actual.setText(two);

        assertEquals(expected.current(), actual.current());

        // next()
        int v = expected.current();
        while (v != BreakIterator.DONE) {
            assertEquals(v = expected.next(), actual.next());
            assertEquals(expected.current(), actual.current());
        }

        // first()
        assertEquals(expected.first(), actual.first());
        assertEquals(expected.current(), actual.current());
        // last()
        assertEquals(expected.last(), actual.last());
        assertEquals(expected.current(), actual.current());

        // previous()
        v = expected.current();
        while (v != BreakIterator.DONE) {
            assertEquals(v = expected.previous(), actual.previous());
            assertEquals(expected.current(), actual.current());
        }

        // following()
        for (int i = one.getBeginIndex(); i <= one.getEndIndex(); i++) {
            expected.first();
            actual.first();
            assertEquals(expected.following(i), actual.following(i));
            assertEquals(expected.current(), actual.current());
        }

        // preceding()
        for (int i = one.getBeginIndex(); i <= one.getEndIndex(); i++) {
            expected.last();
            actual.last();
            assertEquals(expected.preceding(i), actual.preceding(i));
            assertEquals(expected.current(), actual.current());
        }
    }
}
