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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.util.CharArrayIterator;
import org.elasticsearch.common.io.FastCharArrayReader;

import java.io.IOException;
import java.io.Reader;
import java.text.BreakIterator;
import java.util.Locale;

public class SentenceCharFilter extends CharFilter {
    public static CharFilter build(Reader input, Locale locale, int sentences, int analyzedChars) throws IOException {
        char[] cbuf = new char[analyzedChars];
        final int limit = input.read(cbuf);
        CharArrayIterator charIterator = CharArrayIterator.newSentenceInstance();
        charIterator.setText(cbuf, 0, limit);

        // Input was empty so we can't do anything with it.  Wrapping it in a SentenceCharfilter will turn it into an
        // empty CharFilter.
        if (limit < 0) {
            return new SentenceCharFilter(input);
        }

        BreakIterator iterator = BreakIterator.getSentenceInstance(locale);
        iterator.setText(charIterator);
        iterator.first();
        int boundary = iterator.next(sentences);
        if (boundary == BreakIterator.DONE) {
            // We didn't analyze enough chars to find the sentences so just return what we can.
            boundary = limit;
        }
        if (boundary != analyzedChars) {
            // Sentences inside the limit so return the boundary.  Success.
            return new SentenceCharFilter(new FastCharArrayReader(cbuf, 0, boundary));
        }

        // Sentences not inside the limit so break on the last word in case we chopped a word.
        charIterator = CharArrayIterator.newWordInstance();
        charIterator.setText(cbuf, 0, limit);
        iterator = BreakIterator.getWordInstance(locale);
        iterator.setText(charIterator);
        iterator.last();
        boundary = iterator.previous();
        // Note that the boundary could be 0 if there are not word breaks in the text.  That is OK because we'd prefer
        // nothing over a broken word.
        return new SentenceCharFilter(new FastCharArrayReader(cbuf, 0, boundary));
    }

    private SentenceCharFilter(Reader input) {
        super(input);
    }

    @Override
    protected int correct(int currentOff) {
        return currentOff;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        return input.read(cbuf, off, len);
    }
}
