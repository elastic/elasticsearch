/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import com.ibm.icu.text.BreakIterator;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.elasticsearch.xpack.inference.chunking.WordBoundaryChunkerTests.TEST_TEXT;

public class ChunkerUtilsTests extends ESTestCase {
    public void testCountWords() {
        // Test word count matches the whitespace separated word count.
        var splitByWhiteSpaceSentenceSizes = sentenceSizes(TEST_TEXT);

        var sentenceIterator = BreakIterator.getSentenceInstance(Locale.ROOT);
        sentenceIterator.setText(TEST_TEXT);

        var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
        wordIterator.setText(TEST_TEXT);

        int start = 0;
        int end = sentenceIterator.next();
        assertEquals(splitByWhiteSpaceSentenceSizes[0], ChunkerUtils.countWords(start, end, wordIterator));
        start = end;
        end = sentenceIterator.next();
        assertEquals(splitByWhiteSpaceSentenceSizes[1], ChunkerUtils.countWords(start, end, wordIterator));
        start = end;
        end = sentenceIterator.next();
        assertEquals(splitByWhiteSpaceSentenceSizes[2], ChunkerUtils.countWords(start, end, wordIterator));
        start = end;
        end = sentenceIterator.next();
        assertEquals(splitByWhiteSpaceSentenceSizes[3], ChunkerUtils.countWords(start, end, wordIterator));

        assertEquals(BreakIterator.DONE, sentenceIterator.next());
    }

    public void testCountWords_short() {
        // Test word count matches the whitespace separated word count.
        var text = "This is a short sentence. Followed by another.";

        var sentenceIterator = BreakIterator.getSentenceInstance(Locale.ROOT);
        sentenceIterator.setText(text);

        var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
        wordIterator.setText(text);

        int start = 0;
        int end = sentenceIterator.next();
        assertEquals(5, ChunkerUtils.countWords(0, end, wordIterator));
        start = end;
        end = sentenceIterator.next();
        assertEquals(3, ChunkerUtils.countWords(start, end, wordIterator));
        assertEquals(BreakIterator.DONE, sentenceIterator.next());
    }

    public void testCountWords_WithSymbols() {
        {
            var text = "foo != bar";
            var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
            wordIterator.setText(text);
            // "foo", "bar" - "!=" is not counted
            assertEquals(2, ChunkerUtils.countWords(0, text.length(), wordIterator));
        }
        {
            var text = "foo & bar";
            var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
            wordIterator.setText(text);
            // "foo", "bar" - the & is not counted
            assertEquals(2, ChunkerUtils.countWords(0, text.length(), wordIterator));
        }
        {
            var text = "m&s";
            var wordIterator = BreakIterator.getWordInstance(Locale.ROOT);
            wordIterator.setText(text);
            // "m", "s" - the & is not counted
            assertEquals(2, ChunkerUtils.countWords(0, text.length(), wordIterator));
        }
    }

    private int[] sentenceSizes(String text) {
        var sentences = text.split("\\.\\s+");
        var lengths = new int[sentences.length];
        for (int i = 0; i < sentences.length; i++) {
            // strip out the '=' signs as they are not counted as words by ICU
            sentences[i] = sentences[i].replace("=", "");
            // split by hyphen or whitespace to match the way
            // the ICU break iterator counts words
            lengths[i] = sentences[i].split("[ \\-]+").length;
        }
        return lengths;
    }
}
