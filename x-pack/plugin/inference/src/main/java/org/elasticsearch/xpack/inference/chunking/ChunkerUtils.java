/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import com.ibm.icu.text.BreakIterator;

public class ChunkerUtils {

    // setText() should be applied before using this function.
    static int countWords(int start, int end, BreakIterator wordIterator) {
        assert start < end;
        wordIterator.preceding(start); // start of the current word

        int boundary = wordIterator.current();
        int wordCount = 0;
        while (boundary != BreakIterator.DONE && boundary <= end) {
            int wordStatus = wordIterator.getRuleStatus();
            if (wordStatus != BreakIterator.WORD_NONE) {
                wordCount++;
            }
            boundary = wordIterator.next();
        }

        return wordCount;
    }
}
