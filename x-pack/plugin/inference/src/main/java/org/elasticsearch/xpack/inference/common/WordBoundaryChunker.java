/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

//import java.text.BreakIterator;
import com.ibm.icu.text.BreakIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class WordBoundaryChunker {

    private BreakIterator wordIterator;

    public WordBoundaryChunker() {
        wordIterator = BreakIterator.getWordInstance(Locale.ROOT);

    }

    public List<String> chunk(String input, int chunkSize, int overlap) {
        if (overlap >= chunkSize) {
            throw new IllegalArgumentException(
                "Invalid chunking parameters, overlap [" + overlap + "] must be < window size [" + chunkSize + "]"
            );
        }
        wordIterator.setText(input);

        var chunks = new ArrayList<String>();

        int boundary = wordIterator.current();
        final int notOverlappingSize = chunkSize - overlap;
        int windowStart = 0;

        int wordCount = 0;
        while (boundary != BreakIterator.DONE) {
            if (wordCount >= chunkSize) {
                chunks.add(input.substring(0, boundary));
                wordCount = 0;
                break;
            }

            boundary = wordIterator.next();
            if (wordIterator.getRuleStatus() != BreakIterator.WORD_NONE) {
                wordCount++;
            }

            if (wordCount == notOverlappingSize) {
                windowStart = boundary;
            }
        }

        int nextWindowStart = 0;
        while (boundary != BreakIterator.DONE) {
            if (wordCount >= notOverlappingSize) {
                chunks.add(input.substring(windowStart, boundary));
                wordCount = 0;
                windowStart = nextWindowStart;
            }

            boundary = wordIterator.next();
            if (wordIterator.getRuleStatus() != BreakIterator.WORD_NONE) {
                wordCount++;
            }

            if (wordCount == (notOverlappingSize - overlap)) {
                nextWindowStart = boundary;
            }
        }

        chunks.add(input.substring(windowStart));
        return chunks;
    }
}
