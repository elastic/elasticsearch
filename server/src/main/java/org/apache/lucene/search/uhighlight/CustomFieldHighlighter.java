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

package org.apache.lucene.search.uhighlight;

import java.text.BreakIterator;
import java.util.Locale;

import static org.apache.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;

/**
 * Custom {@link FieldHighlighter} that creates a single passage bounded to {@code noMatchSize} when
 * no highlights were found.
 */
class CustomFieldHighlighter extends FieldHighlighter {
    private static final Passage[] EMPTY_PASSAGE = new Passage[0];

    private final Locale breakIteratorLocale;
    private final int noMatchSize;
    private final String fieldValue;

    CustomFieldHighlighter(String field, FieldOffsetStrategy fieldOffsetStrategy,
                           Locale breakIteratorLocale, BreakIterator breakIterator,
                           PassageScorer passageScorer, int maxPassages, int maxNoHighlightPassages,
                           PassageFormatter passageFormatter, int noMatchSize, String fieldValue) {
        super(field, fieldOffsetStrategy, breakIterator, passageScorer, maxPassages,
            maxNoHighlightPassages, passageFormatter);
        this.breakIteratorLocale = breakIteratorLocale;
        this.noMatchSize = noMatchSize;
        this.fieldValue = fieldValue;
    }

    @Override
    protected Passage[] getSummaryPassagesNoHighlight(int maxPassages) {
        if (noMatchSize > 0) {
            int pos = 0;
            while (pos < fieldValue.length() && fieldValue.charAt(pos) == MULTIVAL_SEP_CHAR) {
                pos ++;
            }
            if (pos < fieldValue.length()) {
                int end = fieldValue.indexOf(MULTIVAL_SEP_CHAR, pos);
                if (end == -1) {
                    end = fieldValue.length();
                }
                if (noMatchSize+pos < end) {
                    BreakIterator bi = BreakIterator.getWordInstance(breakIteratorLocale);
                    bi.setText(fieldValue);
                    // Finds the next word boundary **after** noMatchSize.
                    end = bi.following(noMatchSize + pos);
                    if (end == BreakIterator.DONE) {
                        end = fieldValue.length();
                    }
                }
                Passage passage = new Passage();
                passage.setScore(Float.NaN);
                passage.setStartOffset(pos);
                passage.setEndOffset(end);
                return new Passage[]{passage};
            }
        }
        return EMPTY_PASSAGE;
    }
}
