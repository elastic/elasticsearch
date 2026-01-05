/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.uhighlight;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.uhighlight.FieldHighlighter;
import org.apache.lucene.search.uhighlight.FieldOffsetStrategy;
import org.apache.lucene.search.uhighlight.OffsetsEnum;
import org.apache.lucene.search.uhighlight.Passage;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.PassageScorer;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Comparator;
import java.util.Locale;

import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;

/**
 * Custom {@link FieldHighlighter} that creates a single passage bounded to {@code noMatchSize} when
 * no highlights were found.
 */
class CustomFieldHighlighter extends FieldHighlighter {
    private static final Passage[] EMPTY_PASSAGE = new Passage[0];

    private final Locale breakIteratorLocale;
    private final int noMatchSize;
    private String fieldValue;
    private final QueryMaxAnalyzedOffset queryMaxAnalyzedOffset;

    CustomFieldHighlighter(
        String field,
        FieldOffsetStrategy fieldOffsetStrategy,
        Locale breakIteratorLocale,
        BreakIterator breakIterator,
        PassageScorer passageScorer,
        int maxPassages,
        int maxNoHighlightPassages,
        PassageFormatter passageFormatter,
        Comparator<Passage> passageSortComparator,
        int noMatchSize,
        QueryMaxAnalyzedOffset queryMaxAnalyzedOffset
    ) {
        super(
            field,
            fieldOffsetStrategy,
            breakIterator,
            passageScorer,
            maxPassages,
            maxNoHighlightPassages,
            passageFormatter,
            passageSortComparator
        );
        this.breakIteratorLocale = breakIteratorLocale;
        this.noMatchSize = noMatchSize;
        this.queryMaxAnalyzedOffset = queryMaxAnalyzedOffset;
    }

    FieldOffsetStrategy getFieldOffsetStrategy() {
        return fieldOffsetStrategy;
    }

    @Override
    public Object highlightFieldForDoc(LeafReader reader, int docId, String content) throws IOException {
        this.fieldValue = content;
        try {
            return super.highlightFieldForDoc(reader, docId, content);
        } finally {
            // Clear the reference to the field value in case it is large
            fieldValue = null;
        }
    }

    @Override
    protected Passage[] getSummaryPassagesNoHighlight(int maxPassages) {
        if (noMatchSize > 0) {
            int pos = 0;
            while (pos < fieldValue.length() && fieldValue.charAt(pos) == MULTIVAL_SEP_CHAR) {
                pos++;
            }
            if (pos < fieldValue.length()) {
                int end = fieldValue.indexOf(MULTIVAL_SEP_CHAR, pos);
                if (end == -1) {
                    end = fieldValue.length();
                }
                if (noMatchSize + pos < end) {
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
                return new Passage[] { passage };
            }
        }
        return EMPTY_PASSAGE;
    }

    @Override
    protected Passage[] highlightOffsetsEnums(OffsetsEnum off) throws IOException {
        if (queryMaxAnalyzedOffset != null) {
            off = new LimitedOffsetsEnum(off, queryMaxAnalyzedOffset.getNotNull());
        }
        return super.highlightOffsetsEnums(off);
    }
}
