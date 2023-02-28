/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.search.uhighlight;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.uhighlight.FieldHighlighter;
import org.apache.lucene.search.uhighlight.FieldOffsetStrategy;
import org.apache.lucene.search.uhighlight.OffsetsEnum;
import org.apache.lucene.search.uhighlight.Passage;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.PassageScorer;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.PriorityQueue;

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
    private final Integer queryMaxAnalyzedOffset;

    CustomFieldHighlighter(
        String field,
        FieldOffsetStrategy fieldOffsetStrategy,
        Locale breakIteratorLocale,
        BreakIterator breakIterator,
        PassageScorer passageScorer,
        int maxPassages,
        int maxNoHighlightPassages,
        PassageFormatter passageFormatter,
        int noMatchSize,
        Integer queryMaxAnalyzedOffset
    ) {
        super(field, fieldOffsetStrategy, breakIterator, passageScorer, maxPassages, maxNoHighlightPassages, passageFormatter);
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

    // TODO: use FieldHighlighter::highlightOffsetsEnums and modify BoundedBreakIteratorScanner to work with it
    // LUCENE-9093 modified how FieldHighlighter breaks texts into passages,
    // which doesn't work well with BoundedBreakIteratorScanner
    // This is the copy of highlightOffsetsEnums before LUCENE-9093.
    @Override
    protected Passage[] highlightOffsetsEnums(OffsetsEnum off) throws IOException {

        if (queryMaxAnalyzedOffset != null) {
            off = new LimitedOffsetsEnum(off, queryMaxAnalyzedOffset);
        }

        final int contentLength = this.breakIterator.getText().getEndIndex();

        if (off.nextPosition() == false) {
            return new Passage[0];
        }

        PriorityQueue<Passage> passageQueue = new PriorityQueue<>(Math.min(64, maxPassages + 1), (left, right) -> {
            if (left.getScore() < right.getScore()) {
                return -1;
            } else if (left.getScore() > right.getScore()) {
                return 1;
            } else {
                return left.getStartOffset() - right.getStartOffset();
            }
        });
        Passage passage = new Passage(); // the current passage in-progress. Will either get reset or added to queue.

        do {
            int start = off.startOffset();
            if (start == -1) {
                throw new IllegalArgumentException("field '" + field + "' was indexed without offsets, cannot highlight");
            }
            int end = off.endOffset();
            if (start < contentLength && end > contentLength) {
                continue;
            }
            // See if this term should be part of a new passage.
            if (start >= passage.getEndOffset()) {
                passage = maybeAddPassage(passageQueue, passageScorer, passage, contentLength);
                // if we exceed limit, we are done
                if (start >= contentLength) {
                    break;
                }
                passage.setStartOffset(Math.max(this.breakIterator.preceding(start + 1), 0));
                passage.setEndOffset(Math.min(this.breakIterator.following(start), contentLength));
            }
            // Add this term to the passage.
            BytesRef term = off.getTerm();// a reference; safe to refer to
            assert term != null;
            passage.addMatch(start, end, term, off.freq());
        } while (off.nextPosition());
        maybeAddPassage(passageQueue, passageScorer, passage, contentLength);

        Passage[] passages = passageQueue.toArray(new Passage[passageQueue.size()]);
        // sort in ascending order
        Arrays.sort(passages, Comparator.comparingInt(Passage::getStartOffset));
        return passages;
    }

    // TODO: use FieldHighlighter::maybeAddPassage
    // After removing CustomFieldHighlighter::highlightOffsetsEnums, remove this method as well.
    private Passage maybeAddPassage(PriorityQueue<Passage> passageQueue, PassageScorer scorer, Passage passage, int contentLength) {
        if (passage.getStartOffset() == -1) {
            // empty passage, we can ignore it
            return passage;
        }
        passage.setScore(scorer.score(passage, contentLength));
        // new sentence: first add 'passage' to queue
        if (passageQueue.size() == maxPassages && passage.getScore() < passageQueue.peek().getScore()) {
            passage.reset(); // can't compete, just reset it
        } else {
            passageQueue.offer(passage);
            if (passageQueue.size() > maxPassages) {
                passage = passageQueue.poll();
                passage.reset();
            } else {
                passage = new Passage();
            }
        }
        return passage;
    }
}
