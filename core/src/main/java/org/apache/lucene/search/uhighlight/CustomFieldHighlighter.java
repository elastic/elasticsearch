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

    public CustomFieldHighlighter(String field, FieldOffsetStrategy fieldOffsetStrategy,
                                  Locale breakIteratorLocale, BreakIterator breakIterator,
                                  PassageScorer passageScorer, int maxPassages, int maxNoHighlightPassages,
                                  PassageFormatter passageFormatter, int noMatchSize, String fieldValue) {
        super(field, fieldOffsetStrategy, breakIterator, passageScorer, maxPassages, maxNoHighlightPassages, passageFormatter);
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
                if (noMatchSize < end) {
                    BreakIterator bi = BreakIterator.getWordInstance(breakIteratorLocale);
                    bi.setText(fieldValue);
                    // Finds the next word boundary **after** noMatchSize.
                    end = bi.following(noMatchSize);
                    if (end == BreakIterator.DONE) {
                        end = fieldValue.length();
                    }
                }
                Passage passage = new Passage();
                passage.setScore(Float.NaN);
                passage.setStartOffset(0);
                passage.setEndOffset(end);
                return new Passage[]{passage};
            }
        }
        return EMPTY_PASSAGE;
    }
}
