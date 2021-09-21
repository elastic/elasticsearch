/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.lucene.search.uhighlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.search.fetch.subphase.highlight.AbstractHighlighterBuilder.MAX_ANALYZED_OFFSET_FIELD;

/**
 * Subclass of the {@link UnifiedHighlighter} that works for a single field in a single document.
 * Uses a custom {@link PassageFormatter}. Accepts field content as a constructor
 * argument, given that loadings field value can be done reading from _source field.
 * Supports using different {@link BreakIterator} to break the text into fragments. Considers every distinct field
 * value as a discrete passage for highlighting (unless the whole content needs to be highlighted).
 * Supports both returning empty snippets and non highlighted snippets when no highlighting can be performed.
 */
public class CustomUnifiedHighlighter extends UnifiedHighlighter {
    public static final char MULTIVAL_SEP_CHAR = (char) 0;
    private static final Snippet[] EMPTY_SNIPPET = new Snippet[0];

    private final OffsetSource offsetSource;
    private final PassageFormatter passageFormatter;
    private final BreakIterator breakIterator;
    private final String index;
    private final String field;
    private final Locale breakIteratorLocale;
    private final int noMatchSize;
    private final FieldHighlighter fieldHighlighter;
    private final int maxAnalyzedOffset;
    private final Integer queryMaxAnalyzedOffset;

    /**
     * Creates a new instance of {@link CustomUnifiedHighlighter}
     *
     * @param analyzer the analyzer used for the field at index time, used for multi term queries internally.
     * @param offsetSource the {@link OffsetSource} to used for offsets retrieval.
     * @param passageFormatter our own {@link CustomPassageFormatter}
     *                    which generates snippets in forms of {@link Snippet} objects.
     * @param breakIteratorLocale the {@link Locale} to use for dividing text into passages.
     *                    If null {@link Locale#ROOT} is used.
     * @param breakIterator the {@link BreakIterator} to use for dividing text into passages.
     *                    If null {@link BreakIterator#getSentenceInstance(Locale)} is used.
     * @param index the index we're highlighting, mostly used for error messages
     * @param field the name of the field we're highlighting
     * @param query the query we're highlighting
     * @param noMatchSize The size of the text that should be returned when no highlighting can be performed.
     * @param maxPassages the maximum number of passes to highlight
     * @param fieldMatcher decides which terms should be highlighted
     * @param maxAnalyzedOffset if the field is more than this long we'll refuse to use the ANALYZED
     *                          offset source for it because it'd be super slow
     */
    public CustomUnifiedHighlighter(IndexSearcher searcher,
                                    Analyzer analyzer,
                                    OffsetSource offsetSource,
                                    PassageFormatter passageFormatter,
                                    @Nullable Locale breakIteratorLocale,
                                    @Nullable BreakIterator breakIterator,
                                    String index, String field, Query query,
                                    int noMatchSize,
                                    int maxPassages,
                                    Predicate<String> fieldMatcher,
                                    int maxAnalyzedOffset,
                                    Integer queryMaxAnalyzedOffset) throws IOException {
        super(searcher, analyzer);
        this.offsetSource = offsetSource;
        this.breakIterator = breakIterator;
        this.breakIteratorLocale = breakIteratorLocale == null ? Locale.ROOT : breakIteratorLocale;
        this.passageFormatter = passageFormatter;
        this.index = index;
        this.field = field;
        this.noMatchSize = noMatchSize;
        this.setFieldMatcher(fieldMatcher);
        this.maxAnalyzedOffset = maxAnalyzedOffset;
        this.queryMaxAnalyzedOffset = queryMaxAnalyzedOffset;
        fieldHighlighter = getFieldHighlighter(field, query, extractTerms(query), maxPassages);
    }

    /**
     * Highlights the field value.
     */
    public Snippet[] highlightField(LeafReader reader, int docId, CheckedSupplier<String, IOException> loadFieldValue) throws IOException {
        if (fieldHighlighter.fieldOffsetStrategy == NoOpOffsetStrategy.INSTANCE && noMatchSize == 0) {
            // If the query is such that there can't possibly be any matches then skip doing *everything*
            return EMPTY_SNIPPET;
        }
        String fieldValue = loadFieldValue.get();
        if (fieldValue == null) {
            return null;
        }
        int fieldValueLength = fieldValue.length();
        if (((queryMaxAnalyzedOffset == null || queryMaxAnalyzedOffset > maxAnalyzedOffset) &&
                (offsetSource == OffsetSource.ANALYSIS) && (fieldValueLength > maxAnalyzedOffset))) {
            throw new IllegalArgumentException(
                "The length [" + fieldValueLength + "] of field [" + field +"] in doc[" + docId + "]/index[" + index +"] exceeds the ["
                    + IndexSettings.MAX_ANALYZED_OFFSET_SETTING.getKey() + "] limit [" + maxAnalyzedOffset + "]. To avoid this error, set "
                    + "the query parameter [" + MAX_ANALYZED_OFFSET_FIELD.toString() + "] to a value less than index setting ["
                    + maxAnalyzedOffset + "] and this will tolerate long field values by truncating them."
            );
        }
        Snippet[] result = (Snippet[]) fieldHighlighter.highlightFieldForDoc(reader, docId, fieldValue);
        return result == null ? EMPTY_SNIPPET : result;
    }

    @Override
    protected BreakIterator getBreakIterator(String field) {
        return breakIterator;
    }

    public PassageFormatter getFormatter() {
        return passageFormatter;
    }

    @Override
    protected PassageFormatter getFormatter(String field) {
        return passageFormatter;
    }

    @Override
    protected FieldHighlighter getFieldHighlighter(String field, Query query, Set<Term> allTerms, int maxPassages) {
        Predicate<String> fieldMatcher = getFieldMatcher(field);
        BytesRef[] terms = filterExtractedTerms(fieldMatcher, allTerms);
        Set<HighlightFlag> highlightFlags = getFlags(field);
        PhraseHelper phraseHelper = getPhraseHelper(field, query, highlightFlags);
        LabelledCharArrayMatcher[] automata = getAutomata(field, query, highlightFlags);
        UHComponents components = new UHComponents(field, fieldMatcher, query, terms, phraseHelper, automata, false , highlightFlags);
        OffsetSource offsetSource = getOptimizedOffsetSource(components);
        BreakIterator breakIterator = new SplittingBreakIterator(getBreakIterator(field),
            UnifiedHighlighter.MULTIVAL_SEP_CHAR);
        FieldOffsetStrategy strategy = getOffsetStrategy(offsetSource, components);
        return new CustomFieldHighlighter(field, strategy, breakIteratorLocale, breakIterator,
            getScorer(field), maxPassages, (noMatchSize > 0 ? 1 : 0), getFormatter(field), noMatchSize);
    }

    @Override
    protected Collection<Query> preSpanQueryRewrite(Query query) {
        return rewriteCustomQuery(query);
    }

    /**
     * Translate custom queries in queries that are supported by the unified highlighter.
     */
    private Collection<Query> rewriteCustomQuery(Query query) {
        if (query instanceof MultiPhrasePrefixQuery) {
            MultiPhrasePrefixQuery mpq = (MultiPhrasePrefixQuery) query;
            Term[][] terms = mpq.getTerms();
            int[] positions = mpq.getPositions();
            SpanQuery[] positionSpanQueries = new SpanQuery[positions.length];
            int sizeMinus1 = terms.length - 1;
            for (int i = 0; i < positions.length; i++) {
                SpanQuery[] innerQueries = new SpanQuery[terms[i].length];
                for (int j = 0; j < terms[i].length; j++) {
                    if (i == sizeMinus1) {
                        innerQueries[j] = new SpanMultiTermQueryWrapper<>(new PrefixQuery(terms[i][j]));
                    } else {
                        innerQueries[j] = new SpanTermQuery(terms[i][j]);
                    }
                }
                if (innerQueries.length > 1) {
                    positionSpanQueries[i] = new SpanOrQuery(innerQueries);
                } else {
                    positionSpanQueries[i] = innerQueries[0];
                }
            }

            if (positionSpanQueries.length == 1) {
                return Collections.singletonList(positionSpanQueries[0]);
            }
            // sum position increments beyond 1
            int positionGaps = 0;
            if (positions.length >= 2) {
                // positions are in increasing order.   max(0,...) is just a safeguard.
                positionGaps = Math.max(0, positions[positions.length - 1] - positions[0] - positions.length + 1);
            }
            //if original slop is 0 then require inOrder
            boolean inorder = (mpq.getSlop() == 0);
            return Collections.singletonList(new SpanNearQuery(positionSpanQueries,
                mpq.getSlop() + positionGaps, inorder));
        } else if (query instanceof ESToParentBlockJoinQuery) {
            return Collections.singletonList(((ESToParentBlockJoinQuery) query).getChildQuery());
        } else {
            return null;
        }
    }

    /**
     * Forces the offset source for this highlighter
     */
    @Override
    protected OffsetSource getOffsetSource(String field) {
        if (offsetSource == null) {
            return super.getOffsetSource(field);
        }
        return offsetSource;
    }
}
