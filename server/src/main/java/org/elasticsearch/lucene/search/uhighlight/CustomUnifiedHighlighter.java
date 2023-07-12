/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.search.uhighlight;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.uhighlight.FieldHighlighter;
import org.apache.lucene.search.uhighlight.FieldOffsetStrategy;
import org.apache.lucene.search.uhighlight.NoOpOffsetStrategy;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.PassageScorer;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.search.runtime.AbstractScriptFieldQuery;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.function.Supplier;

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
    /**
     * A cluster setting to enable/disable the {@link HighlightFlag#WEIGHT_MATCHES} mode of the unified highlighter.
     */
    public static final Setting<Boolean> WEIGHT_MATCHES_MODE_ENABLE_SETTING = Setting.boolSetting(
        "search.highlight.weight_matches_mode.enabled",
        true,
        Setting.Property.NodeScope
    );

    public static final char MULTIVAL_SEP_CHAR = (char) 0;
    private static final Snippet[] EMPTY_SNIPPET = new Snippet[0];

    private final OffsetSource offsetSource;
    private final String index;
    private final String field;
    private final Locale breakIteratorLocale;
    private final int noMatchSize;
    private final CustomFieldHighlighter fieldHighlighter;
    private final int maxAnalyzedOffset;
    private final Integer queryMaxAnalyzedOffset;

    /**
     * Creates a new instance of {@link CustomUnifiedHighlighter}
     *
     * @param settings the {@link Settings} of the node.
     * @param builder the {@link UnifiedHighlighter.Builder} for the underlying highlighter.
     * @param offsetSource the {@link OffsetSource} to used for offsets retrieval.
     * @param breakIteratorLocale the {@link Locale} to use for dividing text into passages.
     *                    If null {@link Locale#ROOT} is used.
     * @param index the index we're highlighting, mostly used for error messages
     * @param field the name of the field we're highlighting
     * @param query the query we're highlighting
     * @param noMatchSize The size of the text that should be returned when no highlighting can be performed.
     * @param maxPassages the maximum number of passes to highlight
     * @param maxAnalyzedOffset if the field is more than this long we'll refuse to use the ANALYZED
     *                          offset source for it because it'd be super slow
     */
    public CustomUnifiedHighlighter(
        Settings settings,
        Builder builder,
        OffsetSource offsetSource,
        @Nullable Locale breakIteratorLocale,
        String index,
        String field,
        Query query,
        int noMatchSize,
        int maxPassages,
        int maxAnalyzedOffset,
        Integer queryMaxAnalyzedOffset,
        boolean requireFieldMatch
    ) {
        super(builder);
        this.offsetSource = offsetSource;
        this.breakIteratorLocale = breakIteratorLocale == null ? Locale.ROOT : breakIteratorLocale;
        this.index = index;
        this.field = field;
        this.noMatchSize = noMatchSize;
        this.maxAnalyzedOffset = maxAnalyzedOffset;
        this.queryMaxAnalyzedOffset = queryMaxAnalyzedOffset;
        if (WEIGHT_MATCHES_MODE_ENABLE_SETTING.get(settings) == false || requireFieldMatch == false || weightMatchesUnsupported(query)) {
            getFlags(field).remove(HighlightFlag.WEIGHT_MATCHES);
        }
        fieldHighlighter = (CustomFieldHighlighter) getFieldHighlighter(field, query, extractTerms(query), maxPassages);
    }

    /**
     * Highlights the field value.
     */
    public Snippet[] highlightField(LeafReader reader, int docId, CheckedSupplier<String, IOException> loadFieldValue) throws IOException {
        if (fieldHighlighter.getFieldOffsetStrategy() == NoOpOffsetStrategy.INSTANCE && noMatchSize == 0) {
            // If the query is such that there can't possibly be any matches then skip doing *everything*
            return EMPTY_SNIPPET;
        }
        String fieldValue = loadFieldValue.get();
        if (fieldValue == null) {
            return null;
        }
        int fieldValueLength = fieldValue.length();
        if (((queryMaxAnalyzedOffset == null || queryMaxAnalyzedOffset > maxAnalyzedOffset)
            && (getOffsetSource(field) == OffsetSource.ANALYSIS)
            && (fieldValueLength > maxAnalyzedOffset))) {
            throw new IllegalArgumentException(
                "The length ["
                    + fieldValueLength
                    + "] of field ["
                    + field
                    + "] in doc["
                    + docId
                    + "]/index["
                    + index
                    + "] exceeds the ["
                    + IndexSettings.MAX_ANALYZED_OFFSET_SETTING.getKey()
                    + "] limit ["
                    + maxAnalyzedOffset
                    + "]. To avoid this error, set "
                    + "the query parameter ["
                    + MAX_ANALYZED_OFFSET_FIELD
                    + "] to a value less than index setting ["
                    + maxAnalyzedOffset
                    + "] and this will tolerate long field values by truncating them."
            );
        }
        Snippet[] result = (Snippet[]) fieldHighlighter.highlightFieldForDoc(reader, docId, fieldValue);
        return result == null ? EMPTY_SNIPPET : result;
    }

    public PassageFormatter getFormatter() {
        return super.getFormatter(field);
    }

    @Override
    protected FieldHighlighter newFieldHighlighter(
        String field,
        FieldOffsetStrategy fieldOffsetStrategy,
        BreakIterator breakIterator,
        PassageScorer passageScorer,
        int maxPassages,
        int maxNoHighlightPassages,
        PassageFormatter passageFormatter
    ) {
        return new CustomFieldHighlighter(
            field,
            fieldOffsetStrategy,
            breakIteratorLocale,
            breakIterator,
            getScorer(field),
            maxPassages,
            (noMatchSize > 0 ? 1 : 0),
            getFormatter(field),
            noMatchSize,
            queryMaxAnalyzedOffset
        );
    }

    @Override
    protected Collection<Query> preSpanQueryRewrite(Query query) {
        if (query instanceof MultiPhrasePrefixQuery mpq) {
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
                // positions are in increasing order. max(0,...) is just a safeguard.
                positionGaps = Math.max(0, positions[positions.length - 1] - positions[0] - positions.length + 1);
            }
            // if original slop is 0 then require inOrder
            boolean inorder = (mpq.getSlop() == 0);
            return Collections.singletonList(new SpanNearQuery(positionSpanQueries, mpq.getSlop() + positionGaps, inorder));
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

    /**
     * Returns true if the provided {@link Query} is not compatible with the {@link HighlightFlag#WEIGHT_MATCHES}
     * mode of this highlighter.
     *
     * @param query The query to highlight
     */
    private boolean weightMatchesUnsupported(Query query) {
        boolean[] hasUnknownLeaf = new boolean[1];
        query.visit(new QueryVisitor() {
            @Override
            public void visitLeaf(Query leafQuery) {
                /**
                 * The parent-child query requires to load global ordinals and to access
                 * documents outside of the scope of the highlighted doc.
                 * We disable the {@link HighlightFlag#WEIGHT_MATCHES} mode in this case
                 * in order to preserve the compatibility.
                 */
                if (leafQuery.getClass().getSimpleName().equals("LateParsingQuery")) {
                    hasUnknownLeaf[0] = true;
                }
                super.visitLeaf(query);
            }

            @Override
            public void consumeTerms(Query leafQuery, Term... terms) {
                if (leafQuery instanceof AbstractScriptFieldQuery) {
                    /**
                     * Queries on runtime fields don't support the matches API.
                     */
                    hasUnknownLeaf[0] = true;
                }
                super.consumeTerms(query, terms);
            }

            @Override
            public void consumeTermsMatching(Query leafQuery, String field, Supplier<ByteRunAutomaton> automaton) {
                if (leafQuery instanceof AbstractScriptFieldQuery) {
                    /**
                     * Queries on runtime fields don't support the matches API.
                     */
                    hasUnknownLeaf[0] = true;
                }
                super.consumeTermsMatching(query, field, automaton);
            }

            @Override
            public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                /**
                 * Nested queries don't support the matches API.
                 */
                if (parent instanceof ESToParentBlockJoinQuery) {
                    hasUnknownLeaf[0] = true;
                }
                return super.getSubVisitor(occur, parent);
            }
        });
        return hasUnknownLeaf[0];
    }
}
