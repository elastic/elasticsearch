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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.all.AllTermQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
    private final String fieldValue;
    private final PassageFormatter passageFormatter;
    private final BreakIterator breakIterator;
    private final Locale breakIteratorLocale;
    private final int noMatchSize;

    /**
     * Creates a new instance of {@link CustomUnifiedHighlighter}
     *
     * @param analyzer the analyzer used for the field at index time, used for multi term queries internally.
     * @param passageFormatter our own {@link CustomPassageFormatter}
     *                    which generates snippets in forms of {@link Snippet} objects.
     * @param offsetSource the {@link OffsetSource} to used for offsets retrieval.
     * @param breakIteratorLocale the {@link Locale} to use for dividing text into passages.
     *                    If null {@link Locale#ROOT} is used.
     * @param breakIterator the {@link BreakIterator} to use for dividing text into passages.
     *                    If null {@link BreakIterator#getSentenceInstance(Locale)} is used.
     * @param fieldValue the original field values delimited by MULTIVAL_SEP_CHAR.
     * @param noMatchSize The size of the text that should be returned when no highlighting can be performed.
     */
    public CustomUnifiedHighlighter(IndexSearcher searcher,
                                    Analyzer analyzer,
                                    OffsetSource offsetSource,
                                    PassageFormatter passageFormatter,
                                    @Nullable Locale breakIteratorLocale,
                                    @Nullable BreakIterator breakIterator,
                                    String fieldValue,
                                    int noMatchSize) {
        super(searcher, analyzer);
        this.offsetSource = offsetSource;
        this.breakIterator = breakIterator;
        this.breakIteratorLocale = breakIteratorLocale == null ? Locale.ROOT : breakIteratorLocale;
        this.passageFormatter = passageFormatter;
        this.fieldValue = fieldValue;
        this.noMatchSize = noMatchSize;
    }

    /**
     * Highlights terms extracted from the provided query within the content of the provided field name
     */
    public Snippet[] highlightField(String field, Query query, int docId, int maxPassages) throws IOException {
        Map<String, Object[]> fieldsAsObjects = super.highlightFieldsAsObjects(new String[]{field}, query,
            new int[]{docId}, new int[]{maxPassages});
        Object[] snippetObjects = fieldsAsObjects.get(field);
        if (snippetObjects != null) {
            //one single document at a time
            assert snippetObjects.length == 1;
            Object snippetObject = snippetObjects[0];
            if (snippetObject != null && snippetObject instanceof Snippet[]) {
                return (Snippet[]) snippetObject;
            }
        }
        return EMPTY_SNIPPET;
    }

    @Override
    protected List<CharSequence[]> loadFieldValues(String[] fields, DocIdSetIterator docIter,
                                                   int cacheCharsThreshold) throws IOException {
        // we only highlight one field, one document at a time
        return Collections.singletonList(new String[]{fieldValue});
    }

    @Override
    protected BreakIterator getBreakIterator(String field) {
        return breakIterator;
    }

    @Override
    protected PassageFormatter getFormatter(String field) {
        return passageFormatter;
    }

    @Override
    protected FieldHighlighter getFieldHighlighter(String field, Query query, Set<Term> allTerms, int maxPassages) {
        BytesRef[] terms = filterExtractedTerms(getFieldMatcher(field), allTerms);
        Set<HighlightFlag> highlightFlags = getFlags(field);
        PhraseHelper phraseHelper = getPhraseHelper(field, query, highlightFlags);
        CharacterRunAutomaton[] automata = getAutomata(field, query, highlightFlags);
        OffsetSource offsetSource = getOptimizedOffsetSource(field, terms, phraseHelper, automata);
        BreakIterator breakIterator = new SplittingBreakIterator(getBreakIterator(field),
            UnifiedHighlighter.MULTIVAL_SEP_CHAR);
        FieldOffsetStrategy strategy =
            getOffsetStrategy(offsetSource, field, terms, phraseHelper, automata, highlightFlags);
        return new CustomFieldHighlighter(field, strategy, breakIteratorLocale, breakIterator,
            getScorer(field), maxPassages, (noMatchSize > 0 ? 1 : 0), getFormatter(field), noMatchSize, fieldValue);
    }

    @Override
    protected Collection<Query> preMultiTermQueryRewrite(Query query) {
        return rewriteCustomQuery(query);
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
                        innerQueries[j] = new SpanMultiTermQueryWrapper(new PrefixQuery(terms[i][j]));
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
        } else if (query instanceof CommonTermsQuery) {
            CommonTermsQuery ctq = (CommonTermsQuery) query;
            List<Query> tqs = new ArrayList<> ();
            for (Term term : ctq.getTerms()) {
                tqs.add(new TermQuery(term));
            }
            return tqs;
        } else if (query instanceof AllTermQuery) {
            AllTermQuery atq = (AllTermQuery) query;
            return Collections.singletonList(new TermQuery(atq.getTerm()));
        } else if (query instanceof FunctionScoreQuery) {
            return Collections.singletonList(((FunctionScoreQuery) query).getSubQuery());
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
