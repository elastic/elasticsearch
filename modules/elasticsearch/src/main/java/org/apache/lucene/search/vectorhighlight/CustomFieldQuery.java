/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.lucene.search.vectorhighlight;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;

/**
 * @author kimchy (shay.banon)
 */
// LUCENE MONITOR
public class CustomFieldQuery extends FieldQuery {

    private static Field multiTermQueryWrapperFilterQueryField;

    static {
        try {
            multiTermQueryWrapperFilterQueryField = MultiTermQueryWrapperFilter.class.getDeclaredField("query");
            multiTermQueryWrapperFilterQueryField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            // ignore
        }
    }

    // hack since flatten is called from the parent constructor, so we can't pass it
    public static final ThreadLocal<IndexReader> reader = new ThreadLocal<IndexReader>();

    public static final ThreadLocal<Boolean> highlightFilters = new ThreadLocal<Boolean>();

    public CustomFieldQuery(Query query, FastVectorHighlighter highlighter) {
        this(query, highlighter.isPhraseHighlight(), highlighter.isFieldMatch());
    }

    public CustomFieldQuery(Query query, boolean phraseHighlight, boolean fieldMatch) {
        super(query, phraseHighlight, fieldMatch);
        reader.remove();
        highlightFilters.remove();
    }

    @Override void flatten(Query sourceQuery, Collection<Query> flatQueries) {
        if (sourceQuery instanceof DisjunctionMaxQuery) {
            DisjunctionMaxQuery dmq = (DisjunctionMaxQuery) sourceQuery;
            for (Query query : dmq) {
                flatten(query, flatQueries);
            }
        } else if (sourceQuery instanceof SpanTermQuery) {
            TermQuery termQuery = new TermQuery(((SpanTermQuery) sourceQuery).getTerm());
            if (!flatQueries.contains(termQuery)) {
                flatQueries.add(termQuery);
            }
        } else if (sourceQuery instanceof ConstantScoreQuery) {
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) sourceQuery;
            if (constantScoreQuery.getFilter() != null) {
                flatten(constantScoreQuery.getFilter(), flatQueries);
            } else {
                flatten(constantScoreQuery.getQuery(), flatQueries);
            }
        } else if (sourceQuery instanceof DeletionAwareConstantScoreQuery) {
            flatten(((DeletionAwareConstantScoreQuery) sourceQuery).getFilter(), flatQueries);
        } else if (sourceQuery instanceof FunctionScoreQuery) {
            flatten(((FunctionScoreQuery) sourceQuery).getSubQuery(), flatQueries);
        } else if (sourceQuery instanceof MultiTermQuery) {
            MultiTermQuery multiTermQuery = (MultiTermQuery) sourceQuery;
            MultiTermQuery.RewriteMethod rewriteMethod = multiTermQuery.getRewriteMethod();
            // we want to rewrite a multi term query to extract the terms out of it
            // LUCENE MONITOR: The regular Highlighter actually uses MemoryIndex to extract the terms
            multiTermQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
            try {
                flatten(multiTermQuery.rewrite(reader.get()), flatQueries);
            } catch (IOException e) {
                // ignore
            } catch (BooleanQuery.TooManyClauses e) {
                // ignore
            } finally {
                multiTermQuery.setRewriteMethod(rewriteMethod);
            }
        } else if (sourceQuery instanceof FilteredQuery) {
            flatten(((FilteredQuery) sourceQuery).getQuery(), flatQueries);
            flatten(((FilteredQuery) sourceQuery).getFilter(), flatQueries);
        } else {
            super.flatten(sourceQuery, flatQueries);
        }
    }

    void flatten(Filter sourceFilter, Collection<Query> flatQueries) {
        Boolean highlight = highlightFilters.get();
        if (highlight == null || highlight.equals(Boolean.FALSE)) {
            return;
        }
        if (sourceFilter instanceof TermFilter) {
            flatten(new TermQuery(((TermFilter) sourceFilter).getTerm()), flatQueries);
        } else if (sourceFilter instanceof PublicTermsFilter) {
            PublicTermsFilter termsFilter = (PublicTermsFilter) sourceFilter;
            for (Term term : termsFilter.getTerms()) {
                flatten(new TermQuery(term), flatQueries);
            }
        } else if (sourceFilter instanceof MultiTermQueryWrapperFilter) {
            if (multiTermQueryWrapperFilterQueryField != null) {
                try {
                    flatten((Query) multiTermQueryWrapperFilterQueryField.get(sourceFilter), flatQueries);
                } catch (IllegalAccessException e) {
                    // ignore
                }
            }
        } else if (sourceFilter instanceof XBooleanFilter) {
            XBooleanFilter booleanFilter = (XBooleanFilter) sourceFilter;
            for (Filter filter : booleanFilter.getMustFilters()) {
                flatten(filter, flatQueries);
            }
            for (Filter filter : booleanFilter.getNotFilters()) {
                flatten(filter, flatQueries);
            }
        }
    }
}
