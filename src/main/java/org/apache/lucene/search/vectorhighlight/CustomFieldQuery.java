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

package org.apache.lucene.search.vectorhighlight;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;

/**
 *
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

    public static final ThreadLocal<Boolean> highlightFilters = new ThreadLocal<>();

    public CustomFieldQuery(Query query, IndexReader reader, FastVectorHighlighter highlighter) throws IOException {
        this(query, reader, highlighter.isPhraseHighlight(), highlighter.isFieldMatch());
    }

    public CustomFieldQuery(Query query, IndexReader reader, boolean phraseHighlight, boolean fieldMatch) throws IOException {
        super(query, reader, phraseHighlight, fieldMatch);
        highlightFilters.remove();
    }

    @Override
    void flatten(Query sourceQuery, IndexReader reader, Collection<Query> flatQueries) throws IOException {
        if (sourceQuery instanceof SpanTermQuery) {
            super.flatten(new TermQuery(((SpanTermQuery) sourceQuery).getTerm()), reader, flatQueries);
        } else if (sourceQuery instanceof ConstantScoreQuery) {
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) sourceQuery;
            if (constantScoreQuery.getFilter() != null) {
                flatten(constantScoreQuery.getFilter(), reader, flatQueries);
            } else {
                flatten(constantScoreQuery.getQuery(), reader, flatQueries);
            }
        } else if (sourceQuery instanceof FunctionScoreQuery) {
            flatten(((FunctionScoreQuery) sourceQuery).getSubQuery(), reader, flatQueries);
        } else if (sourceQuery instanceof FilteredQuery) {
            flatten(((FilteredQuery) sourceQuery).getQuery(), reader, flatQueries);
            flatten(((FilteredQuery) sourceQuery).getFilter(), reader, flatQueries);
        } else if (sourceQuery instanceof XFilteredQuery) {
            flatten(((XFilteredQuery) sourceQuery).getQuery(), reader, flatQueries);
            flatten(((XFilteredQuery) sourceQuery).getFilter(), reader, flatQueries);
        } else if (sourceQuery instanceof MultiPhrasePrefixQuery) {
            flatten(sourceQuery.rewrite(reader), reader, flatQueries);
        } else if (sourceQuery instanceof FiltersFunctionScoreQuery) {
            flatten(((FiltersFunctionScoreQuery) sourceQuery).getSubQuery(), reader, flatQueries);
        } else if (sourceQuery instanceof MultiPhraseQuery) {
            MultiPhraseQuery q = ((MultiPhraseQuery) sourceQuery);
            convertMultiPhraseQuery(0, new int[q.getTermArrays().size()], q, q.getTermArrays(), q.getPositions(), reader, flatQueries);
        } else if (sourceQuery instanceof BlendedTermQuery) {
            final BlendedTermQuery blendedTermQuery = (BlendedTermQuery) sourceQuery;
            flatten(blendedTermQuery.rewrite(reader), reader, flatQueries);
        } else {
            super.flatten(sourceQuery, reader, flatQueries);
        }
    }
    
    private void convertMultiPhraseQuery(int currentPos, int[] termsIdx, MultiPhraseQuery orig, List<Term[]> terms, int[] pos, IndexReader reader, Collection<Query> flatQueries) throws IOException {
        if (currentPos == 0) {
            // if we have more than 16 terms 
            int numTerms = 0;
            for (Term[] currentPosTerm : terms) {
                numTerms += currentPosTerm.length;
            }
            if (numTerms > 16) {
                for (Term[] currentPosTerm : terms) {
                    for (Term term : currentPosTerm) {
                        super.flatten(new TermQuery(term), reader, flatQueries);    
                    }
                }
                return;
            }
        }
        /*
         * we walk all possible ways and for each path down the MPQ we create a PhraseQuery this is what FieldQuery supports.
         * It seems expensive but most queries will pretty small.
         */
        if (currentPos == terms.size()) {
            PhraseQuery query = new PhraseQuery();
            query.setBoost(orig.getBoost());
            query.setSlop(orig.getSlop());
            for (int i = 0; i < termsIdx.length; i++) {
                query.add(terms.get(i)[termsIdx[i]], pos[i]);
            }
            this.flatten(query, reader, flatQueries);
        } else {
            Term[] t = terms.get(currentPos);
            for (int i = 0; i < t.length; i++) {
                termsIdx[currentPos] = i;
                convertMultiPhraseQuery(currentPos+1, termsIdx, orig, terms, pos, reader, flatQueries);
            }
        }
    }
    
    void flatten(Filter sourceFilter, IndexReader reader, Collection<Query> flatQueries) throws IOException {
        Boolean highlight = highlightFilters.get();
        if (highlight == null || highlight.equals(Boolean.FALSE)) {
            return;
        }
        if (sourceFilter instanceof TermFilter) {
            flatten(new TermQuery(((TermFilter) sourceFilter).getTerm()), reader, flatQueries);
        } else if (sourceFilter instanceof MultiTermQueryWrapperFilter) {
            if (multiTermQueryWrapperFilterQueryField != null) {
                try {
                    flatten((Query) multiTermQueryWrapperFilterQueryField.get(sourceFilter), reader, flatQueries);
                } catch (IllegalAccessException e) {
                    // ignore
                }
            }
        } else if (sourceFilter instanceof XBooleanFilter) {
            XBooleanFilter booleanFilter = (XBooleanFilter) sourceFilter;
            for (FilterClause clause : booleanFilter.clauses()) {
                if (clause.getOccur() == BooleanClause.Occur.MUST || clause.getOccur() == BooleanClause.Occur.SHOULD) {
                    flatten(clause.getFilter(), reader, flatQueries);
                }
            }
        }
    }
}
