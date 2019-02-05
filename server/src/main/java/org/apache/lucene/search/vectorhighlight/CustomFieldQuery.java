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
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;

import java.io.IOException;
import java.util.Collection;

// LUCENE MONITOR
// TODO: remove me!
public class CustomFieldQuery extends FieldQuery {

    public static final ThreadLocal<Boolean> highlightFilters = new ThreadLocal<>();

    public CustomFieldQuery(Query query, IndexReader reader, FastVectorHighlighter highlighter) throws IOException {
        this(query, reader, highlighter.isPhraseHighlight(), highlighter.isFieldMatch());
    }

    public CustomFieldQuery(Query query, IndexReader reader, boolean phraseHighlight, boolean fieldMatch) throws IOException {
        super(query, reader, phraseHighlight, fieldMatch);
        highlightFilters.remove();
    }

    @Override
    protected void flatten(Query sourceQuery, IndexReader reader, Collection<Query> flatQueries, float boost) throws IOException {
        if (sourceQuery instanceof BoostQuery) {
            BoostQuery bq = (BoostQuery) sourceQuery;
            sourceQuery = bq.getQuery();
            boost *= bq.getBoost();
            flatten(sourceQuery, reader, flatQueries, boost);
        } else if (sourceQuery instanceof SpanTermQuery) {
            super.flatten(new TermQuery(((SpanTermQuery) sourceQuery).getTerm()), reader, flatQueries, boost);
        } else if (sourceQuery instanceof ConstantScoreQuery) {
            flatten(((ConstantScoreQuery) sourceQuery).getQuery(), reader, flatQueries, boost);
        } else if (sourceQuery instanceof FunctionScoreQuery) {
            flatten(((FunctionScoreQuery) sourceQuery).getSubQuery(), reader, flatQueries, boost);
        } else if (sourceQuery instanceof MultiPhrasePrefixQuery) {
            flatten(sourceQuery.rewrite(reader), reader, flatQueries, boost);
        } else if (sourceQuery instanceof MultiPhraseQuery) {
            MultiPhraseQuery q = ((MultiPhraseQuery) sourceQuery);
            convertMultiPhraseQuery(0, new int[q.getTermArrays().length], q, q.getTermArrays(), q.getPositions(), reader, flatQueries);
        } else if (sourceQuery instanceof BlendedTermQuery) {
            final BlendedTermQuery blendedTermQuery = (BlendedTermQuery) sourceQuery;
            flatten(blendedTermQuery.rewrite(reader), reader, flatQueries, boost);
        } else if (sourceQuery instanceof org.apache.lucene.queries.function.FunctionScoreQuery) {
            org.apache.lucene.queries.function.FunctionScoreQuery funcScoreQuery =
                (org.apache.lucene.queries.function.FunctionScoreQuery) sourceQuery;
            //flatten query with query boost
            flatten(funcScoreQuery.getWrappedQuery(), reader, flatQueries, boost);
        } else if (sourceQuery instanceof SynonymQuery) {
            // SynonymQuery should be handled by the parent class directly.
            // This statement should be removed when https://issues.apache.org/jira/browse/LUCENE-7484 is merged.
            SynonymQuery synQuery = (SynonymQuery) sourceQuery;
            for (Term term : synQuery.getTerms()) {
                flatten(new TermQuery(term), reader, flatQueries, boost);
            }
        } else if (sourceQuery instanceof ESToParentBlockJoinQuery) {
            Query childQuery = ((ESToParentBlockJoinQuery) sourceQuery).getChildQuery();
            if (childQuery != null) {
                flatten(childQuery, reader, flatQueries, boost);
            }
        } else {
            super.flatten(sourceQuery, reader, flatQueries, boost);
        }
    }

    private void convertMultiPhraseQuery(int currentPos, int[] termsIdx, MultiPhraseQuery orig, Term[][] terms, int[] pos,
            IndexReader reader, Collection<Query> flatQueries) throws IOException {
        if (currentPos == 0) {
            // if we have more than 16 terms
            int numTerms = 0;
            for (Term[] currentPosTerm : terms) {
                numTerms += currentPosTerm.length;
            }
            if (numTerms > 16) {
                for (Term[] currentPosTerm : terms) {
                    for (Term term : currentPosTerm) {
                        super.flatten(new TermQuery(term), reader, flatQueries, 1F);
                    }
                }
                return;
            }
        }
        /*
         * we walk all possible ways and for each path down the MPQ we create a PhraseQuery this is what FieldQuery supports.
         * It seems expensive but most queries will pretty small.
         */
        if (currentPos == terms.length) {
            PhraseQuery.Builder queryBuilder = new PhraseQuery.Builder();
            queryBuilder.setSlop(orig.getSlop());
            for (int i = 0; i < termsIdx.length; i++) {
                queryBuilder.add(terms[i][termsIdx[i]], pos[i]);
            }
            Query query = queryBuilder.build();
            this.flatten(query, reader, flatQueries, 1F);
        } else {
            Term[] t = terms[currentPos];
            for (int i = 0; i < t.length; i++) {
                termsIdx[currentPos] = i;
                convertMultiPhraseQuery(currentPos+1, termsIdx, orig, terms, pos, reader, flatQueries);
            }
        }
    }
}
