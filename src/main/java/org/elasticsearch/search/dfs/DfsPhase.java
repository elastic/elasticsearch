/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.search.dfs;

import com.google.common.collect.ImmutableMap;
import gnu.trove.set.hash.THashSet;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.elasticsearch.common.collect.XMaps;
import org.elasticsearch.common.util.concurrent.ThreadLocals;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;

/**
 *
 */
public class DfsPhase implements SearchPhase {

    private static ThreadLocal<ThreadLocals.CleanableValue<THashSet<Term>>> cachedTermsSet = new ThreadLocal<ThreadLocals.CleanableValue<THashSet<Term>>>() {
        @Override
        protected ThreadLocals.CleanableValue<THashSet<Term>> initialValue() {
            return new ThreadLocals.CleanableValue<THashSet<Term>>(new THashSet<Term>());
        }
    };

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of();
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    public void execute(SearchContext context) {
        THashSet<Term> termsSet = null;
        try {
            if (!context.queryRewritten()) {
                context.updateRewriteQuery(context.searcher().rewrite(context.query()));
            }

            termsSet = cachedTermsSet.get().get();
            if (!termsSet.isEmpty()) {
                termsSet.clear();
            }
            context.query().extractTerms(termsSet);
            if (context.rescore() != null) {
                context.rescore().rescorer().extractTerms(context, context.rescore(), termsSet);
            }

            Term[] terms = termsSet.toArray(new Term[termsSet.size()]);
            TermStatistics[] termStatistics = new TermStatistics[terms.length];
            IndexReaderContext indexReaderContext = context.searcher().getTopReaderContext();
            for (int i = 0; i < terms.length; i++) {
                // LUCENE 4 UPGRADE: cache TermContext?
                TermContext termContext = TermContext.build(indexReaderContext, terms[i], false);
                termStatistics[i] = context.searcher().termStatistics(terms[i], termContext);
            }

            Map<String, CollectionStatistics> fieldStatistics = XMaps.newNoNullKeysMap();
            for (Term term : terms) {
                assert term.field() != null : "field is null";
                if (!fieldStatistics.containsKey(term.field())) {
                    final CollectionStatistics collectionStatistics = context.searcher().collectionStatistics(term.field());
                    fieldStatistics.put(term.field(), collectionStatistics);
                }
            }

            context.dfsResult().termsStatistics(terms, termStatistics)
                    .fieldStatistics(fieldStatistics)
                    .maxDoc(context.searcher().getIndexReader().maxDoc());
        } catch (Exception e) {
            throw new DfsPhaseExecutionException(context, "Exception during dfs phase", e);
        } finally {
            if (termsSet != null) {
                termsSet.clear(); // don't hold on to terms
            }
        }
    }
}
