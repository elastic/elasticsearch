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
import org.apache.lucene.index.Term;
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
        try {
            if (!context.queryRewritten()) {
                context.updateRewriteQuery(context.searcher().rewrite(context.query()));
            }

            THashSet<Term> termsSet = cachedTermsSet.get().get();
            termsSet.clear();
            context.query().extractTerms(termsSet);
            Term[] terms = termsSet.toArray(new Term[termsSet.size()]);
            int[] freqs = context.searcher().docFreqs(terms);

            context.dfsResult().termsAndFreqs(terms, freqs);
            context.dfsResult().maxDoc(context.searcher().getIndexReader().maxDoc());
        } catch (Exception e) {
            throw new DfsPhaseExecutionException(context, "", e);
        }
    }
}
