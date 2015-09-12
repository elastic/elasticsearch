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

package org.elasticsearch.search.dfs;

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreSearchContext;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class DfsPhase implements SearchPhase {

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of();
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    @Override
    public void execute(SearchContext context) {
        final ObjectHashSet<Term> termsSet = new ObjectHashSet<>();
        try {
            context.searcher().createNormalizedWeight(context.query(), true).extractTerms(new DelegateSet(termsSet));
            for (RescoreSearchContext rescoreContext : context.rescore()) {
                rescoreContext.rescorer().extractTerms(context, rescoreContext, new DelegateSet(termsSet));
            }

            Term[] terms = termsSet.toArray(Term.class);
            TermStatistics[] termStatistics = new TermStatistics[terms.length];
            IndexReaderContext indexReaderContext = context.searcher().getTopReaderContext();
            for (int i = 0; i < terms.length; i++) {
                // LUCENE 4 UPGRADE: cache TermContext?
                TermContext termContext = TermContext.build(indexReaderContext, terms[i]);
                termStatistics[i] = context.searcher().termStatistics(terms[i], termContext);
            }

            ObjectObjectHashMap<String, CollectionStatistics> fieldStatistics = HppcMaps.newNoNullKeysMap();
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
            termsSet.clear(); // don't hold on to terms
        }
    }

    // We need to bridge to JCF world, b/c of Query#extractTerms
    private static class DelegateSet extends AbstractSet<Term> {

        private final ObjectHashSet<Term> delegate;

        private DelegateSet(ObjectHashSet<Term> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean add(Term term) {
            return delegate.add(term);
        }

        @Override
        public boolean addAll(Collection<? extends Term> terms) {
            boolean result = false;
            for (Term term : terms) {
                result = delegate.add(term);
            }
            return result;
        }

        @Override
        public Iterator<Term> iterator() {
            final Iterator<ObjectCursor<Term>> iterator = delegate.iterator();
            return new Iterator<Term>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Term next() {
                    return iterator.next().value;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public int size() {
            return delegate.size();
        }
    }

}
