/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.facet.terms.doubles;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import gnu.trove.iterator.TDoubleIntIterator;
import gnu.trove.map.hash.TDoubleIntHashMap;
import gnu.trove.set.hash.TDoubleHashSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.DoubleFacetAggregatorBase;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 *
 */
public class TermsDoubleFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData indexFieldData;
    private final TermsFacet.ComparatorType comparatorType;
    private final int size;
    private final SearchScript script;
    private final ImmutableSet<BytesRef> excluded;

    final Recycler.V<TDoubleIntHashMap> facets;
    long missing;
    long total;

    public TermsDoubleFacetExecutor(IndexNumericFieldData indexFieldData, int size, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                    ImmutableSet<BytesRef> excluded, SearchScript script, CacheRecycler cacheRecycler) {
        this.indexFieldData = indexFieldData;
        this.size = size;
        this.comparatorType = comparatorType;
        this.script = script;
        this.excluded = excluded;

        this.facets = cacheRecycler.doubleIntMap(-1);

        if (allTerms) {
            for (AtomicReaderContext readerContext : context.searcher().getTopReaderContext().leaves()) {
                int maxDoc = readerContext.reader().maxDoc();
                DoubleValues values = indexFieldData.load(readerContext).getDoubleValues();
                if (values instanceof DoubleValues.WithOrdinals) {
                    DoubleValues.WithOrdinals valuesWithOrds = (DoubleValues.WithOrdinals) values;
                    Ordinals.Docs ordinals = valuesWithOrds.ordinals();
                    for (int ord = 1; ord < ordinals.getMaxOrd(); ord++) {
                        facets.v().putIfAbsent(valuesWithOrds.getValueByOrd(ord), 0);
                    }
                } else {
                    // Shouldn't be true, otherwise it is WithOrdinals... just to be sure...
                    if (values.isMultiValued()) {
                        for (int docId = 0; docId < maxDoc; docId++) {
                            if (!values.hasValue(docId)) {
                                continue;
                            }

                            DoubleValues.Iter iter = values.getIter(docId);
                            while (iter.hasNext()) {
                                facets.v().putIfAbsent(iter.next(), 0);
                            }
                        }
                    } else {
                        for (int docId = 0; docId < maxDoc; docId++) {
                            if (!values.hasValue(docId)) {
                                continue;
                            }

                            double value = values.getValue(docId);
                            facets.v().putIfAbsent(value, 0);
                        }
                    }
                }
            }
        }
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        if (facets.v().isEmpty()) {
            facets.release();
            return new InternalDoubleTermsFacet(facetName, comparatorType, size, ImmutableList.<InternalDoubleTermsFacet.DoubleEntry>of(), missing, total);
        } else {
            if (size < EntryPriorityQueue.LIMIT) {
                EntryPriorityQueue ordered = new EntryPriorityQueue(size, comparatorType.comparator());
                for (TDoubleIntIterator it = facets.v().iterator(); it.hasNext(); ) {
                    it.advance();
                    ordered.insertWithOverflow(new InternalDoubleTermsFacet.DoubleEntry(it.key(), it.value()));
                }
                InternalDoubleTermsFacet.DoubleEntry[] list = new InternalDoubleTermsFacet.DoubleEntry[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = (InternalDoubleTermsFacet.DoubleEntry) ordered.pop();
                }
                facets.release();
                return new InternalDoubleTermsFacet(facetName, comparatorType, size, Arrays.asList(list), missing, total);
            } else {
                BoundedTreeSet<InternalDoubleTermsFacet.DoubleEntry> ordered = new BoundedTreeSet<InternalDoubleTermsFacet.DoubleEntry>(comparatorType.comparator(), size);
                for (TDoubleIntIterator it = facets.v().iterator(); it.hasNext(); ) {
                    it.advance();
                    ordered.add(new InternalDoubleTermsFacet.DoubleEntry(it.key(), it.value()));
                }
                facets.release();
                return new InternalDoubleTermsFacet(facetName, comparatorType, size, ordered, missing, total);
            }
        }
    }

    class Collector extends FacetExecutor.Collector {

        private final StaticAggregatorValueProc aggregator;
        private DoubleValues values;

        public Collector() {
            if (script == null && excluded.isEmpty()) {
                aggregator = new StaticAggregatorValueProc(facets.v());
            } else {
                aggregator = new AggregatorValueProc(facets.v(), excluded, script);
            }
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            if (script != null) {
                script.setScorer(scorer);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getDoubleValues();
            if (script != null) {
                script.setNextReader(context);
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            aggregator.onDoc(doc, values);
        }

        @Override
        public void postCollection() {
            TermsDoubleFacetExecutor.this.missing = aggregator.missing();
            TermsDoubleFacetExecutor.this.total = aggregator.total();
        }
    }

    public static class AggregatorValueProc extends StaticAggregatorValueProc {

        private final SearchScript script;

        private final TDoubleHashSet excluded;

        public AggregatorValueProc(TDoubleIntHashMap facets, Set<BytesRef> excluded, SearchScript script) {
            super(facets);
            this.script = script;
            if (excluded == null || excluded.isEmpty()) {
                this.excluded = null;
            } else {
                this.excluded = new TDoubleHashSet(excluded.size());
                for (BytesRef s : excluded) {
                    this.excluded.add(Double.parseDouble(s.utf8ToString()));
                }
            }
        }

        @Override
        public void onValue(int docId, double value) {
            if (excluded != null && excluded.contains(value)) {
                return;
            }
            if (script != null) {
                script.setNextDocId(docId);
                script.setNextVar("term", value);
                Object scriptValue = script.run();
                if (scriptValue == null) {
                    return;
                }
                if (scriptValue instanceof Boolean) {
                    if (!((Boolean) scriptValue)) {
                        return;
                    }
                } else {
                    value = ((Number) scriptValue).doubleValue();
                }
            }
            super.onValue(docId, value);
        }
    }

    public static class StaticAggregatorValueProc extends DoubleFacetAggregatorBase {

        private final TDoubleIntHashMap facets;

        public StaticAggregatorValueProc(TDoubleIntHashMap facets) {
            this.facets = facets;
        }

        @Override
        public void onValue(int docId, double value) {
            facets.adjustOrPutValue(value, 1, 1);
        }

        public final TDoubleIntHashMap facets() {
            return facets;
        }
    }
}
