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
package org.elasticsearch.search.facet.terms.longs;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.LongFacetAggregatorBase;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 *
 */
public class TermsLongFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData indexFieldData;
    private final TermsFacet.ComparatorType comparatorType;
    private final int shardSize;
    private final int size;
    private final SearchScript script;
    private final ImmutableSet<BytesRef> excluded;

    final Recycler.V<LongIntOpenHashMap> facets;
    long missing;
    long total;

    public TermsLongFacetExecutor(IndexNumericFieldData indexFieldData, int size, int shardSize, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                  ImmutableSet<BytesRef> excluded, SearchScript script, CacheRecycler cacheRecycler) {
        this.indexFieldData = indexFieldData;
        this.size = size;
        this.shardSize = shardSize;
        this.comparatorType = comparatorType;
        this.script = script;
        this.excluded = excluded;
        this.facets = cacheRecycler.longIntMap(-1);

        if (allTerms) {
            for (AtomicReaderContext readerContext : context.searcher().getTopReaderContext().leaves()) {
                int maxDoc = readerContext.reader().maxDoc();
                SortedNumericDocValues values = indexFieldData.load(readerContext).getLongValues();
                for (int docId = 0; docId < maxDoc; docId++) {
                    values.setDocument(docId);
                    final int numValues = values.count();
                    final LongIntOpenHashMap v = facets.v();
                    for (int i = 0; i < numValues; i++) {
                        v.putIfAbsent(values.valueAt(i), 0);
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
            facets.close();
            return new InternalLongTermsFacet(facetName, comparatorType, size, ImmutableList.<InternalLongTermsFacet.LongEntry>of(), missing, total);
        } else {
            LongIntOpenHashMap facetEntries  = facets.v();
            final boolean[] states = facets.v().allocated;
            final long[] keys = facets.v().keys;
            final int[] values = facets.v().values;
            if (size < EntryPriorityQueue.LIMIT) {
                EntryPriorityQueue ordered = new EntryPriorityQueue(shardSize, comparatorType.comparator());
                for (int i = 0; i < states.length; i++) {
                    if (states[i]) {
                        ordered.insertWithOverflow(new InternalLongTermsFacet.LongEntry(keys[i], values[i]));
                    }
                }
                InternalLongTermsFacet.LongEntry[] list = new InternalLongTermsFacet.LongEntry[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = (InternalLongTermsFacet.LongEntry) ordered.pop();
                }
                facets.close();
                return new InternalLongTermsFacet(facetName, comparatorType, size, Arrays.asList(list), missing, total);
            } else {
                BoundedTreeSet<InternalLongTermsFacet.LongEntry> ordered = new BoundedTreeSet<>(comparatorType.comparator(), shardSize);
                for (int i = 0; i < states.length; i++) {
                    if (states[i]) {
                        ordered.add(new InternalLongTermsFacet.LongEntry(keys[i], values[i]));
                    }
                }
                facets.close();
                return new InternalLongTermsFacet(facetName, comparatorType, size, ordered, missing, total);
            }
        }
    }

    class Collector extends FacetExecutor.Collector {

        private final StaticAggregatorValueProc aggregator;
        private SortedNumericDocValues values;

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
            values = indexFieldData.load(context).getLongValues();
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
            TermsLongFacetExecutor.this.missing = aggregator.missing();
            TermsLongFacetExecutor.this.total = aggregator.total();
        }
    }

    public static class AggregatorValueProc extends StaticAggregatorValueProc {

        private final SearchScript script;

        private final LongOpenHashSet excluded;

        public AggregatorValueProc(LongIntOpenHashMap facets, Set<BytesRef> excluded, SearchScript script) {
            super(facets);
            this.script = script;
            if (excluded == null || excluded.isEmpty()) {
                this.excluded = null;
            } else {
                this.excluded = new LongOpenHashSet(excluded.size());
                for (BytesRef s : excluded) {
                    this.excluded.add(Long.parseLong(s.utf8ToString()));
                }
            }
        }

        @Override
        public void onValue(int docId, long value) {
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
                    value = ((Number) scriptValue).longValue();
                }
            }
            super.onValue(docId, value);
        }
    }

    public static class StaticAggregatorValueProc extends LongFacetAggregatorBase {

        private final LongIntOpenHashMap facets;

        public StaticAggregatorValueProc(LongIntOpenHashMap facets) {
            this.facets = facets;
        }

        @Override
        public void onValue(int docId, long value) {
            facets.addTo(value, 1);
        }

        public final LongIntOpenHashMap facets() {
            return facets;
        }
    }
}
