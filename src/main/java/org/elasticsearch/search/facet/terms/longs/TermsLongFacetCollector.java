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

package org.elasticsearch.search.facet.terms.longs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.TLongHashSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 *
 */
public class TermsLongFacetCollector extends AbstractFacetCollector {

    private final IndexNumericFieldData indexFieldData;

    private final TermsFacet.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private LongValues values;

    private final StaticAggregatorValueProc aggregator;

    private final SearchScript script;

    public TermsLongFacetCollector(String facetName, IndexNumericFieldData indexFieldData, int size, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                   ImmutableSet<BytesRef> excluded, SearchScript script) {
        super(facetName);
        this.indexFieldData = indexFieldData;
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();
        this.script = script;

        if (this.script == null && excluded.isEmpty()) {
            aggregator = new StaticAggregatorValueProc(CacheRecycler.popLongIntMap());
        } else {
            aggregator = new AggregatorValueProc(CacheRecycler.popLongIntMap(), excluded, this.script);
        }

        // TODO: we need to support this with the new field data....
//        if (allTerms) {
//            try {
//                for (AtomicReaderContext readerContext : context.searcher().getTopReaderContext().leaves()) {
//                    LongFieldData fieldData = (LongFieldData) fieldDataCache.cache(fieldDataType, readerContext.reader(), indexFieldName);
//                    fieldData.forEachValue(aggregator);
//                }
//            } catch (Exception e) {
//                throw new FacetPhaseExecutionException(facetName, "failed to load all terms", e);
//            }
//        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        if (script != null) {
            script.setScorer(scorer);
        }
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        values = indexFieldData.load(context).getLongValues();
        if (script != null) {
            script.setNextReader(context);
        }
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        values.forEachValueInDoc(doc, aggregator);
    }

    @Override
    public Facet facet() {
        TLongIntHashMap facets = aggregator.facets();
        if (facets.isEmpty()) {
            CacheRecycler.pushLongIntMap(facets);
            return new InternalLongTermsFacet(facetName, comparatorType, size, ImmutableList.<InternalLongTermsFacet.LongEntry>of(), aggregator.missing(), aggregator.total());
        } else {
            if (size < EntryPriorityQueue.LIMIT) {
                EntryPriorityQueue ordered = new EntryPriorityQueue(size, comparatorType.comparator());
                for (TLongIntIterator it = facets.iterator(); it.hasNext(); ) {
                    it.advance();
                    ordered.insertWithOverflow(new InternalLongTermsFacet.LongEntry(it.key(), it.value()));
                }
                InternalLongTermsFacet.LongEntry[] list = new InternalLongTermsFacet.LongEntry[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = (InternalLongTermsFacet.LongEntry) ordered.pop();
                }
                CacheRecycler.pushLongIntMap(facets);
                return new InternalLongTermsFacet(facetName, comparatorType, size, Arrays.asList(list), aggregator.missing(), aggregator.total());
            } else {
                BoundedTreeSet<InternalLongTermsFacet.LongEntry> ordered = new BoundedTreeSet<InternalLongTermsFacet.LongEntry>(comparatorType.comparator(), size);
                for (TLongIntIterator it = facets.iterator(); it.hasNext(); ) {
                    it.advance();
                    ordered.add(new InternalLongTermsFacet.LongEntry(it.key(), it.value()));
                }
                CacheRecycler.pushLongIntMap(facets);
                return new InternalLongTermsFacet(facetName, comparatorType, size, ordered, aggregator.missing(), aggregator.total());
            }
        }
    }

    public static class AggregatorValueProc extends StaticAggregatorValueProc {

        private final SearchScript script;

        private final TLongHashSet excluded;

        public AggregatorValueProc(TLongIntHashMap facets, Set<BytesRef> excluded, SearchScript script) {
            super(facets);
            this.script = script;
            if (excluded == null || excluded.isEmpty()) {
                this.excluded = null;
            } else {
                this.excluded = new TLongHashSet(excluded.size());
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

    public static class StaticAggregatorValueProc implements LongValues.ValueInDocProc {

        private final TLongIntHashMap facets;

        private int missing;
        private int total;

        public StaticAggregatorValueProc(TLongIntHashMap facets) {
            this.facets = facets;
        }

        @Override
        public void onValue(int docId, long value) {
            facets.adjustOrPutValue(value, 1, 1);
            total++;
        }

        @Override
        public void onMissing(int docId) {
            missing++;
        }

        public final TLongIntHashMap facets() {
            return facets;
        }

        public final int missing() {
            return this.missing;
        }

        public final int total() {
            return this.total;
        }
    }
}
