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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.util.concurrent.ThreadLocals;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.longs.LongFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class TermsLongFacetCollector extends AbstractFacetCollector {

    static ThreadLocal<ThreadLocals.CleanableValue<Deque<TLongIntHashMap>>> cache = new ThreadLocal<ThreadLocals.CleanableValue<Deque<TLongIntHashMap>>>() {
        @Override
        protected ThreadLocals.CleanableValue<Deque<TLongIntHashMap>> initialValue() {
            return new ThreadLocals.CleanableValue<Deque<TLongIntHashMap>>(new ArrayDeque<TLongIntHashMap>());
        }
    };


    private final FieldDataCache fieldDataCache;

    private final String indexFieldName;

    private final TermsFacet.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private final FieldDataType fieldDataType;

    private LongFieldData fieldData;

    private final StaticAggregatorValueProc aggregator;

    private final SearchScript script;

    public TermsLongFacetCollector(String facetName, String fieldName, int size, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                   ImmutableSet<String> excluded, String scriptLang, String script, Map<String, Object> params) {
        super(facetName);
        this.fieldDataCache = context.fieldDataCache();
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();

        MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new ElasticSearchIllegalArgumentException("Field [" + fieldName + "] doesn't have a type, can't run terms long facet collector on it");
        }
        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.explicitTypeInNameWithDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        if (smartMappers.mapper().fieldDataType() != FieldDataType.DefaultTypes.LONG) {
            throw new ElasticSearchIllegalArgumentException("Field [" + fieldName + "] is not of long type, can't run terms long facet collector on it");
        }

        this.indexFieldName = smartMappers.mapper().names().indexName();
        this.fieldDataType = smartMappers.mapper().fieldDataType();

        if (script != null) {
            this.script = context.scriptService().search(context.lookup(), scriptLang, script, params);
        } else {
            this.script = null;
        }

        if (this.script == null && excluded.isEmpty()) {
            aggregator = new StaticAggregatorValueProc(CacheRecycler.popLongIntMap());
        } else {
            aggregator = new AggregatorValueProc(CacheRecycler.popLongIntMap(), excluded, this.script);
        }

        if (allTerms) {
            try {
                for (IndexReader reader : context.searcher().subReaders()) {
                    LongFieldData fieldData = (LongFieldData) fieldDataCache.cache(fieldDataType, reader, indexFieldName);
                    fieldData.forEachValue(aggregator);
                }
            } catch (Exception e) {
                throw new FacetPhaseExecutionException(facetName, "failed to load all terms", e);
            }
        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        if (script != null) {
            script.setScorer(scorer);
        }
    }

    @Override
    protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        fieldData = (LongFieldData) fieldDataCache.cache(fieldDataType, reader, indexFieldName);
        if (script != null) {
            script.setNextReader(reader);
        }
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        fieldData.forEachValueInDoc(doc, aggregator);
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

        public AggregatorValueProc(TLongIntHashMap facets, Set<String> excluded, SearchScript script) {
            super(facets);
            this.script = script;
            if (excluded == null || excluded.isEmpty()) {
                this.excluded = null;
            } else {
                this.excluded = new TLongHashSet(excluded.size());
                for (String s : excluded) {
                    this.excluded.add(Long.parseLong(s));
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

    public static class StaticAggregatorValueProc implements LongFieldData.ValueInDocProc, LongFieldData.ValueProc {

        private final TLongIntHashMap facets;

        private int missing;
        private int total;

        public StaticAggregatorValueProc(TLongIntHashMap facets) {
            this.facets = facets;
        }

        @Override
        public void onValue(long value) {
            facets.putIfAbsent(value, 0);
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
