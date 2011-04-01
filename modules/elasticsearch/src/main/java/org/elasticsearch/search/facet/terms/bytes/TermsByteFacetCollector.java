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

package org.elasticsearch.search.facet.terms.bytes;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.trove.iterator.TByteIntIterator;
import org.elasticsearch.common.trove.map.hash.TByteIntHashMap;
import org.elasticsearch.common.trove.set.hash.TByteHashSet;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.bytes.ByteFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * @author kimchy (shay.banon)
 */
public class TermsByteFacetCollector extends AbstractFacetCollector {

    private final FieldDataCache fieldDataCache;

    private final String indexFieldName;

    private final TermsFacet.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private final FieldDataType fieldDataType;

    private ByteFieldData fieldData;

    private final StaticAggregatorValueProc aggregator;

    private final SearchScript script;

    public TermsByteFacetCollector(String facetName, String fieldName, int size, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                   ImmutableSet<String> excluded, String scriptLang, String script, Map<String, Object> params) {
        super(facetName);
        this.fieldDataCache = context.fieldDataCache();
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();

        MapperService.SmartNameFieldMappers smartMappers = context.mapperService().smartName(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new ElasticSearchIllegalArgumentException("Field [" + fieldName + "] doesn't have a type, can't run terms short facet collector on it");
        } else {
            // add type filter if there is exact doc mapper associated with it
            if (smartMappers.hasDocMapper()) {
                setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
            }

            if (smartMappers.mapper().fieldDataType() != FieldDataType.DefaultTypes.BYTE) {
                throw new ElasticSearchIllegalArgumentException("Field [" + fieldName + "] is not of byte type, can't run terms short facet collector on it");
            }

            this.indexFieldName = smartMappers.mapper().names().indexName();
            this.fieldDataType = smartMappers.mapper().fieldDataType();
        }

        if (script != null) {
            this.script = context.scriptService().search(context.lookup(), scriptLang, script, params);
        } else {
            this.script = null;
        }

        if (this.script == null && excluded.isEmpty()) {
            aggregator = new StaticAggregatorValueProc(CacheRecycler.popByteIntMap());
        } else {
            aggregator = new AggregatorValueProc(CacheRecycler.popByteIntMap(), excluded, this.script);
        }

        if (allTerms) {
            try {
                for (IndexReader reader : context.searcher().subReaders()) {
                    ByteFieldData fieldData = (ByteFieldData) fieldDataCache.cache(fieldDataType, reader, indexFieldName);
                    fieldData.forEachValue(aggregator);
                }
            } catch (Exception e) {
                throw new FacetPhaseExecutionException(facetName, "failed to load all terms", e);
            }
        }
    }

    @Override public void setScorer(Scorer scorer) throws IOException {
        if (script != null) {
            script.setScorer(scorer);
        }
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        fieldData = (ByteFieldData) fieldDataCache.cache(fieldDataType, reader, indexFieldName);
        if (script != null) {
            script.setNextReader(reader);
        }
    }

    @Override protected void doCollect(int doc) throws IOException {
        fieldData.forEachValueInDoc(doc, aggregator);
    }

    @Override public Facet facet() {
        TByteIntHashMap facets = aggregator.facets();
        if (facets.isEmpty()) {
            CacheRecycler.pushByteIntMap(facets);
            return new InternalByteTermsFacet(facetName, comparatorType, size, ImmutableList.<InternalByteTermsFacet.ByteEntry>of(), aggregator.missing());
        } else {
            if (size < EntryPriorityQueue.LIMIT) {
                EntryPriorityQueue ordered = new EntryPriorityQueue(size, comparatorType.comparator());
                for (TByteIntIterator it = facets.iterator(); it.hasNext();) {
                    it.advance();
                    ordered.insertWithOverflow(new InternalByteTermsFacet.ByteEntry(it.key(), it.value()));
                }
                InternalByteTermsFacet.ByteEntry[] list = new InternalByteTermsFacet.ByteEntry[ordered.size()];
                for (int i = ordered.size() - 1; i >= 0; i--) {
                    list[i] = (InternalByteTermsFacet.ByteEntry) ordered.pop();
                }
                CacheRecycler.pushByteIntMap(facets);
                return new InternalByteTermsFacet(facetName, comparatorType, size, Arrays.asList(list), aggregator.missing());
            } else {
                BoundedTreeSet<InternalByteTermsFacet.ByteEntry> ordered = new BoundedTreeSet<InternalByteTermsFacet.ByteEntry>(comparatorType.comparator(), size);
                for (TByteIntIterator it = facets.iterator(); it.hasNext();) {
                    it.advance();
                    ordered.add(new InternalByteTermsFacet.ByteEntry(it.key(), it.value()));
                }
                CacheRecycler.pushByteIntMap(facets);
                return new InternalByteTermsFacet(facetName, comparatorType, size, ordered, aggregator.missing());
            }
        }
    }

    public static class AggregatorValueProc extends StaticAggregatorValueProc {

        private final SearchScript script;

        private final TByteHashSet excluded;

        public AggregatorValueProc(TByteIntHashMap facets, Set<String> excluded, SearchScript script) {
            super(facets);
            if (excluded == null || excluded.isEmpty()) {
                this.excluded = null;
            } else {
                this.excluded = new TByteHashSet(excluded.size());
                for (String s : excluded) {
                    this.excluded.add(Byte.parseByte(s));
                }
            }
            this.script = script;
        }

        @Override public void onValue(int docId, byte value) {
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
                    value = ((Number) scriptValue).byteValue();
                }
            }
            super.onValue(docId, value);
        }
    }

    public static class StaticAggregatorValueProc implements ByteFieldData.ValueInDocProc, ByteFieldData.ValueProc {

        private final TByteIntHashMap facets;

        private int missing;

        public StaticAggregatorValueProc(TByteIntHashMap facets) {
            this.facets = facets;
        }

        @Override public void onValue(byte value) {
            facets.putIfAbsent(value, 0);
        }

        @Override public void onValue(int docId, byte value) {
            facets.adjustOrPutValue(value, 1, 1);
        }

        @Override public void onMissing(int docID) {
            missing++;
        }

        public final TByteIntHashMap facets() {
            return facets;
        }

        public final int missing() {
            return this.missing;
        }
    }
}
