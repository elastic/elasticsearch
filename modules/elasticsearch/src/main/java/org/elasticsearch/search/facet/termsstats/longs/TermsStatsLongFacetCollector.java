/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.search.facet.termsstats.longs;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.trove.map.hash.TLongObjectHashMap;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.TreeSet;

public class TermsStatsLongFacetCollector extends AbstractFacetCollector {

    private final TermsStatsFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final String keyFieldName;

    private final String valueFieldName;

    private final int size;

    private final int numberOfShards;

    private final FieldDataType keyFieldDataType;

    private NumericFieldData keyFieldData;

    private final FieldDataType valueFieldDataType;

    private NumericFieldData valueFieldData;

    private final SearchScript script;


    private int missing = 0;
    private final TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry> entries;

    public TermsStatsLongFacetCollector(String facetName, String keyFieldName, String valueFieldName, int size, TermsStatsFacet.ComparatorType comparatorType,
                                        SearchContext context, String scriptLang, String script, Map<String, Object> params) {
        super(facetName);
        this.fieldDataCache = context.fieldDataCache();
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();

        MapperService.SmartNameFieldMappers smartMappers = context.mapperService().smartName(keyFieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            this.keyFieldName = keyFieldName;
            this.keyFieldDataType = FieldDataType.DefaultTypes.STRING;
        } else {
            // add type filter if there is exact doc mapper associated with it
            if (smartMappers.hasDocMapper()) {
                setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
            }

            this.keyFieldName = smartMappers.mapper().names().indexName();
            this.keyFieldDataType = smartMappers.mapper().fieldDataType();
        }

        if (script == null) {
            FieldMapper fieldMapper = context.mapperService().smartNameFieldMapper(valueFieldName);
            if (fieldMapper == null) {
                throw new ElasticSearchIllegalArgumentException("failed to find mappings for [" + valueFieldName + "]");
            }
            this.valueFieldName = fieldMapper.names().indexName();
            this.valueFieldDataType = fieldMapper.fieldDataType();
            this.script = null;
        } else {
            this.valueFieldName = null;
            this.valueFieldDataType = null;
            this.script = context.scriptService().search(context.lookup(), scriptLang, script, params);
        }

        this.entries = popFacets();
    }

    @Override public void setScorer(Scorer scorer) throws IOException {
        if (script != null) {
            script.setScorer(scorer);
        }
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        keyFieldData = (NumericFieldData) fieldDataCache.cache(keyFieldDataType, reader, keyFieldName);
        if (valueFieldName != null) {
            valueFieldData = (NumericFieldData) fieldDataCache.cache(valueFieldDataType, reader, valueFieldName);
        }
        if (script != null) {
            script.setNextReader(reader);
        }
    }

    @Override protected void doCollect(int doc) throws IOException {
        if (!keyFieldData.hasValue(doc)) {
            missing++;
            return;
        }
        long key = keyFieldData.longValue(doc);
        InternalTermsStatsLongFacet.LongEntry LongEntry = entries.get(key);
        if (LongEntry == null) {
            LongEntry = new InternalTermsStatsLongFacet.LongEntry(key, 1, 0);
            entries.put(key, LongEntry);
        } else {
            LongEntry.count++;
        }
        if (script == null) {
            if (valueFieldData.multiValued()) {
                for (double value : valueFieldData.doubleValues(doc)) {
                    LongEntry.total += value;
                }
            } else {
                double value = valueFieldData.doubleValue(doc);
                LongEntry.total += value;
            }
        } else {
            script.setNextDocId(doc);
            double value = script.runAsDouble();
            LongEntry.total += value;
        }
    }

    @Override public Facet facet() {
        if (entries.isEmpty()) {
            return new InternalTermsStatsLongFacet(facetName, comparatorType, size, ImmutableList.<InternalTermsStatsLongFacet.LongEntry>of(), missing);
        }
        if (size == 0) { // all terms
            // all terms, just return the collection, we will sort it on the way back
            return new InternalTermsStatsLongFacet(facetName, comparatorType, 0 /* indicates all terms*/, entries.valueCollection(), missing);
        }
        // we need to fetch facets of "size * numberOfShards" because of problems in how they are distributed across shards
        TreeSet<InternalTermsStatsLongFacet.LongEntry> ordered = new BoundedTreeSet<InternalTermsStatsLongFacet.LongEntry>(comparatorType.comparator(), size * numberOfShards);
        ordered.addAll(entries.valueCollection());

        // that's fine to push here, this thread will be released AFTER the entries have either been serialized
        // or processed
        pushFacets(entries);
        return new InternalTermsStatsLongFacet(facetName, comparatorType, size, ordered, missing);
    }


    static TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry> popFacets() {
        Deque<TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry>> deque = cache.get().get();
        if (deque.isEmpty()) {
            deque.add(new TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry>());
        }
        TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry> facets = deque.pollFirst();
        facets.clear();
        return facets;
    }

    static void pushFacets(TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry> facets) {
        facets.clear();
        Deque<TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry>> deque = cache.get().get();
        if (deque != null) {
            deque.add(facets);
        }
    }

    static ThreadLocal<ThreadLocals.CleanableValue<Deque<TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry>>>> cache = new ThreadLocal<ThreadLocals.CleanableValue<Deque<TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry>>>>() {
        @Override protected ThreadLocals.CleanableValue<Deque<TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry>>> initialValue() {
            return new ThreadLocals.CleanableValue<Deque<TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry>>>(new ArrayDeque<TLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry>>());
        }
    };
}