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

package org.elasticsearch.search.facet.termsstats.strings;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
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
import java.util.*;

public class TermsStatsStringFacetCollector extends AbstractFacetCollector {

    private final TermsStatsFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final String keyFieldName;

    private final String valueFieldName;

    private final int size;

    private final int numberOfShards;

    private final FieldDataType keyFieldDataType;

    private FieldData keyFieldData;

    private final FieldDataType valueFieldDataType;

    private NumericFieldData valueFieldData;

    private final SearchScript script;


    private int missing = 0;
    private final ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry> entries;

    public TermsStatsStringFacetCollector(String facetName, String keyFieldName, String valueFieldName, int size, TermsStatsFacet.ComparatorType comparatorType,
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
        keyFieldData = fieldDataCache.cache(keyFieldDataType, reader, keyFieldName);
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
        String key = keyFieldData.stringValue(doc);
        InternalTermsStatsStringFacet.StringEntry stringEntry = entries.get(key);
        if (stringEntry == null) {
            stringEntry = new InternalTermsStatsStringFacet.StringEntry(key, 1, 0);
            entries.put(key, stringEntry);
        } else {
            stringEntry.count++;
        }
        if (script == null) {
            if (valueFieldData.multiValued()) {
                for (double value : valueFieldData.doubleValues(doc)) {
                    stringEntry.total += value;
                }
            } else {
                double value = valueFieldData.doubleValue(doc);
                stringEntry.total += value;
            }
        } else {
            script.setNextDocId(doc);
            double value = script.runAsDouble();
            stringEntry.total += value;
        }
    }

    @Override public Facet facet() {
        if (entries.isEmpty()) {
            return new InternalTermsStatsStringFacet(facetName, comparatorType, size, ImmutableList.<InternalTermsStatsStringFacet.StringEntry>of(), missing);
        }
        if (size == 0) { // all terms
            // all terms, just return the collection, we will sort it on the way back
            return new InternalTermsStatsStringFacet(facetName, comparatorType, 0 /* indicates all terms*/, entries.values(), missing);
        }
        // we need to fetch facets of "size * numberOfShards" because of problems in how they are distributed across shards
        Object[] values = entries.internalValues();
        Arrays.sort(values, (Comparator) comparatorType.comparator());

        List<InternalTermsStatsStringFacet.StringEntry> ordered = Lists.newArrayList();
        int limit = size * numberOfShards;
        for (int i = 0; i < limit; i++) {
            InternalTermsStatsStringFacet.StringEntry value = (InternalTermsStatsStringFacet.StringEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }

        // that's fine to push here, this thread will be released AFTER the entries have either been serialized
        // or processed
        pushFacets(entries);
        return new InternalTermsStatsStringFacet(facetName, comparatorType, size, ordered, missing);
    }


    static ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry> popFacets() {
        Deque<ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry>> deque = cache.get().get();
        if (deque.isEmpty()) {
            deque.add(new ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry>());
        }
        ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry> facets = deque.pollFirst();
        facets.clear();
        return facets;
    }

    static void pushFacets(ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry> facets) {
        facets.clear();
        Deque<ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry>> deque = cache.get().get();
        if (deque != null) {
            deque.add(facets);
        }
    }

    static ThreadLocal<ThreadLocals.CleanableValue<Deque<ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry>>>> cache = new ThreadLocal<ThreadLocals.CleanableValue<Deque<ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry>>>>() {
        @Override protected ThreadLocals.CleanableValue<Deque<ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry>>> initialValue() {
            return new ThreadLocals.CleanableValue<Deque<ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry>>>(new ArrayDeque<ExtTHashMap<String, InternalTermsStatsStringFacet.StringEntry>>());
        }
    };
}