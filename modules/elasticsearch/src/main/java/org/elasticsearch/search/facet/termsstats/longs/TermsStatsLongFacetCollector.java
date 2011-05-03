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
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

    private final SearchScript script;


    private final Aggregator aggregator;

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
            this.aggregator = new Aggregator();
        } else {
            this.valueFieldName = null;
            this.valueFieldDataType = null;
            this.script = context.scriptService().search(context.lookup(), scriptLang, script, params);
            this.aggregator = new ScriptAggregator(this.script);
        }
    }

    @Override public void setScorer(Scorer scorer) throws IOException {
        if (script != null) {
            script.setScorer(scorer);
        }
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        keyFieldData = (NumericFieldData) fieldDataCache.cache(keyFieldDataType, reader, keyFieldName);
        if (script != null) {
            script.setNextReader(reader);
        } else {
            aggregator.valueFieldData = (NumericFieldData) fieldDataCache.cache(valueFieldDataType, reader, valueFieldName);
        }
    }

    @Override protected void doCollect(int doc) throws IOException {
        keyFieldData.forEachValueInDoc(doc, aggregator);
    }

    @Override public Facet facet() {
        if (aggregator.entries.isEmpty()) {
            return new InternalTermsStatsLongFacet(facetName, comparatorType, size, ImmutableList.<InternalTermsStatsLongFacet.LongEntry>of(), aggregator.missing);
        }
        if (size == 0) { // all terms
            // all terms, just return the collection, we will sort it on the way back
            return new InternalTermsStatsLongFacet(facetName, comparatorType, 0 /* indicates all terms*/, aggregator.entries.valueCollection(), aggregator.missing);
        }

        // we need to fetch facets of "size * numberOfShards" because of problems in how they are distributed across shards
        Object[] values = aggregator.entries.internalValues();
        Arrays.sort(values, (Comparator) comparatorType.comparator());

        int limit = size;
        List<InternalTermsStatsLongFacet.LongEntry> ordered = Lists.newArrayList();
        for (int i = 0; i < limit; i++) {
            InternalTermsStatsLongFacet.LongEntry value = (InternalTermsStatsLongFacet.LongEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }
        CacheRecycler.pushLongObjectMap(aggregator.entries);
        return new InternalTermsStatsLongFacet(facetName, comparatorType, size, ordered, aggregator.missing);
    }

    public static class Aggregator implements NumericFieldData.MissingLongValueInDocProc {

        final ExtTLongObjectHashMap<InternalTermsStatsLongFacet.LongEntry> entries = CacheRecycler.popLongObjectMap();

        int missing;

        NumericFieldData valueFieldData;

        final ValueAggregator valueAggregator = new ValueAggregator();

        @Override public void onValue(int docId, long value) {
            InternalTermsStatsLongFacet.LongEntry longEntry = entries.get(value);
            if (longEntry == null) {
                longEntry = new InternalTermsStatsLongFacet.LongEntry(value, 0, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
                entries.put(value, longEntry);
            }
            longEntry.count++;
            valueAggregator.longEntry = longEntry;
            valueFieldData.forEachValueInDoc(docId, valueAggregator);
        }

        @Override public void onMissing(int docId) {
            missing++;
        }

        public static class ValueAggregator implements NumericFieldData.DoubleValueInDocProc {

            InternalTermsStatsLongFacet.LongEntry longEntry;

            @Override public void onValue(int docId, double value) {
                if (value < longEntry.min) {
                    longEntry.min = value;
                }
                if (value > longEntry.max) {
                    longEntry.max = value;
                }
                longEntry.total += value;
                longEntry.totalCount++;
            }
        }
    }

    public static class ScriptAggregator extends Aggregator {

        private final SearchScript script;

        public ScriptAggregator(SearchScript script) {
            this.script = script;
        }

        @Override public void onValue(int docId, long value) {
            InternalTermsStatsLongFacet.LongEntry longEntry = entries.get(value);
            if (longEntry == null) {
                longEntry = new InternalTermsStatsLongFacet.LongEntry(value, 1, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
                entries.put(value, longEntry);
            } else {
                longEntry.count++;
            }
            script.setNextDocId(docId);
            double valueValue = script.runAsDouble();
            if (valueValue < longEntry.min) {
                longEntry.min = valueValue;
            }
            if (valueValue > longEntry.max) {
                longEntry.max = valueValue;
            }
            longEntry.totalCount++;
            longEntry.total += valueValue;
        }
    }
}