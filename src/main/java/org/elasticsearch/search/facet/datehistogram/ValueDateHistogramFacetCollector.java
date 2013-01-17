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

package org.elasticsearch.search.facet.datehistogram;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.field.data.longs.LongFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * A histogram facet collector that uses different fields for the key and the value.
 */
public class ValueDateHistogramFacetCollector extends AbstractFacetCollector {

    private final String keyIndexFieldName;
    private final String valueIndexFieldName;

    private final DateHistogramFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final FieldDataType keyFieldDataType;
    private LongFieldData keyFieldData;

    private final FieldDataType valueFieldDataType;

    private final DateHistogramProc histoProc;

    private final String interval;

    public ValueDateHistogramFacetCollector(String facetName, String keyFieldName, String valueFieldName, String interval, TimeZoneRounding tzRounding, DateHistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.comparatorType = comparatorType;
        this.fieldDataCache = context.fieldDataCache();
        this.interval = interval;

        MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(keyFieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + keyFieldName + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.explicitTypeInNameWithDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        keyIndexFieldName = smartMappers.mapper().names().indexName();
        keyFieldDataType = smartMappers.mapper().fieldDataType();

        FieldMapper mapper = context.smartNameFieldMapper(valueFieldName);
        if (mapper == null) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for value_field [" + valueFieldName + "]");
        }
        valueIndexFieldName = mapper.names().indexName();
        valueFieldDataType = mapper.fieldDataType();

        this.histoProc = new DateHistogramProc(tzRounding);
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        keyFieldData.forEachValueInDoc(doc, histoProc);
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        keyFieldData = (LongFieldData) fieldDataCache.cache(keyFieldDataType, context.reader(), keyIndexFieldName);
        histoProc.valueFieldData = (NumericFieldData) fieldDataCache.cache(valueFieldDataType, context.reader(), valueIndexFieldName);
    }

    @Override
    public Facet facet() {
        return new InternalFullDateHistogramFacet(facetName, interval, comparatorType, histoProc.entries, true);
    }

    public static class DateHistogramProc implements LongFieldData.LongValueInDocProc {

        final ExtTLongObjectHashMap<InternalFullDateHistogramFacet.FullEntry> entries = CacheRecycler.popLongObjectMap();

        private final TimeZoneRounding tzRounding;

        NumericFieldData valueFieldData;

        final ValueAggregator valueAggregator = new ValueAggregator();

        public DateHistogramProc(TimeZoneRounding tzRounding) {
            this.tzRounding = tzRounding;
        }

        @Override
        public void onValue(int docId, long value) {
            long time = tzRounding.calc(value);

            InternalFullDateHistogramFacet.FullEntry entry = entries.get(time);
            if (entry == null) {
                entry = new InternalFullDateHistogramFacet.FullEntry(time, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0);
                entries.put(time, entry);
            }
            entry.count++;
            valueAggregator.entry = entry;
            valueFieldData.forEachValueInDoc(docId, valueAggregator);
        }

        public static class ValueAggregator implements NumericFieldData.DoubleValueInDocProc {

            InternalFullDateHistogramFacet.FullEntry entry;

            @Override
            public void onValue(int docId, double value) {
                entry.totalCount++;
                entry.total += value;
                if (value < entry.min) {
                    entry.min = value;
                }
                if (value > entry.max) {
                    entry.max = value;
                }
            }
        }
    }
}
