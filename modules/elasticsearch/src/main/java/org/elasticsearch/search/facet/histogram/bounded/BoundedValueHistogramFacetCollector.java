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

package org.elasticsearch.search.facet.histogram.bounded;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class BoundedValueHistogramFacetCollector extends AbstractFacetCollector {

    private final String keyIndexFieldName;

    private final String valueIndexFieldName;

    private final long interval;

    private final HistogramFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final FieldDataType keyFieldDataType;
    private NumericFieldData keyFieldData;

    private final FieldDataType valueFieldDataType;

    private final HistogramProc histoProc;

    public BoundedValueHistogramFacetCollector(String facetName, String keyFieldName, String valueFieldName, long interval, long from, long to, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.interval = interval;
        this.comparatorType = comparatorType;
        this.fieldDataCache = context.fieldDataCache();

        MapperService.SmartNameFieldMappers smartMappers = context.mapperService().smartName(keyFieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + keyFieldName + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.hasDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        keyIndexFieldName = smartMappers.mapper().names().indexName();
        keyFieldDataType = smartMappers.mapper().fieldDataType();

        FieldMapper mapper = context.mapperService().smartNameFieldMapper(valueFieldName);
        if (mapper == null) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for value_field [" + valueFieldName + "]");
        }
        valueIndexFieldName = mapper.names().indexName();
        valueFieldDataType = mapper.fieldDataType();

        long normalizedFrom = (((long) ((double) from / interval)) * interval);
        long normalizedTo = (((long) ((double) to / interval)) * interval);
        if ((to % interval) != 0) {
            normalizedTo += interval;
        }
        long offset = -normalizedFrom;
        int size = (int) ((normalizedTo - normalizedFrom) / interval);

        histoProc = new HistogramProc(from, to, interval, offset, size);
    }

    @Override protected void doCollect(int doc) throws IOException {
        keyFieldData.forEachValueInDoc(doc, histoProc);
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        keyFieldData = (NumericFieldData) fieldDataCache.cache(keyFieldDataType, reader, keyIndexFieldName);
        histoProc.valueFieldData = (NumericFieldData) fieldDataCache.cache(valueFieldDataType, reader, valueIndexFieldName);
    }

    @Override public Facet facet() {
        return new InternalBoundedFullHistogramFacet(facetName, comparatorType, interval, -histoProc.offset, histoProc.size, histoProc.entries, true);
    }

    public static class HistogramProc implements NumericFieldData.LongValueInDocProc {

        final long from;
        final long to;

        final long interval;

        final long offset;

        final int size;

        final Object[] entries;

        NumericFieldData valueFieldData;

        final ValueAggregator valueAggregator = new ValueAggregator();

        public HistogramProc(long from, long to, long interval, long offset, int size) {
            this.from = from;
            this.to = to;
            this.interval = interval;
            this.offset = offset;
            this.size = size;
            this.entries = CacheRecycler.popObjectArray(size);
        }

        @Override public void onValue(int docId, long value) {
            if (value <= from || value > to) { // bounds check
                return;
            }
            int index = ((int) ((value + offset) / interval));
            InternalBoundedFullHistogramFacet.FullEntry entry = (InternalBoundedFullHistogramFacet.FullEntry) entries[index];
            if (entry == null) {
                entry = new InternalBoundedFullHistogramFacet.FullEntry(index, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0);
                entries[index] = entry;
            }
            entry.count++;
            valueAggregator.entry = entry;
            valueFieldData.forEachValueInDoc(docId, valueAggregator);
        }


        public static class ValueAggregator implements NumericFieldData.DoubleValueInDocProc {

            InternalBoundedFullHistogramFacet.FullEntry entry;

            @Override public void onValue(int docId, double value) {
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