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

package org.elasticsearch.search.facet.datehistogram;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.time.MutableDateTime;
import org.elasticsearch.common.trove.map.hash.TLongLongHashMap;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.longs.LongFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * A date histogram facet collector that uses the same field as the key as well as the
 * value.
 *
 * @author kimchy (shay.banon)
 */
public class CountDateHistogramFacetCollector extends AbstractFacetCollector {

    private final String indexFieldName;

    private final MutableDateTime dateTime;

    private final DateHistogramFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final FieldDataType fieldDataType;

    private LongFieldData fieldData;

    private final DateHistogramProc histoProc;

    public CountDateHistogramFacetCollector(String facetName, String fieldName, MutableDateTime dateTime, long interval, DateHistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.dateTime = dateTime;
        this.comparatorType = comparatorType;
        this.fieldDataCache = context.fieldDataCache();

        MapperService.SmartNameFieldMappers smartMappers = context.mapperService().smartName(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldName + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.hasDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        FieldMapper mapper = smartMappers.mapper();

        indexFieldName = mapper.names().indexName();
        fieldDataType = mapper.fieldDataType();

        if (interval == 1) {
            histoProc = new DateHistogramProc();
        } else {
            histoProc = new IntervalDateHistogramProc(interval);
        }
    }

    @Override protected void doCollect(int doc) throws IOException {
        fieldData.forEachValueInDoc(doc, dateTime, histoProc);
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        fieldData = (LongFieldData) fieldDataCache.cache(fieldDataType, reader, indexFieldName);
    }

    @Override public Facet facet() {
        return new InternalCountDateHistogramFacet(facetName, comparatorType, histoProc.counts(), true);
    }

    public static long bucket(long value, long interval) {
        return ((value / interval) * interval);
    }

    public static class DateHistogramProc implements LongFieldData.DateValueInDocProc {

        protected final TLongLongHashMap counts = CacheRecycler.popLongLongMap();

        @Override public void onValue(int docId, MutableDateTime dateTime) {
            counts.adjustOrPutValue(dateTime.getMillis(), 1, 1);
        }

        public TLongLongHashMap counts() {
            return counts;
        }
    }

    public static class IntervalDateHistogramProc extends DateHistogramProc {

        private final long interval;

        public IntervalDateHistogramProc(long interval) {
            this.interval = interval;
        }

        @Override public void onValue(int docId, MutableDateTime dateTime) {
            long bucket = bucket(dateTime.getMillis(), interval);
            counts.adjustOrPutValue(bucket, 1, 1);
        }
    }
}