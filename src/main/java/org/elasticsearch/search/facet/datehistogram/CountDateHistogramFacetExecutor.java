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

import gnu.trove.map.hash.TLongLongHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.LongFacetAggregatorBase;

import java.io.IOException;

/**
 * A date histogram facet collector that uses the same field as the key as well as the
 * value.
 */
public class CountDateHistogramFacetExecutor extends FacetExecutor {

    private final CacheRecycler cacheRecycler;
    private final TimeZoneRounding tzRounding;
    private final IndexNumericFieldData indexFieldData;
    final DateHistogramFacet.ComparatorType comparatorType;

    final TLongLongHashMap counts;

    public CountDateHistogramFacetExecutor(IndexNumericFieldData indexFieldData, TimeZoneRounding tzRounding, DateHistogramFacet.ComparatorType comparatorType, CacheRecycler cacheRecycler) {
        this.comparatorType = comparatorType;
        this.indexFieldData = indexFieldData;
        this.tzRounding = tzRounding;
        this.cacheRecycler = cacheRecycler;

        this.counts = cacheRecycler.popLongLongMap();
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new InternalCountDateHistogramFacet(facetName, comparatorType, counts, cacheRecycler);
    }

    class Collector extends FacetExecutor.Collector {

        private LongValues values;
        private final DateHistogramProc histoProc;

        public Collector() {
            this.histoProc = new DateHistogramProc(counts, tzRounding);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getLongValues();
        }

        @Override
        public void collect(int doc) throws IOException {
            histoProc.onDoc(doc, values);
        }

        @Override
        public void postCollection() {
        }
    }

    public static class DateHistogramProc extends LongFacetAggregatorBase {

        private final TLongLongHashMap counts;
        private final TimeZoneRounding tzRounding;

        public DateHistogramProc(TLongLongHashMap counts, TimeZoneRounding tzRounding) {
            this.counts = counts;
            this.tzRounding = tzRounding;
        }

        @Override
        public void onValue(int docId, long value) {
            counts.adjustOrPutValue(tzRounding.calc(value), 1, 1);
        }

        public TLongLongHashMap counts() {
            return counts;
        }
    }
}