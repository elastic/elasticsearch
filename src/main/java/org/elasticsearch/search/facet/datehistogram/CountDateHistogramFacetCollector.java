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
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * A date histogram facet collector that uses the same field as the key as well as the
 * value.
 */
public class CountDateHistogramFacetCollector extends AbstractFacetCollector {

    private final IndexNumericFieldData indexFieldData;

    private final DateHistogramFacet.ComparatorType comparatorType;

    private LongValues values;
    private final DateHistogramProc histoProc;

    public CountDateHistogramFacetCollector(String facetName, IndexNumericFieldData indexFieldData, TimeZoneRounding tzRounding, DateHistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.comparatorType = comparatorType;
        this.indexFieldData = indexFieldData;
        this.histoProc = new DateHistogramProc(tzRounding);
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        values.forEachValueInDoc(doc, histoProc);
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        values = indexFieldData.load(context).getLongValues();
    }

    @Override
    public Facet facet() {
        return new InternalCountDateHistogramFacet(facetName, comparatorType, histoProc.counts(), true);
    }

    public static class DateHistogramProc implements LongValues.ValueInDocProc {

        private final TLongLongHashMap counts = CacheRecycler.popLongLongMap();

        private final TimeZoneRounding tzRounding;

        public DateHistogramProc(TimeZoneRounding tzRounding) {
            this.tzRounding = tzRounding;
        }

        @Override
        public void onMissing(int docId) {
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