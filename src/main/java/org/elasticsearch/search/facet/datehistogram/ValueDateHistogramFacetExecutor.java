/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.facet.DoubleFacetAggregatorBase;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.LongFacetAggregatorBase;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A histogram facet collector that uses different fields for the key and the value.
 */
public class ValueDateHistogramFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData keyIndexFieldData;
    private final IndexNumericFieldData valueIndexFieldData;
    private final DateHistogramFacet.ComparatorType comparatorType;
    final Rounding tzRounding;

    final Recycler.V<LongObjectOpenHashMap<InternalFullDateHistogramFacet.FullEntry>> entries;

    public ValueDateHistogramFacetExecutor(IndexNumericFieldData keyIndexFieldData, IndexNumericFieldData valueIndexFieldData, Rounding tzRounding, DateHistogramFacet.ComparatorType comparatorType, CacheRecycler cacheRecycler) {
        this.comparatorType = comparatorType;
        this.keyIndexFieldData = keyIndexFieldData;
        this.valueIndexFieldData = valueIndexFieldData;
        this.tzRounding = tzRounding;

        this.entries = cacheRecycler.longObjectMap(-1);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        ArrayList<InternalFullDateHistogramFacet.FullEntry> entries1 = new ArrayList<>(entries.v().size());
        final boolean[] states = entries.v().allocated;
        final Object[] values = entries.v().values;
        for (int i = 0; i < states.length; i++) {
            if (states[i]) {
                InternalFullDateHistogramFacet.FullEntry value  = (InternalFullDateHistogramFacet.FullEntry) values[i];
                entries1.add(value);
            }
        }

        entries.close();
        return new InternalFullDateHistogramFacet(facetName, comparatorType, entries1);
    }

    class Collector extends FacetExecutor.Collector {

        private final DateHistogramProc histoProc;
        private SortedNumericDocValues keyValues;

        public Collector() {
            this.histoProc = new DateHistogramProc(tzRounding, entries.v());
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            keyValues = keyIndexFieldData.load(context).getLongValues();
            histoProc.valueValues = valueIndexFieldData.load(context).getDoubleValues();
        }

        @Override
        public void collect(int doc) throws IOException {
            histoProc.onDoc(doc, keyValues);
        }

        @Override
        public void postCollection() {
        }
    }

    public static class DateHistogramProc extends LongFacetAggregatorBase {

        final LongObjectOpenHashMap<InternalFullDateHistogramFacet.FullEntry> entries;
        private final Rounding tzRounding;

        SortedNumericDoubleValues valueValues;

        final ValueAggregator valueAggregator = new ValueAggregator();

        public DateHistogramProc(Rounding tzRounding, LongObjectOpenHashMap<InternalFullDateHistogramFacet.FullEntry> entries) {
            this.tzRounding = tzRounding;
            this.entries = entries;
        }

        @Override
        public void onValue(int docId, long value) {
            long time = tzRounding.round(value);

            InternalFullDateHistogramFacet.FullEntry entry = entries.get(time);
            if (entry == null) {
                entry = new InternalFullDateHistogramFacet.FullEntry(time, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0);
                entries.put(time, entry);
            }
            entry.count++;
            valueAggregator.entry = entry;
            valueAggregator.onDoc(docId, valueValues);
        }

        public final static class ValueAggregator extends DoubleFacetAggregatorBase {

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