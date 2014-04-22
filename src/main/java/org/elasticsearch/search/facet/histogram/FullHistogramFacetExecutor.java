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

package org.elasticsearch.search.facet.histogram;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.facet.DoubleFacetAggregatorBase;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A histogram facet collector that uses the same field as the key as well as the
 * value.
 */
public class FullHistogramFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData indexFieldData;
    private final HistogramFacet.ComparatorType comparatorType;
    final long interval;

    final Recycler.V<LongObjectOpenHashMap<InternalFullHistogramFacet.FullEntry>> entries;

    public FullHistogramFacetExecutor(IndexNumericFieldData indexFieldData, long interval, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        this.comparatorType = comparatorType;
        this.indexFieldData = indexFieldData;
        this.interval = interval;

        this.entries = context.cacheRecycler().longObjectMap(-1);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        List<InternalFullHistogramFacet.FullEntry> fullEntries = new ArrayList<>(entries.v().size());
        boolean[] states = entries.v().allocated;
        Object[] values = entries.v().values;
        for (int i = 0; i < states.length; i++) {
            if (states[i]) {
                fullEntries.add((InternalFullHistogramFacet.FullEntry) values[i]);
            }
        }
        entries.close();
        return new InternalFullHistogramFacet(facetName, comparatorType, fullEntries);
    }

    public static long bucket(double value, long interval) {
        return (((long) (value / interval)) * interval);
    }

    class Collector extends FacetExecutor.Collector {

        private final HistogramProc histoProc;
        private DoubleValues values;

        Collector() {
            this.histoProc = new HistogramProc(interval, entries.v());
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getDoubleValues();
        }

        @Override
        public void collect(int doc) throws IOException {
            histoProc.onDoc(doc, values);
        }

        @Override
        public void postCollection() {
        }
    }

    public final static class HistogramProc extends DoubleFacetAggregatorBase {

        final long interval;
        final LongObjectOpenHashMap<InternalFullHistogramFacet.FullEntry> entries;

        public HistogramProc(long interval, LongObjectOpenHashMap<InternalFullHistogramFacet.FullEntry> entries) {
            this.interval = interval;
            this.entries = entries;
        }

        @Override
        public void onValue(int docId, double value) {
            long bucket = bucket(value, interval);
            InternalFullHistogramFacet.FullEntry entry = entries.get(bucket);
            if (entry == null) {
                entry = new InternalFullHistogramFacet.FullEntry(bucket, 1, value, value, 1, value);
                entries.put(bucket, entry);
            } else {
                entry.count++;
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