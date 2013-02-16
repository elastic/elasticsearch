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

package org.elasticsearch.search.facet.histogram;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * A histogram facet collector that uses different fields for the key and the value.
 */
public class ValueHistogramFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData keyIndexFieldData;
    private final IndexNumericFieldData valueIndexFieldData;
    private final HistogramFacet.ComparatorType comparatorType;
    private final long interval;

    final ExtTLongObjectHashMap<InternalFullHistogramFacet.FullEntry> entries;

    public ValueHistogramFacetExecutor(IndexNumericFieldData keyIndexFieldData, IndexNumericFieldData valueIndexFieldData, long interval, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        this.comparatorType = comparatorType;
        this.keyIndexFieldData = keyIndexFieldData;
        this.valueIndexFieldData = valueIndexFieldData;
        this.interval = interval;
        this.entries = CacheRecycler.popLongObjectMap();
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public Post post() {
        return null;
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new InternalFullHistogramFacet(facetName, comparatorType, entries, true);
    }

    class Collector extends FacetExecutor.Collector {

        private final HistogramProc histoProc;
        private DoubleValues keyValues;

        public Collector() {
            this.histoProc = new HistogramProc(interval, entries);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            keyValues = keyIndexFieldData.load(context).getDoubleValues();
            histoProc.valueValues = valueIndexFieldData.load(context).getDoubleValues();
        }

        @Override
        public void collect(int doc) throws IOException {
            keyValues.forEachValueInDoc(doc, histoProc);
        }

        @Override
        public void postCollection() {
        }
    }

    public static class HistogramProc implements DoubleValues.ValueInDocProc {

        final long interval;
        final ExtTLongObjectHashMap<InternalFullHistogramFacet.FullEntry> entries;

        DoubleValues valueValues;

        final ValueAggregator valueAggregator = new ValueAggregator();

        public HistogramProc(long interval, ExtTLongObjectHashMap<InternalFullHistogramFacet.FullEntry> entries) {
            this.interval = interval;
            this.entries = entries;
        }

        @Override
        public void onMissing(int docId) {
        }

        @Override
        public void onValue(int docId, double value) {
            long bucket = FullHistogramFacetExecutor.bucket(value, interval);
            InternalFullHistogramFacet.FullEntry entry = entries.get(bucket);
            if (entry == null) {
                entry = new InternalFullHistogramFacet.FullEntry(bucket, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0, 0);
                entries.put(bucket, entry);
            }
            entry.count++;
            valueAggregator.entry = entry;
            valueValues.forEachValueInDoc(docId, valueAggregator);
        }

        public static class ValueAggregator implements DoubleValues.ValueInDocProc {

            InternalFullHistogramFacet.FullEntry entry;

            @Override
            public void onMissing(int docId) {
            }

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