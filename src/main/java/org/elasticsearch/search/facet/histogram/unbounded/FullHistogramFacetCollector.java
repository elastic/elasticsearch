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

package org.elasticsearch.search.facet.histogram.unbounded;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * A histogram facet collector that uses the same field as the key as well as the
 * value.
 */
public class FullHistogramFacetCollector extends AbstractFacetCollector {

    private final IndexNumericFieldData indexFieldData;

    private final HistogramFacet.ComparatorType comparatorType;

    private DoubleValues values;
    private final HistogramProc histoProc;

    public FullHistogramFacetCollector(String facetName, IndexNumericFieldData indexFieldData, long interval, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.comparatorType = comparatorType;
        this.indexFieldData = indexFieldData;
        histoProc = new HistogramProc(interval);
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        values.forEachValueInDoc(doc, histoProc);
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        values = indexFieldData.load(context).getDoubleValues();
    }

    @Override
    public Facet facet() {
        return new InternalFullHistogramFacet(facetName, comparatorType, histoProc.entries, true);
    }

    public static long bucket(double value, long interval) {
        return (((long) (value / interval)) * interval);
    }

    public static class HistogramProc implements DoubleValues.ValueInDocProc {

        final long interval;

        final ExtTLongObjectHashMap<InternalFullHistogramFacet.FullEntry> entries = CacheRecycler.popLongObjectMap();

        public HistogramProc(long interval) {
            this.interval = interval;
        }

        @Override
        public void onMissing(int docId) {
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