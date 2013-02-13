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

package org.elasticsearch.search.facet.histogram.bounded;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

public class BoundedCountHistogramFacetCollector extends AbstractFacetCollector {

    private final IndexNumericFieldData indexFieldData;

    private final HistogramFacet.ComparatorType comparatorType;

    private LongValues values;

    private final HistogramProc histoProc;

    public BoundedCountHistogramFacetCollector(String facetName, IndexNumericFieldData indexFieldData, long interval, long from, long to, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.comparatorType = comparatorType;
        this.indexFieldData = indexFieldData;
        long normalizedFrom = (((long) ((double) from / interval)) * interval);
        long normalizedTo = (((long) ((double) to / interval)) * interval);
        if ((to % interval) != 0) {
            normalizedTo += interval;
        }
        long offset = -normalizedFrom;
        int size = (int) ((normalizedTo - normalizedFrom) / interval);

        histoProc = new HistogramProc(from, to, interval, offset, size);
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
        return new InternalBoundedCountHistogramFacet(facetName, comparatorType, histoProc.interval, -histoProc.offset, histoProc.size, histoProc.counts, true);
    }

    public static class HistogramProc implements LongValues.ValueInDocProc {

        final long from;
        final long to;

        final long interval;

        final long offset;

        final int size;

        final int[] counts;

        public HistogramProc(long from, long to, long interval, long offset, int size) {
            this.from = from;
            this.to = to;
            this.interval = interval;
            this.offset = offset;
            this.size = size;
            this.counts = CacheRecycler.popIntArray(size);
        }

        @Override
        public void onMissing(int docId) {
        }

        @Override
        public void onValue(int docId, long value) {
            if (value <= from || value > to) { // bounds check
                return;
            }
            counts[((int) ((value + offset) / interval))]++;
        }
    }
}