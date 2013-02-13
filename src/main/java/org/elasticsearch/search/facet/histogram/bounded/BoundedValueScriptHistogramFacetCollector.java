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
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class BoundedValueScriptHistogramFacetCollector extends AbstractFacetCollector {

    private final IndexNumericFieldData indexFieldData;

    private final HistogramFacet.ComparatorType comparatorType;

    private LongValues keyValues;

    private final SearchScript valueScript;

    private final HistogramProc histoProc;

    public BoundedValueScriptHistogramFacetCollector(String facetName, IndexNumericFieldData indexFieldData, String scriptLang, String valueScript, Map<String, Object> params, long interval, long from, long to, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.comparatorType = comparatorType;
        this.indexFieldData = indexFieldData;

        this.valueScript = context.scriptService().search(context.lookup(), scriptLang, valueScript, params);

        long normalizedFrom = (((long) ((double) from / interval)) * interval);
        long normalizedTo = (((long) ((double) to / interval)) * interval);
        if ((to % interval) != 0) {
            normalizedTo += interval;
        }
        long offset = -normalizedFrom;
        int size = (int) ((normalizedTo - normalizedFrom) / interval);

        histoProc = new HistogramProc(from, to, interval, offset, size, this.valueScript);
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        keyValues.forEachValueInDoc(doc, histoProc);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        valueScript.setScorer(scorer);
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        keyValues = indexFieldData.load(context).getLongValues();
        valueScript.setNextReader(context);
    }

    @Override
    public Facet facet() {
        return new InternalBoundedFullHistogramFacet(facetName, comparatorType, histoProc.interval, -histoProc.offset, histoProc.size, histoProc.entries, true);
    }

    public static long bucket(double value, long interval) {
        return (((long) (value / interval)) * interval);
    }

    public static class HistogramProc implements LongValues.ValueInDocProc {

        final long from;
        final long to;

        final long interval;

        final long offset;

        final int size;

        final Object[] entries;

        private final SearchScript valueScript;

        public HistogramProc(long from, long to, long interval, long offset, int size, SearchScript valueScript) {
            this.from = from;
            this.to = to;
            this.interval = interval;
            this.offset = offset;
            this.size = size;
            this.entries = CacheRecycler.popObjectArray(size);
            this.valueScript = valueScript;
        }

        @Override
        public void onMissing(int docId) {
        }

        @Override
        public void onValue(int docId, long value) {
            if (value <= from || value > to) { // bounds check
                return;
            }
            int index = ((int) ((value + offset) / interval));

            valueScript.setNextDocId(docId);
            double scriptValue = valueScript.runAsDouble();

            InternalBoundedFullHistogramFacet.FullEntry entry = (InternalBoundedFullHistogramFacet.FullEntry) entries[index];
            if (entry == null) {
                entries[index] = new InternalBoundedFullHistogramFacet.FullEntry(index, 1, scriptValue, scriptValue, 1, scriptValue);
            } else {
                entry.count++;
                entry.totalCount++;
                entry.total += scriptValue;
                if (scriptValue < entry.min) {
                    entry.min = scriptValue;
                }
                if (scriptValue > entry.max) {
                    entry.max = scriptValue;
                }
            }
        }
    }
}