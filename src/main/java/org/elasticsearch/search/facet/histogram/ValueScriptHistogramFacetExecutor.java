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
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.DoubleFacetAggregatorBase;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A histogram facet collector that uses the same field as the key as well as the
 * value.
 */
public class ValueScriptHistogramFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData indexFieldData;
    private final HistogramFacet.ComparatorType comparatorType;
    final SearchScript valueScript;
    final long interval;

    final Recycler.V<LongObjectOpenHashMap<InternalFullHistogramFacet.FullEntry>> entries;

    public ValueScriptHistogramFacetExecutor(IndexNumericFieldData indexFieldData, String scriptLang, String valueScript, Map<String, Object> params, long interval, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        this.comparatorType = comparatorType;
        this.indexFieldData = indexFieldData;
        this.interval = interval;
        this.valueScript = context.scriptService().search(context.lookup(), scriptLang, valueScript, params);

        this.entries = context.cacheRecycler().longObjectMap(-1);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        List<InternalFullHistogramFacet.FullEntry> entries1 = new ArrayList<>(entries.v().size());
        final boolean[] states = entries.v().allocated;
        final Object[] values = entries.v().values;
        for (int i = 0; i < states.length; i++) {
            if (states[i]) {
                InternalFullHistogramFacet.FullEntry value = (InternalFullHistogramFacet.FullEntry) values[i];
                entries1.add(value);
            }
        }

        entries.close();
        return new InternalFullHistogramFacet(facetName, comparatorType, entries1);
    }

    public static long bucket(double value, long interval) {
        return (((long) (value / interval)) * interval);
    }

    class Collector extends FacetExecutor.Collector {

        private DoubleValues values;
        private final HistogramProc histoProc;

        public Collector() {
            histoProc = new HistogramProc(interval, valueScript, entries.v());
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            valueScript.setScorer(scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getDoubleValues();
            valueScript.setNextReader(context);
        }

        @Override
        public void collect(int doc) throws IOException {
            histoProc.onDoc(doc, values);
        }

        @Override
        public void postCollection() {
        }
    }

    public static class HistogramProc extends DoubleFacetAggregatorBase {

        private final long interval;

        private final SearchScript valueScript;

        final LongObjectOpenHashMap<InternalFullHistogramFacet.FullEntry> entries;

        public HistogramProc(long interval, SearchScript valueScript, LongObjectOpenHashMap<InternalFullHistogramFacet.FullEntry> entries) {
            this.interval = interval;
            this.valueScript = valueScript;
            this.entries = entries;
        }

        @Override
        public void onValue(int docId, double value) {
            valueScript.setNextDocId(docId);
            long bucket = bucket(value, interval);
            double scriptValue = valueScript.runAsDouble();

            InternalFullHistogramFacet.FullEntry entry = entries.get(bucket);
            if (entry == null) {
                entry = new InternalFullHistogramFacet.FullEntry(bucket, 1, scriptValue, scriptValue, 1, scriptValue);
                entries.put(bucket, entry);
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