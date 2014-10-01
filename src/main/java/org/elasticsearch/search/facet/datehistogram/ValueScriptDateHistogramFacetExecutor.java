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
import org.apache.lucene.search.Scorer;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.LongFacetAggregatorBase;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A histogram facet collector that uses the same field as the key as well as the
 * value.
 */
public class ValueScriptDateHistogramFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData keyIndexFieldData;
    private final DateHistogramFacet.ComparatorType comparatorType;
    final SearchScript valueScript;
    final Rounding tzRounding;

    final Recycler.V<LongObjectOpenHashMap<InternalFullDateHistogramFacet.FullEntry>> entries;

    public ValueScriptDateHistogramFacetExecutor(IndexNumericFieldData keyIndexFieldData, SearchScript valueScript, Rounding tzRounding, DateHistogramFacet.ComparatorType comparatorType, CacheRecycler cacheRecycler) {
        this.comparatorType = comparatorType;
        this.keyIndexFieldData = keyIndexFieldData;
        this.valueScript = valueScript;
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
                InternalFullDateHistogramFacet.FullEntry value = (InternalFullDateHistogramFacet.FullEntry) values[i];
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
            histoProc = new DateHistogramProc(tzRounding, valueScript, entries.v());
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            valueScript.setScorer(scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            keyValues = keyIndexFieldData.load(context).getLongValues();
            valueScript.setNextReader(context);
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

        private final Rounding tzRounding;
        protected final SearchScript valueScript;

        final LongObjectOpenHashMap<InternalFullDateHistogramFacet.FullEntry> entries;

        public DateHistogramProc(Rounding tzRounding, SearchScript valueScript, final LongObjectOpenHashMap<InternalFullDateHistogramFacet.FullEntry> entries) {
            this.tzRounding = tzRounding;
            this.valueScript = valueScript;
            this.entries = entries;
        }

        @Override
        public void onValue(int docId, long value) {
            valueScript.setNextDocId(docId);
            long time = tzRounding.round(value);
            double scriptValue = valueScript.runAsDouble();

            InternalFullDateHistogramFacet.FullEntry entry = entries.get(time);
            if (entry == null) {
                entry = new InternalFullDateHistogramFacet.FullEntry(time, 1, scriptValue, scriptValue, 1, scriptValue);
                entries.put(time, entry);
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