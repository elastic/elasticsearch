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

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ScriptHistogramFacetExecutor extends FacetExecutor {

    final SearchScript keyScript;
    final SearchScript valueScript;
    final long interval;
    private final HistogramFacet.ComparatorType comparatorType;

    final Recycler.V<LongObjectOpenHashMap<InternalFullHistogramFacet.FullEntry>> entries;

    public ScriptHistogramFacetExecutor(String scriptLang, String keyScript, String valueScript, Map<String, Object> params, long interval, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        this.keyScript = context.scriptService().search(context.lookup(), scriptLang, keyScript, params);
        this.valueScript = context.scriptService().search(context.lookup(), scriptLang, valueScript, params);
        this.interval = interval > 0 ? interval : 0;
        this.comparatorType = comparatorType;

        this.entries = context.cacheRecycler().longObjectMap(-1);
    }

    @Override
    public Collector collector() {
        return new Collector(entries.v());
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        List<InternalFullHistogramFacet.FullEntry> entries1 = new ArrayList<InternalFullHistogramFacet.FullEntry>(entries.v().size());
        final boolean[] states = entries.v().allocated;
        final Object[] values = entries.v().values;
        for (int i = 0; i < states.length; i++) {
            if (states[i]) {
                InternalFullHistogramFacet.FullEntry value = (InternalFullHistogramFacet.FullEntry) values[i];
                entries1.add(value);
            }
        }

        entries.release();
        return new InternalFullHistogramFacet(facetName, comparatorType, entries1);
    }

    public static long bucket(double value, long interval) {
        return (((long) (value / interval)) * interval);
    }

    class Collector extends FacetExecutor.Collector {

        final LongObjectOpenHashMap<InternalFullHistogramFacet.FullEntry> entries;

        Collector(LongObjectOpenHashMap<InternalFullHistogramFacet.FullEntry> entries) {
            this.entries = entries;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            keyScript.setScorer(scorer);
            valueScript.setScorer(scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            keyScript.setNextReader(context);
            valueScript.setNextReader(context);
        }

        @Override
        public void collect(int doc) throws IOException {
            keyScript.setNextDocId(doc);
            valueScript.setNextDocId(doc);
            long bucket;
            if (interval == 0) {
                bucket = keyScript.runAsLong();
            } else {
                bucket = bucket(keyScript.runAsDouble(), interval);
            }
            double value = valueScript.runAsDouble();

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

        @Override
        public void postCollection() {
        }
    }
}