/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptHistogramFacetCollector extends AbstractFacetCollector {

    private final SearchScript keyScript;

    private final SearchScript valueScript;

    private final long interval;

    private final HistogramFacet.ComparatorType comparatorType;

    final ExtTLongObjectHashMap<InternalFullHistogramFacet.FullEntry> entries = CacheRecycler.popLongObjectMap();

    public ScriptHistogramFacetCollector(String facetName, String scriptLang, String keyScript, String valueScript, Map<String, Object> params, long interval, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.keyScript = context.scriptService().search(context.lookup(), scriptLang, keyScript, params);
        this.valueScript = context.scriptService().search(context.lookup(), scriptLang, valueScript, params);
        this.interval = interval > 0 ? interval : 0;
        this.comparatorType = comparatorType;
    }

    @Override protected void doCollect(int doc) throws IOException {
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

    @Override public void setScorer(Scorer scorer) throws IOException {
        keyScript.setScorer(scorer);
        valueScript.setScorer(scorer);
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        keyScript.setNextReader(reader);
        valueScript.setNextReader(reader);
    }

    @Override public Facet facet() {
        return new InternalFullHistogramFacet(facetName, comparatorType, entries, true);
    }

    public static long bucket(double value, long interval) {
        return (((long) (value / interval)) * interval);
    }
}