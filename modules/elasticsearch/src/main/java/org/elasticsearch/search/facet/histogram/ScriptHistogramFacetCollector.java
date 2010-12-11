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

package org.elasticsearch.search.facet.histogram;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.trove.TLongDoubleHashMap;
import org.elasticsearch.common.trove.TLongLongHashMap;
import org.elasticsearch.script.search.SearchScript;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.support.AbstractFacetCollector;
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

    private final TLongLongHashMap counts = new TLongLongHashMap();

    private final TLongDoubleHashMap totals = new TLongDoubleHashMap();

    public ScriptHistogramFacetCollector(String facetName, String scriptLang, String keyScript, String valueScript, Map<String, Object> params, long interval, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.keyScript = new SearchScript(context.lookup(), scriptLang, keyScript, params, context.scriptService());
        this.valueScript = new SearchScript(context.lookup(), scriptLang, valueScript, params, context.scriptService());
        this.interval = interval > 0 ? interval : 0;
        this.comparatorType = comparatorType;
    }

    @Override protected void doCollect(int doc) throws IOException {
        Number keyValue = (Number) keyScript.execute(doc);
        long bucket;
        if (interval == 0) {
            bucket = keyValue.longValue();
        } else {
            bucket = bucket(keyValue.doubleValue(), interval);
        }
        double value = ((Number) valueScript.execute(doc)).doubleValue();
        counts.adjustOrPutValue(bucket, 1, 1);
        totals.adjustOrPutValue(bucket, value, value);
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        keyScript.setNextReader(reader);
        valueScript.setNextReader(reader);
    }

    @Override public Facet facet() {
        return new InternalHistogramFacet(facetName, "_na", "_na", -1, comparatorType, counts, totals);
    }

    public static long bucket(double value, long interval) {
        return (((long) (value / interval)) * interval);
    }
}