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

package org.elasticsearch.search.facet.datehistogram;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * A histogram facet collector that uses the same field as the key as well as the
 * value.
 */
public class ValueScriptDateHistogramFacetCollector extends AbstractFacetCollector {

    private final IndexNumericFieldData keyIndexFieldData;

    private final DateHistogramFacet.ComparatorType comparatorType;

    private LongValues keyValues;
    private final SearchScript valueScript;
    private final DateHistogramProc histoProc;

    public ValueScriptDateHistogramFacetCollector(String facetName, IndexNumericFieldData keyIndexFieldData, SearchScript valueScript, TimeZoneRounding tzRounding, DateHistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.comparatorType = comparatorType;
        this.keyIndexFieldData = keyIndexFieldData;
        this.valueScript = valueScript;
        histoProc = new DateHistogramProc(tzRounding, this.valueScript);
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
        keyValues = keyIndexFieldData.load(context).getLongValues();
        valueScript.setNextReader(context);
    }

    @Override
    public Facet facet() {
        return new InternalFullDateHistogramFacet(facetName, comparatorType, histoProc.entries, true);
    }

    public static class DateHistogramProc implements LongValues.ValueInDocProc {

        private final TimeZoneRounding tzRounding;

        protected final SearchScript valueScript;

        final ExtTLongObjectHashMap<InternalFullDateHistogramFacet.FullEntry> entries = CacheRecycler.popLongObjectMap();

        public DateHistogramProc(TimeZoneRounding tzRounding, SearchScript valueScript) {
            this.tzRounding = tzRounding;
            this.valueScript = valueScript;
        }

        @Override
        public void onMissing(int docId) {
        }

        @Override
        public void onValue(int docId, long value) {
            valueScript.setNextDocId(docId);
            long time = tzRounding.calc(value);
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