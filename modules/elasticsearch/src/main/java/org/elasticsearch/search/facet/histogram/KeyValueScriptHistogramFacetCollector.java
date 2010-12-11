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
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.search.SearchScript;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.support.AbstractFacetCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * A histogram facet collector that uses the same field as the key as well as the
 * value.
 *
 * @author kimchy (shay.banon)
 */
public class KeyValueScriptHistogramFacetCollector extends AbstractFacetCollector {

    private final String fieldName;

    private final String indexFieldName;

    private final long interval;

    private final HistogramFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final FieldDataType fieldDataType;

    private NumericFieldData fieldData;

    private final SearchScript valueScript;

    private final HistogramProc histoProc;

    public KeyValueScriptHistogramFacetCollector(String facetName, String fieldName, String scriptLang, String valueScript, Map<String, Object> params, long interval, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.fieldName = fieldName;
        this.interval = interval;
        this.comparatorType = comparatorType;
        this.fieldDataCache = context.fieldDataCache();

        MapperService.SmartNameFieldMappers smartMappers = context.mapperService().smartName(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldName + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.hasDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        this.valueScript = new SearchScript(context.lookup(), scriptLang, valueScript, params, context.scriptService());

        FieldMapper mapper = smartMappers.mapper();

        indexFieldName = mapper.names().indexName();
        fieldDataType = mapper.fieldDataType();

        histoProc = new HistogramProc(interval, this.valueScript);
    }

    @Override protected void doCollect(int doc) throws IOException {
        fieldData.forEachValueInDoc(doc, histoProc);
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        fieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, indexFieldName);
        valueScript.setNextReader(reader);
    }

    @Override public Facet facet() {
        return new InternalHistogramFacet(facetName, fieldName, fieldName, interval, comparatorType, histoProc.counts(), histoProc.totals());
    }

    public static long bucket(double value, long interval) {
        return (((long) (value / interval)) * interval);
    }

    public static class HistogramProc implements NumericFieldData.DoubleValueInDocProc {

        private final long interval;

        private final SearchScript valueScript;

        private final TLongLongHashMap counts = new TLongLongHashMap();

        private final TLongDoubleHashMap totals = new TLongDoubleHashMap();

        public HistogramProc(long interval, SearchScript valueScript) {
            this.interval = interval;
            this.valueScript = valueScript;
        }

        @Override public void onValue(int docId, double value) {
            long bucket = bucket(value, interval);
            counts.adjustOrPutValue(bucket, 1, 1);
            double scriptValue = ((Number) valueScript.execute(docId)).doubleValue();
            totals.adjustOrPutValue(bucket, scriptValue, scriptValue);
        }

        public TLongLongHashMap counts() {
            return counts;
        }

        public TLongDoubleHashMap totals() {
            return totals;
        }
    }
}