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
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.longs.LongFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * A histogram facet collector that uses the same field as the key as well as the
 * value.
 */
public class ValueScriptDateHistogramFacetCollector extends AbstractFacetCollector {

    private final String indexFieldName;

    private final DateHistogramFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final FieldDataType fieldDataType;

    private LongFieldData fieldData;

    private final SearchScript valueScript;

    private final DateHistogramProc histoProc;

    private final String interval;

    public ValueScriptDateHistogramFacetCollector(String facetName, String fieldName, String interval, String scriptLang, String valueScript, Map<String, Object> params, TimeZoneRounding tzRounding, DateHistogramFacet.ComparatorType comparatorType, SearchContext context) {
        super(facetName);
        this.comparatorType = comparatorType;
        this.fieldDataCache = context.fieldDataCache();
        this.interval = interval;

        MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldName + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.explicitTypeInNameWithDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        this.valueScript = context.scriptService().search(context.lookup(), scriptLang, valueScript, params);

        FieldMapper mapper = smartMappers.mapper();

        indexFieldName = mapper.names().indexName();
        fieldDataType = mapper.fieldDataType();

        histoProc = new DateHistogramProc(tzRounding, this.valueScript);
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        fieldData.forEachValueInDoc(doc, histoProc);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        valueScript.setScorer(scorer);
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        fieldData = (LongFieldData) fieldDataCache.cache(fieldDataType, context.reader(), indexFieldName);
        valueScript.setNextReader(context);
    }

    @Override
    public Facet facet() {
        return new InternalFullDateHistogramFacet(facetName, interval, comparatorType, histoProc.entries, true);
    }

    public static class DateHistogramProc implements LongFieldData.LongValueInDocProc {

        private final TimeZoneRounding tzRounding;

        protected final SearchScript valueScript;

        final ExtTLongObjectHashMap<InternalFullDateHistogramFacet.FullEntry> entries = CacheRecycler.popLongObjectMap();

        public DateHistogramProc(TimeZoneRounding tzRounding, SearchScript valueScript) {
            this.tzRounding = tzRounding;
            this.valueScript = valueScript;
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
